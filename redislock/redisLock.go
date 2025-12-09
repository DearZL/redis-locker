package redislock

import (
	"context"
	"errors"
	"math/rand/v2"
	"sync"
	"time"

	"github.com/oklog/ulid/v2"
	"github.com/redis/go-redis/v9"
	"github.com/zeromicro/go-zero/core/logx"
)

// 此处使用Lua脚本是为了保证续期锁和释放锁只操作自身持有锁
// 用Lua脚本来实现检查值和操作键在同一个原子操作中
// Lua脚本：原子性释放锁
const unlockScript = `
if redis.call("get", KEYS[1]) == ARGV[1] then
    return redis.call("del", KEYS[1])
else
    return 0
end
`

// Lua脚本：原子性续期
const renewScript = `
if redis.call("get", KEYS[1]) == ARGV[1] then
    return redis.call("pexpire", KEYS[1], ARGV[2])
else
    return 0
end
`

var (
	// 提前实例化对象 避免多次重复创建
	unlockRedisScript = redis.NewScript(unlockScript)
	renewRedisScript  = redis.NewScript(renewScript)

	// ErrLockFailed 获取锁失败
	ErrLockFailed = errors.New("failed to acquire lock")
	// ErrLockNotHeld 未持有锁
	ErrLockNotHeld = errors.New("lock not held")
)

const (
	// LockPrefix 锁前缀
	LockPrefix = "RedisLocker:"
	// MinRetryDelay 最小重试间隔
	MinRetryDelay = 100 * time.Millisecond
	// MinExpiration 最小过期时间
	MinExpiration = time.Second
)

// RedisLock Redis分布式锁
type RedisLock struct {
	logger       logx.Logger
	client       *redis.Client
	key          string
	value        string
	expiration   time.Duration
	retryCount   uint8
	retryDelay   time.Duration
	watchdogStop chan struct{}
	watchdogMu   sync.Mutex
}

// Config 锁配置
type Config struct {
	// 锁的名称 默认会加入前缀 LockPrefix
	// 不同业务推荐使用不同 Const 变量来传入 防止意外错误
	Key string
	// 锁的过期时间 默认30秒
	Expiration time.Duration
	// 重试次数 默认0（不重试）
	RetryCount uint8
	// 重试间隔 默认100ms
	RetryDelay time.Duration
}

// NewRedisLock 创建Redis分布式锁
func NewRedisLock(client *redis.Client, config Config, logger logx.Logger) *RedisLock {
	if config.Expiration == 0 {
		config.Expiration = 30 * time.Second
	} else {
		// 最小过期时间 5 秒
		if config.Expiration < MinExpiration {
			config.Expiration = MinExpiration
		}
	}
	if config.RetryDelay == 0 {
		config.RetryDelay = 100 * time.Millisecond
	} else {
		// 最小重试间隔 100 毫秒
		if config.RetryDelay < MinRetryDelay {
			config.RetryDelay = MinRetryDelay
		}
	}
	if logger == nil {
		logger = logx.WithContext(context.Background())
	}

	return &RedisLock{
		logger: logger,
		client: client,
		key:    LockPrefix + config.Key,
		// 使用ULID作为锁的唯一标识
		// 详见github.com/oklog/ulid
		value:      ulid.Make().String(),
		expiration: config.Expiration,
		retryCount: config.RetryCount,
		retryDelay: config.RetryDelay,
	}
}

// Lock 获取锁（阻塞式 带重试）
func (l *RedisLock) Lock(ctx context.Context) error {
	if ok, err := l.tryLock(ctx); ok && err == nil {
		return nil
	}

	for i := uint8(0); i < l.retryCount; i++ {
		// 生成 [-20%, +20%] 的随机抖动
		jitterRange := int64(l.retryDelay) * 20 / 100
		jitter := rand.Int64N(2*jitterRange+1) - jitterRange
		delay := l.retryDelay + time.Duration(jitter)

		// 防御性检查
		if delay <= 0 {
			delay = l.retryDelay
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(delay):
			if ok, err := l.tryLock(ctx); ok && err == nil {
				return nil
			}
		}
	}

	return ErrLockFailed
}

// TryLock 尝试获取锁（非阻塞）
func (l *RedisLock) TryLock(ctx context.Context) error {
	if ok, err := l.tryLock(ctx); ok && err == nil {
		return nil
	}
	return ErrLockFailed
}

// tryLock 获取锁的实现
func (l *RedisLock) tryLock(ctx context.Context) (bool, error) {
	// 使用SET NX EX命令原子性地设置锁
	result, err := l.client.SetNX(ctx, l.key, l.value, l.expiration).Result()
	if err != nil {
		// 只有 Redis 操作出错才打日志
		l.logger.Errorf("redis SetNX error: %v, locker key: %s", err, l.key)
		return false, err
	}
	return result, nil
}

// Unlock 释放锁
func (l *RedisLock) Unlock(ctx context.Context) error {
	// 防止竞态条件
	l.watchdogMu.Lock()
	if l.watchdogStop != nil {
		close(l.watchdogStop)
		l.watchdogStop = nil
	}
	l.watchdogMu.Unlock()

	// 使用Lua脚本确保只删除自己持有的锁
	script := unlockRedisScript
	result, err := script.Run(ctx, l.client, []string{l.key}, l.value).Int64()
	if err != nil {
		l.logger.Errorf("redis unlock error: %v, locker key: %s", err, l.key)
		return err
	}

	if result == 0 {
		return ErrLockNotHeld
	}

	return nil
}

// Renew 手动续期
func (l *RedisLock) Renew(ctx context.Context) error {
	script := renewRedisScript
	result, err := script.Run(
		ctx, l.client, []string{l.key},
		l.value, l.expiration.Milliseconds(),
	).Int64()

	if err != nil {
		return err
	}

	if result == 0 {
		return ErrLockNotHeld
	}

	return nil
}

// StartWatchdog 启动看门狗自动续期
// renewInterval 续期间隔 建议设置为expiration的1/3
func (l *RedisLock) StartWatchdog(ctx context.Context, renewInterval time.Duration) {
	// 防止 watchdog 启动时竞态条件
	l.watchdogMu.Lock()
	defer l.watchdogMu.Unlock()

	if l.watchdogStop != nil {
		// 已经启动
		return
	}

	l.watchdogStop = make(chan struct{})
	ticker := time.NewTicker(renewInterval)

	go func() {
		// 捕获 panic
		defer func() {
			if r := recover(); r != nil {
				l.logger.Errorf("watchdog panic recovered: %v, locker key: %s", r, l.key)
			}
		}()
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				if err := l.Renew(ctx); err != nil {
					// 续期失败 可能锁已被释放或过期
					return
				}
			case <-l.watchdogStop:
				return
			case <-ctx.Done():
				return
			}
		}
	}()
}

// LockWithWatchdog 获取锁并自动启动看门狗
func (l *RedisLock) LockWithWatchdog(ctx context.Context) error {
	if err := l.Lock(ctx); err != nil {
		return err
	}

	// 自动续期间隔设置为过期时间的1/3
	renewInterval := l.expiration / 3
	l.StartWatchdog(ctx, renewInterval)

	return nil
}

// WithLock 使用锁执行函数(自动解锁,带重试)
func (l *RedisLock) WithLock(ctx context.Context, fn func() error) error {
	// 获取锁
	if err := l.Lock(ctx); err != nil {
		return err
	}

	// 确保释放锁
	defer func() {
		unlockCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = l.Unlock(unlockCtx)
	}()

	// 执行业务逻辑
	return fn()
}

// WithLockAndWatchdog 使用锁和看门狗执行函数（自动解锁,适用于长时间任务）
func (l *RedisLock) WithLockAndWatchdog(ctx context.Context, fn func() error) error {
	// 获取锁并启动看门狗
	if err := l.LockWithWatchdog(ctx); err != nil {
		return err
	}

	// 确保释放锁
	defer func() {
		unlockCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = l.Unlock(unlockCtx)
	}()

	// 执行业务逻辑
	return fn()
}
