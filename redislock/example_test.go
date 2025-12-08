package redislock

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/zeromicro/go-zero/core/logx"
)

// 创建测试用的Redis客户端
func getTestRedisClient() *redis.Client {
	return redis.NewClient(&redis.Options{
		Addr:     "dev-server:6379",
		Password: "",
		DB:       0,
	})
}

// TestBasicLockUnlock 基本的加锁解锁测试
func TestBasicLockUnlock(t *testing.T) {
	client := getTestRedisClient()
	ctx := context.Background()

	lock := NewRedisLock(client, Config{
		Key:        "test:basic",
		Expiration: 10 * time.Second,
	}, logx.WithContext(ctx))

	// 获取锁
	err := lock.TryLock(ctx)
	assert.NoError(t, err, "应该成功获取锁")

	// 释放锁
	err = lock.Unlock(ctx)
	assert.NoError(t, err, "应该成功释放锁")

	fmt.Println("✅ 基本锁测试通过")
}

// TestConcurrentLock 并发竞争锁测试
func TestConcurrentLock(t *testing.T) {
	client := getTestRedisClient()
	ctx := context.Background()

	const goroutineCount = 10
	var counter int32
	var wg sync.WaitGroup

	lockKey := fmt.Sprintf("test:concurrent:%d", time.Now().Unix())

	// 启动多个goroutine竞争同一把锁
	for i := 0; i < goroutineCount; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()

			lock := NewRedisLock(client, Config{
				Key:        lockKey,
				Expiration: 5 * time.Second,
				RetryCount: 50,
				RetryDelay: 100 * time.Millisecond,
			}, logx.WithContext(ctx))

			err := lock.WithLock(ctx, func() error {
				// 模拟读取-修改-写入操作
				current := atomic.LoadInt32(&counter)
				time.Sleep(10 * time.Millisecond) // 模拟处理时间
				atomic.StoreInt32(&counter, current+1)
				fmt.Printf("Goroutine %d: counter = %d\n", id, current+1)
				return nil
			})

			if err != nil {
				t.Errorf("Goroutine %d 获取锁失败: %v", id, err)
			}
		}(i)
	}

	wg.Wait()

	// 验证counter应该正好等于goroutine数量
	assert.Equal(t, int32(goroutineCount), counter,
		"并发安全测试失败: counter应该等于%d", goroutineCount)

	fmt.Printf("✅ 并发锁测试通过: %d个goroutine, counter = %d\n",
		goroutineCount, counter)
}

// TestLockReentry 重入锁测试（分布式锁不支持重入，应该失败）
func TestLockReentry(t *testing.T) {
	client := getTestRedisClient()
	ctx := context.Background()

	lock := NewRedisLock(client, Config{
		Key:        "test:reentry",
		Expiration: 10 * time.Second,
	}, logx.WithContext(ctx))

	// 第一次获取锁
	err := lock.TryLock(ctx)
	assert.NoError(t, err, "第一次获取锁应该成功")

	// 同一个锁对象尝试再次获取（会失败，因为Redis中已存在）
	lock2 := NewRedisLock(client, Config{
		Key:        "test:reentry",
		Expiration: 10 * time.Second,
	}, logx.WithContext(ctx))
	err = lock2.TryLock(ctx)
	assert.Error(t, err, "重入获取锁应该失败")

	lock.Unlock(ctx)
	fmt.Println("✅ 重入锁测试通过（符合预期：不支持重入）")
}

// TestLockExpiration 锁过期测试
func TestLockExpiration(t *testing.T) {
	client := getTestRedisClient()
	ctx := context.Background()

	lock := NewRedisLock(client, Config{
		Key:        "test:expiration",
		Expiration: 2 * time.Second, // 2秒过期
	}, logx.WithContext(ctx))

	// 获取锁
	err := lock.TryLock(ctx)
	assert.NoError(t, err, "应该成功获取锁")

	// 等待锁过期
	fmt.Println("等待锁过期...")
	time.Sleep(3 * time.Second)

	// 另一个客户端应该能获取锁
	lock2 := NewRedisLock(client, Config{
		Key:        "test:expiration",
		Expiration: 5 * time.Second,
	}, logx.WithContext(ctx))
	err = lock2.TryLock(ctx)
	assert.NoError(t, err, "锁过期后应该能重新获取")

	lock2.Unlock(ctx)
	fmt.Println("✅ 锁过期测试通过")
}

// TestWatchdog 看门狗自动续期测试
func TestWatchdog(t *testing.T) {
	client := getTestRedisClient()
	ctx := context.Background()

	lock := NewRedisLock(client, Config{
		Key:        "test:watchdog",
		Expiration: 1 * time.Second, // 3秒过期
	}, logx.WithContext(ctx))

	startTime := time.Now()

	err := lock.WithLockAndWatchdog(ctx, func() error {
		// 执行10秒的任务，但锁只有3秒过期时间
		// 看门狗应该自动续期
		fmt.Println("开始执行长时间任务...")
		for i := 0; i < 10; i++ {
			time.Sleep(1 * time.Second)
			fmt.Printf("任务执行中... %d秒\n", i+1)
		}
		return nil
	})

	duration := time.Since(startTime)
	assert.NoError(t, err, "看门狗任务应该成功完成")
	assert.True(t, duration >= 10*time.Second,
		"任务应该完整执行10秒")

	fmt.Printf("✅ 看门狗测试通过: 任务执行时间 %.1f秒\n", duration.Seconds())
}

// TestHighConcurrency 高并发压力测试
func TestHighConcurrency(t *testing.T) {
	client := getTestRedisClient()
	ctx := context.Background()

	const (
		goroutineCount = 100
		iterations     = 10
	)

	var successCount int32
	var failCount int32
	var wg sync.WaitGroup

	lockKey := fmt.Sprintf("test:highconcurrency:%d", time.Now().Unix())
	startTime := time.Now()

	for i := 0; i < goroutineCount; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()

			for j := 0; j < iterations; j++ {
				lock := NewRedisLock(client, Config{
					Key:        lockKey,
					Expiration: 1 * time.Second,
					RetryCount: 200,
					RetryDelay: 100 * time.Millisecond,
				}, logx.WithContext(ctx))

				err := lock.WithLock(ctx, func() error {
					// 模拟业务处理
					time.Sleep(10 * time.Millisecond)
					return nil
				})

				if err == nil {
					atomic.AddInt32(&successCount, 1)
				} else {
					atomic.AddInt32(&failCount, 1)
				}
			}
		}(i)
	}

	wg.Wait()
	duration := time.Since(startTime)

	fmt.Printf("\n=== 高并发压力测试结果 ===\n")
	fmt.Printf("并发数: %d\n", goroutineCount)
	fmt.Printf("每个goroutine迭代: %d次\n", iterations)
	fmt.Printf("总请求数: %d\n", goroutineCount*iterations)
	fmt.Printf("成功: %d\n", successCount)
	fmt.Printf("失败: %d\n", failCount)
	fmt.Printf("成功率: %.2f%%\n", float64(successCount)/float64(goroutineCount*iterations)*100)
	fmt.Printf("总耗时: %.2f秒\n", duration.Seconds())
	fmt.Printf("QPS: %.2f\n", float64(successCount)/duration.Seconds())
	fmt.Println("========================")

	assert.Equal(t, int32(goroutineCount*iterations), successCount,
		"所有请求都应该成功")
	fmt.Println("✅ 高并发压力测试通过")
}

// TestUnlockWithoutLock 未持有锁就释放的测试
func TestUnlockWithoutLock(t *testing.T) {
	client := getTestRedisClient()
	ctx := context.Background()

	lock := NewRedisLock(client, Config{
		Key:        "test:unlock",
		Expiration: 5 * time.Second,
	}, logx.WithContext(ctx))

	// 尝试释放未获取的锁
	err := lock.Unlock(ctx)
	assert.Error(t, err, "释放未持有的锁应该返回错误")
	assert.Equal(t, ErrLockNotHeld, err)

	fmt.Println("✅ 未持有锁释放测试通过")
}

// TestContextCancellation Context取消测试
func TestContextCancellation(t *testing.T) {
	ctx := context.Background()
	client := getTestRedisClient()

	lock := NewRedisLock(client, Config{
		Key:        "test:context",
		Expiration: 30 * time.Second,
		RetryCount: 100,
		RetryDelay: 100 * time.Millisecond,
	}, logx.WithContext(ctx))

	// 先获取锁
	ctx1 := context.Background()
	err := lock.TryLock(ctx1)
	assert.NoError(t, err)

	// 创建一个会取消的context
	ctx2, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()

	// 另一个锁尝试获取（会因为context超时而失败）
	lock2 := NewRedisLock(client, Config{
		Key:        "test:context",
		Expiration: 30 * time.Second,
		RetryCount: 100,
		RetryDelay: 100 * time.Millisecond,
	}, logx.WithContext(ctx))

	err = lock2.Lock(ctx2)
	assert.Error(t, err, "context取消后应该返回错误")

	lock.Unlock(ctx1)
	fmt.Println("✅ Context取消测试通过")
}

// TestRaceCondition 竞态条件测试
func TestRaceCondition(t *testing.T) {
	client := getTestRedisClient()
	ctx := context.Background()

	// 共享资源
	sharedResource := 0
	var mu sync.Mutex
	raceDetected := false

	const goroutineCount = 20
	var wg sync.WaitGroup

	lockKey := fmt.Sprintf("test:race:%d", time.Now().Unix())

	for i := 0; i < goroutineCount; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()

			lock := NewRedisLock(client, Config{
				Key:        lockKey,
				Expiration: 5 * time.Second,
				RetryCount: 50,
				RetryDelay: 50 * time.Millisecond,
			}, logx.WithContext(ctx))

			err := lock.WithLock(ctx, func() error {
				// 读取当前值
				current := sharedResource

				// 检测竞态条件
				mu.Lock()
				if sharedResource != current {
					raceDetected = true
					fmt.Printf("❌ 检测到竞态条件! Goroutine %d\n", id)
				}
				mu.Unlock()

				// 模拟处理
				time.Sleep(10 * time.Millisecond)

				// 写入新值
				sharedResource = current + 1
				return nil
			})

			if err != nil {
				t.Errorf("Goroutine %d 失败: %v", id, err)
			}
		}(i)
	}

	wg.Wait()

	assert.False(t, raceDetected, "不应该检测到竞态条件")
	assert.Equal(t, goroutineCount, sharedResource,
		"最终值应该等于goroutine数量")

	fmt.Printf("✅ 竞态条件测试通过: 最终值 = %d\n", sharedResource)
}

// ========== 场景1：无竞争（全唯一Key）- 基准性能 ==========
func BenchmarkLock_Parallel_NoCompete(b *testing.B) {
	client := getTestRedisClient()
	ctx := context.Background()

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			// 每个请求用唯一Key（无任何竞争）
			lock := NewRedisLock(client, Config{
				Key:        fmt.Sprintf("bench:lock:no_compete:%d", time.Now().UnixNano()+rand.Int63()),
				Expiration: 5 * time.Second,
			}, logx.WithContext(ctx))
			_ = lock.WithLock(ctx, func() error { return nil })
		}
	})
}

// ========== 场景2：部分竞争（10%热点Key）- 真实业务场景 ==========
func BenchmarkLock_Parallel_PartCompete(b *testing.B) {
	client := getTestRedisClient()
	ctx := context.Background()
	// 模拟10个热点Key（占总请求的10%），其余为唯一Key
	hotKeys := make([]string, 10)
	for i := 0; i < 10; i++ {
		hotKeys[i] = fmt.Sprintf("bench:lock:hot:%d", i)
	}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		r := rand.New(rand.NewSource(time.Now().UnixNano())) // 每个goroutine独立随机数
		for pb.Next() {
			var key string
			// 10%概率使用热点Key（竞争），90%概率使用唯一Key（无竞争）
			if r.Intn(10) == 0 {
				key = hotKeys[r.Intn(len(hotKeys))]
			} else {
				key = fmt.Sprintf("bench:lock:part_compete:%d", time.Now().UnixNano()+r.Int63())
			}

			lock := NewRedisLock(client, Config{
				Key:        key,
				Expiration: 5 * time.Second,
			}, logx.WithContext(ctx))
			_ = lock.WithLock(ctx, func() error { return nil })
		}
	})
}

// ========== 场景3：全竞争（单一Key）- 最坏性能 ==========
func BenchmarkLock_Parallel_FullCompete(b *testing.B) {
	client := getTestRedisClient()
	ctx := context.Background()
	fullCompeteKey := "bench:lock:full_compete:single_key" // 所有请求竞争同一个Key

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			lock := NewRedisLock(client, Config{
				Key:        fullCompeteKey,
				Expiration: 5 * time.Second,
				RetryCount: 3, // 竞争场景下开启重试（贴合真实使用）
				RetryDelay: 100 * time.Millisecond,
			}, logx.WithContext(ctx))
			_ = lock.WithLock(ctx, func() error { return nil })
		}
	})
}

// 运行所有测试
func TestAll(t *testing.T) {
	fmt.Println("========== Redis分布式锁测试套件 ==========")

	t.Run("基本锁测试", TestBasicLockUnlock)
	t.Run("并发锁测试", TestConcurrentLock)
	t.Run("重入锁测试", TestLockReentry)
	t.Run("锁过期测试", TestLockExpiration)
	t.Run("看门狗测试", TestWatchdog)
	t.Run("高并发测试", TestHighConcurrency)
	t.Run("未持有锁释放测试", TestUnlockWithoutLock)
	t.Run("Context取消测试", TestContextCancellation)
	t.Run("竞态条件测试", TestRaceCondition)

	fmt.Println("========== 所有测试完成 ==========")
}
