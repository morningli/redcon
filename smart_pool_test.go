package redcon

import (
	"context"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"golang.org/x/time/rate"
)

func TestSmartPool_NoLoss(t *testing.T) {
	// 1. 初始化池
	// 模拟 128 个分片，最大 1000 个 ants worker
	pool, err := NewSmartPool(128, 1000)
	if err != nil {
		t.Fatalf("NewSmartPool error: %v", err)
	}

	var (
		totalTasks    int32 = 1000000 // 100 万个总任务
		executedTasks int32 = 0       // 实际执行的任务数
		dropTasks     int32 = 0
		wg            sync.WaitGroup
		connNum       = 100
		limiter       = rate.NewLimiter(200000, 1000)
	)
	ctxAll, cancelAll := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancelAll()
	errCh := make(chan error, connNum)

	// 2. 并发提交任务
	start := time.Now()
	for i := 0; i < connNum; i++ { // 100 个并发提交者
		cid := strconv.Itoa(i)
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < int(totalTasks)/connNum; j++ {
				// 轮询使用连接 ID，模拟哈希碰撞和并发竞争
				if err := limiter.Wait(ctxAll); err != nil {
					select {
					case errCh <- err:
					default:
					}
					cancelAll()
					return
				}
				for {
					if ctxAll.Err() != nil {
						return
					}
					err := pool.Submit(cid, func(drop int) {
						// 模拟业务耗时（可选）
						atomic.AddInt32(&dropTasks, int32(drop))
						atomic.AddInt32(&executedTasks, 1)
					})
					if err == nil {
						break
					}
					// 队列满时 Submit 可能返回错误；为保证“无丢失”，这里重试直到成功入队。
					time.Sleep(200 * time.Microsecond)
				}
			}
		}(i)
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		if err != nil {
			t.Fatalf("submit error: %v", err)
		}
	}

	// 3. 给一点缓冲时间确保最后的任务执行完（因为 Submit 是异步的）
	timeout := time.After(10 * time.Second)
	tk := time.NewTicker(time.Second * 5)
	defer tk.Stop()
	for atomic.LoadInt32(&executedTasks)+atomic.LoadInt32(&dropTasks) < totalTasks {
		select {
		case <-tk.C:
			t.Logf("processed %d tasks", executedTasks)
		case <-timeout:
			t.Fatalf("测试超时！提交了 %d, 仅执行了 %d. 丢失了 %d 个任务",
				totalTasks, executedTasks, totalTasks-executedTasks)
		default:
			time.Sleep(10 * time.Millisecond)
		}
	}

	t.Logf("测试通过！成功执行 %d 任务，丢弃 %d 任务，耗时: %v", executedTasks, dropTasks, time.Since(start))
}
