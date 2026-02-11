package redcon

import (
	"context"
	"golang.org/x/time/rate"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestSmartPool_NoLoss(t *testing.T) {
	// 1. 初始化池
	// 模拟 128 个分片，最大 1000 个 ants worker
	pool, _ := NewSmartPool(128, 1000)

	var (
		totalTasks    int32 = 1000000 // 100 万个总任务
		executedTasks int32 = 0       // 实际执行的任务数
		dropTasks     int32 = 0
		wg            sync.WaitGroup
		connNum       = 100
		limiter       = rate.NewLimiter(200000, 1000)
	)

	// 2. 并发提交任务
	start := time.Now()
	for i := 0; i < connNum; i++ { // 100 个并发提交者
		cid := strconv.Itoa(i)
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < int(totalTasks)/connNum; j++ {
				// 轮询使用连接 ID，模拟哈希碰撞和并发竞争
				_ = limiter.Wait(context.Background())
				err := pool.Submit(cid, func(drop int) {
					// 模拟业务耗时（可选）
					atomic.AddInt32(&dropTasks, int32(drop))
					atomic.AddInt32(&executedTasks, 1)
				})
				if err != nil {
					t.Log("Submit fail:" + err.Error())
				}
			}
		}(i)
	}
	wg.Wait()

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
