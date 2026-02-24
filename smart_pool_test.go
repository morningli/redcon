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
					err := pool.Submit(cid, nil, func(_ context.Context) {
						// 模拟业务耗时（可选）
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
	for atomic.LoadInt32(&executedTasks) < totalTasks {
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

	t.Logf("测试通过！成功执行 %d 任务，耗时: %v", executedTasks, time.Since(start))
}

func TestSmartPool_Fairness(t *testing.T) {
	// 初始化池：为了观察穿插效果，限制 worker 数为 1
	// 这样可以极大地放大“霸占线程”的现象
	pool, _ := NewSmartPool(128, 1)

	var (
		connA         = "heavy-pipeline-client"
		connB         = "light-short-client"
		tasksA        = 128 // A 发送 128 个, 不超过chan长度
		tasksB        = 10  // B 只发 10 个
		executedA     int32
		executedB     int32
		firstBFinishA int32 // 当 B 完成时，A 已经执行了多少个
		wg            sync.WaitGroup
	)

	wg.Add(2)

	// 1. 先启动连接 A 的大 Pipeline
	go func() {
		defer wg.Done()
		for i := 0; i < tasksA; i++ {
			pool.Submit(connA, nil, func(context.Context) {
				// 模拟耗时，放大 CPU 占用
				time.Sleep(time.Millisecond)
				atomic.AddInt32(&executedA, 1)
			})
		}
	}()

	// 2. 稍微延迟一下，确保 A 已经占住了唯一的线程
	time.Sleep(50 * time.Millisecond)

	// 3. 在 A 运行期间，提交 B 的短请求
	go func() {
		defer wg.Done()
		for i := 0; i < tasksB; i++ {
			_ = pool.Submit(connB, nil, func(_ context.Context) {
				time.Sleep(time.Millisecond)
				atomic.AddInt32(&executedB, 1)
			})
		}
		// 当 B 的所有任务提交并执行完（此处 Submit 是异步的，所以要在内部等待 B 完成）
		for atomic.LoadInt32(&executedB) < int32(tasksB) {
			time.Sleep(time.Millisecond)
		}
		// 记录此刻 A 的进度
		atomic.StoreInt32(&firstBFinishA, atomic.LoadInt32(&executedA))
	}()

	wg.Wait()

	// 等待 A 彻底跑完
	for atomic.LoadInt32(&executedA) < int32(tasksA) {
		time.Sleep(time.Millisecond)
	}

	// 4. 验证结论
	finishedA := atomic.LoadInt32(&firstBFinishA)
	t.Logf("当短连接 B 完成时，长连接 A 执行了 %d/%d 个任务", finishedA, tasksA)

	if finishedA >= int32(tasksA) {
		t.Errorf("调度不公平！短连接 B 被长连接 A 彻底阻塞了")
	} else {
		t.Logf("调度公平！短连接 B 成功穿插在长连接 A 运行期间完成")
	}
}
