package redcon

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/panjf2000/ants/v2"
)

// Task 封装请求上下文
type Task struct {
	Ctx     context.Context // 带有超时的上下文
	Handler func(ctx context.Context, dropped int)
	Drop    int
}

type Bucket struct {
	tasks   chan *Task
	id      string
	running bool
	drop    int
}

var (
	// 对象池：复用 Task 结构
	taskPool = sync.Pool{New: func() interface{} { return &Task{} }}
	// 对象池：复用连接桶，避免 3w 连接频繁分配内存
	bucketPool = sync.Pool{New: func() interface{} {
		return &Bucket{tasks: make(chan *Task, 128)}
	}}
)

// SmartPool 基于 worker pool 提供“按连接串行、跨连接并行”的任务执行能力。
type SmartPool struct {
	shards     []*shard
	workerPool *ants.Pool
	shardMask  uint32
}

type shard struct {
	mu      sync.RWMutex
	buckets map[string]*Bucket
}

// NewSmartPool 推荐 shardCount=1024, antsSize=实际核心数*2~10
func NewSmartPool(shardCount int, antsSize int) (*SmartPool, error) {
	wp, err := ants.NewPool(antsSize, ants.WithNonblocking(false))
	if err != nil {
		return nil, err
	}

	p := &SmartPool{
		shards:     make([]*shard, shardCount),
		workerPool: wp,
		shardMask:  uint32(shardCount - 1),
	}
	for i := 0; i < shardCount; i++ {
		p.shards[i] = &shard{buckets: make(map[string]*Bucket)}
	}
	return p, nil
}

// 快速 Hash
func fnv34a(key string) uint32 {
	hash := uint32(2166136261)
	for i := 0; i < len(key); i++ {
		hash ^= uint32(key[i])
		hash *= 16777619
	}
	return hash
}

// Submit 相同ID的任务会确保执行顺序，必须前一个任务处理完后再执行后一个
func (p *SmartPool) Submit(id string, handler func(ctx context.Context, drop int)) error {
	h := fnv34a(id)
	s := p.shards[h&p.shardMask]

	s.mu.Lock()
	bucket, exists := s.buckets[id]
	if !exists {
		bucket = bucketPool.Get().(*Bucket)
		bucket.id = id
		bucket.running = false
		s.buckets[id] = bucket
	}

	// 1. 获取 Task 对象
	t := taskPool.Get().(*Task)
	t.Handler = handler
	t.Drop = bucket.drop

	// 2. 尝试非阻塞入队
	select {
	case bucket.tasks <- t:
		// 入队成功：检查并启动 Worker
		if !bucket.running {
			bucket.running = true
			_ = p.workerPool.Submit(func() { p.processConn(id, bucket, s) })
		}
		bucket.drop = 0
		s.mu.Unlock()
	default:
		// 3. 队列满了：立即释放锁并丢弃任务
		s.mu.Unlock()

		// 归还 Task 对象，防止内存泄漏
		t.Handler = nil
		taskPool.Put(t)
		bucket.drop++

		// 可选：记录丢弃日志或上报监控指标
		// metrics.Incr("proxy.request.drop")
		// fmt.Printf("connection %s queue full, request dropped\n", id)
		return errors.New("task pool full")
	}
	return nil
}

func (p *SmartPool) processConn(id string, b *Bucket, s *shard) {
	const (
		maxQuota = 64
		maxCost  = 500 * time.Millisecond
	)

	// 1. 记录开始时间
	start := time.Now()
	quota := maxQuota

	for {
		for quota > 0 {
			select {
			case t := <-b.tasks:

				// 2. 执行业务逻辑，传入 Context
				if t.Handler != nil {
					t.Handler(t.Ctx, t.Drop)
					t.Handler = nil
				}

				// 3. 计算耗时并识别慢连接
				cost := time.Since(start)
				if cost > maxCost {
					// 识别为顽固慢连接，可以在此处记录日志或标记熔断
					quota = 1
				}

				taskPool.Put(t)
				quota--
			default:
				goto CHECK_EMPTY
			}
		}

		// 4. 配额用尽，异步让出 (逻辑同前)
		if len(b.tasks) > 0 {
			go func() {
				_ = p.workerPool.Submit(func() { p.processConn(id, b, s) })
			}()
			return
		}

	CHECK_EMPTY:
		s.mu.Lock()
		if len(b.tasks) > 0 {
			s.mu.Unlock()
			continue
		}
		delete(s.buckets, id)
		b.running = false
		b.id = ""
		s.mu.Unlock()

		bucketPool.Put(b)
		return
	}
}
