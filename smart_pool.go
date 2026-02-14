package redcon

import (
	"errors"
	"sync"

	"github.com/panjf2000/ants/v2"
)

// Task 封装请求上下文
type Task struct {
	Handler func(drop int) // 具体的 Redis 处理逻辑
	Drop    int
}

// Bucket 每个连接的状态桶
type Bucket struct {
	id      string
	tasks   chan *Task
	running bool // 退化为普通 bool
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
func (p *SmartPool) Submit(id string, handler func(drop int)) error {
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
	for {
		// 1. 批量处理：尽可能在不加锁的情况下清空当前任务队列，提高吞吐量
		for {
			select {
			case t := <-b.tasks:
				if t.Handler != nil {
					t.Handler(t.Drop)
					t.Handler = nil // 显式释放引用，帮助 GC
				}
				taskPool.Put(t) // 任务处理完立即归还到对象池
			default:
				// 当前没有任务，准备进入收尾阶段
				goto CHECK_EMPTY
			}
		}

	CHECK_EMPTY:
		// 2. 状态收敛：在分片锁的保护下执行清理，防止与 Submit 产生竞态
		s.mu.Lock()

		// 二次检查：如果在 select 判定为空到获取锁的间隙有新任务入队
		if len(b.tasks) > 0 {
			s.mu.Unlock()
			continue // 锁内发现新任务，解锁并跳回外层循环继续处理
		}

		// 3. 彻底解绑：在锁内完成“从 Map 移除”和“重置运行状态”
		// 这样可以确保 Submit 函数在锁内执行 exists 检查时，逻辑完全闭环
		delete(s.buckets, id)
		b.running = false
		b.id = "" // 清空身份标识，彻底消除指针悬挂导致的“发错人”风险
		s.mu.Unlock()

		// 4. 安全归还：此时该 Bucket 已从分片映射中剔除，且没有 Worker 在运行
		// 即使 Submit 在此瞬间进入，它也会因为 exists=false 而从池中获取新的 Bucket
		bucketPool.Put(b)
		return // 当前 Worker 协程功成身退
	}
}
