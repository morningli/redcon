package redcon

import (
	"context"
	"errors"
	"fmt"
	"github.com/panjf2000/gnet/v2"
	"sync"
	"time"
)

type Task struct {
	Ctx     context.Context
	Drop    int
	Handler func(ctx context.Context)
}

type Bucket struct {
	mu     sync.RWMutex
	tasks  chan *Task
	conn   *conn
	drop   int
	cancel context.CancelFunc
	err    error
}

type shard struct {
	mu      sync.RWMutex
	buckets map[string]*Bucket
}

type GPerC struct {
	shards    []*shard
	shardMask uint32
}

var (
	// 对象池：复用 Task 结构
	taskPool = sync.Pool{New: func() interface{} { return &Task{} }}
	// 对象池：复用连接桶，避免 3w 连接频繁分配内存
	bucketPool = sync.Pool{New: func() interface{} {
		return &Bucket{tasks: make(chan *Task, 128)}
	}}
)

func (b *Bucket) handleDrops(drop int) {
	c := b.conn
	buff := NewBuffer()
	for i := 0; i < drop; i++ {
		_, _ = buff.Write(ErrQueueOverflow)
	}

	err := c.conn.AsyncWritev(buff.Data(), func(_ gnet.Conn, err error) error {
		if err != nil {
			buff.Free()
			_ = c.close()
			return err
		}
		buff.Free()
		return nil
	})
	if err != nil {
		buff.Free()
		_ = c.close()
		return
	}
}

func (b *Bucket) handleError() {
	c := b.conn
	err_ := c.conn.AsyncWrite([]byte("-ERR "+b.err.Error()), func(_ gnet.Conn, err error) error {
		_ = c.close()
		return err
	})
	if err_ != nil {
		_ = c.close()
	}
}

func (b *Bucket) Close() {
	b.cancel()
}

func NewGPerC(shardCount int) (*GPerC, error) {
	p := &GPerC{
		shards:    make([]*shard, shardCount),
		shardMask: uint32(shardCount - 1),
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

func (c *GPerC) Register(id string, conn *conn) {

	h := fnv34a(id)
	s := c.shards[h&c.shardMask]

	s.mu.Lock()
	defer s.mu.Unlock()

	b, exists := s.buckets[id]
	if exists {
		return
	}

	b = bucketPool.Get().(*Bucket)
	b.drop = 0
	b.err = nil
	b.conn = conn
	ctx, cancel := context.WithCancel(context.Background())
	b.cancel = cancel
	go b.processConn(ctx)
	s.buckets[id] = b
}

func (c *GPerC) Unregister(id string) {
	fmt.Println("Unregister")

	h := fnv34a(id)
	s := c.shards[h&c.shardMask]

	s.mu.Lock()
	defer s.mu.Unlock()
	b, exists := s.buckets[id]
	if !exists {
		return
	}

	b.cancel()
	delete(s.buckets, id)
}

func (c *GPerC) SubmitError(ctx context.Context, id string, err error) {
	fmt.Println("SubmitError")

	h := fnv34a(id)
	s := c.shards[h&c.shardMask]

	var b *Bucket

	s.mu.RLock()
	var ok bool
	if b, ok = s.buckets[id]; !ok {
		s.mu.RUnlock()
	}
	s.mu.RUnlock()

	b.mu.Lock()
	defer b.mu.Unlock()
	b.err = err
}

// Submit 相同ID的任务会确保执行顺序，必须前一个任务处理完后再执行后一个
func (c *GPerC) Submit(ctx context.Context, id string, handler func(ctx context.Context)) error {
	fmt.Println("Submit")

	h := fnv34a(id)
	s := c.shards[h&c.shardMask]

	var b *Bucket

	s.mu.RLock()
	var ok bool
	if b, ok = s.buckets[id]; !ok {
		s.mu.RUnlock()
		return errors.New("no such bucket")
	}
	s.mu.RUnlock()

	// 1. 获取 Task 对象
	t := taskPool.Get().(*Task)
	t.Ctx = ctx
	t.Drop = b.drop
	t.Handler = handler

	b.mu.Lock()
	defer b.mu.Unlock()

	if b.err != nil {
		return b.err
	}

	// 2. 尝试非阻塞入队
	select {
	case b.tasks <- t:
		b.drop = 0
	default:
		// 3. 队列满了：立即释放锁并丢弃任务，记录丢弃数量（在锁内更新，避免竞态/丢失）
		b.drop++
		taskPool.Put(t)

		// 可选：记录丢弃日志或上报监控指标
		// metrics.Incr("proxy.request.drop")
		// fmt.Printf("connection %s queue full, request dropped\n", id)
		return errors.New("task pool full")
	}
	return nil
}

func (b *Bucket) processConn(ctx context.Context) {
	t := time.NewTicker(time.Millisecond * 10)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case t := <-b.tasks:
			if t.Drop > 0 {
				b.handleDrops(t.Drop)
			}
			t.Handler(t.Ctx)
			taskPool.Put(t)
		case <-t.C:
			if len(b.tasks) > 0 {
				continue
			}
			drop := 0
			var err error
			b.mu.Lock()
			if len(b.tasks) > 0 {
				b.mu.Unlock()
				continue
			}
			drop = b.drop
			err = b.err
			b.drop = 0
			b.mu.Unlock()
			if drop > 0 {
				b.handleDrops(drop)
			}
			if err != nil {
				b.handleError()
				_ = b.conn.close()
			}
		}
	}
}
