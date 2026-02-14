package redcon

import (
	"errors"
	"fmt"
	"sync"
)

// ChunkSize 表示每个物理页（Chunk）的字节大小。
const ChunkSize = 4096

// Chunk 是 Buffer 使用的固定大小内存页（从对象池复用）。
type Chunk [ChunkSize]byte

var chunkPool = sync.Pool{
	New: func() interface{} { return new(Chunk) },
}

// Buffer 是基于固定大小页的可增长字节缓冲区，支持零拷贝 Slice。
type Buffer struct {
	pages           []*Chunk
	length          int // 逻辑上的总有效数据长度
	firstPageOffset int // 第一页的起始有效数据偏移（0 ~ ChunkSize-1）
}

// NewBuffer 创建一个空 Buffer。
func NewBuffer() *Buffer {
	return &Buffer{
		pages: make([]*Chunk, 0, 8),
	}
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

// Write 向 Buffer 末尾追加数据
func (b *Buffer) Write(p []byte) (int, error) {
	total := len(p)
	srcOff := 0

	for srcOff < total {
		// 计算物理写入位置
		// 物理总长度 = 逻辑长度 + 第一页的缩进
		physicalLen := b.length + b.firstPageOffset
		pageIdx := physicalLen / ChunkSize
		innerOff := physicalLen % ChunkSize

		if pageIdx >= len(b.pages) {
			b.pages = append(b.pages, chunkPool.Get().(*Chunk))
		}

		currPage := b.pages[pageIdx]
		canWrite := ChunkSize - innerOff
		copyLen := min(canWrite, total-srcOff)

		copy(currPage[innerOff:], p[srcOff:srcOff+copyLen])

		srcOff += copyLen
		b.length += copyLen
	}
	return total, nil
}

// Slice 模拟 Go 原生切片操作 b[n:m]
// n: 起始位移 (inclusive)
// m: 结束位移 (exclusive)
func (b *Buffer) Slice(n, m int) *BufferView {
	if n < 0 || m < n || m > b.length {
		panic(fmt.Errorf("index out of range [%d:%d] with length %d", n, m, b.length))
	}

	length := m - n // 内部转换为长度逻辑
	physicalStart := n + b.firstPageOffset
	startPageIdx := physicalStart / ChunkSize
	newFirstOff := physicalStart % ChunkSize

	return &BufferView{
		pages:           b.pages[startPageIdx:],
		length:          length,
		firstPageOffset: newFirstOff,
	}
}

// Tail 返回从 n 到末尾的视图，等价于 Go 切片 b[n:].
func (b *Buffer) Tail(n int) *BufferView {
	return b.Slice(n, b.Len())
}

// Split 在 n 位置切断数据。
// 原有的 b 将保留 [0, n) 字节（完整指令，通过拷贝分割点实现物理隔离）。
// 返回的新 Buffer 将承接 [n, length) 字节（剩余流，通过位移实现零平移）。
func (b *Buffer) Split(n int) *Buffer {
	if n <= 0 {
		// 情况：n=0，原 b 变为空，所有数据移交给新返回的 Buffer
		newBuf := &Buffer{
			pages:           b.pages,
			length:          b.length,
			firstPageOffset: b.firstPageOffset,
		}
		b.pages, b.length, b.firstPageOffset = nil, 0, 0
		return newBuf
	}
	if n >= b.length {
		// 情况：n 超过长度，b 保留所有，返回一个空的 Buffer
		return NewBuffer()
	}

	// 1. 计算分割点在物理页中的绝对位置
	absoluteSplitPos := n + b.firstPageOffset
	splitPageIdx := absoluteSplitPos / ChunkSize
	innerOff := absoluteSplitPos % ChunkSize

	// 备份原始引用，用于重组
	originalPages := b.pages
	originalLen := b.length
	originalFPO := b.firstPageOffset

	newBuf := NewBuffer()

	if innerOff > 0 {
		// 情况：分割点在物理页中间
		sourcePage := originalPages[splitPageIdx]

		// --- A. 原 Buffer b (承接前半段指令: [0, n)) ---
		// 申请一个新物理页，实现物理隔离
		frontPage := chunkPool.Get().(*Chunk)

		startInPage := 0
		if splitPageIdx == 0 {
			startInPage = originalFPO
		}

		// 物理隔离：只拷贝 [startInPage : innerOff] 这部分数据
		copy(frontPage[startInPage:innerOff], sourcePage[startInPage:innerOff])

		// 重组 b 的页表：保留之前的完整页 + 这个新拷贝页
		b.pages = append(make([]*Chunk, 0, splitPageIdx+1), originalPages[:splitPageIdx]...)
		b.pages = append(b.pages, frontPage)
		b.length = n
		// b.firstPageOffset 保持不变

		// --- B. 返回的新 Buffer newBuf (承接后半段残余: [n, length)) ---
		// 直接接管从分割页开始的所有后续物理页
		newBuf.pages = append(make([]*Chunk, 0, len(originalPages)-splitPageIdx), originalPages[splitPageIdx:]...)
		newBuf.firstPageOffset = innerOff // 关键：新 Buffer 的逻辑起点是 innerOff
		newBuf.length = originalLen - n
	} else {
		// 情况：恰好在物理页边界切分 (innerOff == 0)
		b.pages = append(make([]*Chunk, 0, splitPageIdx), originalPages[:splitPageIdx]...)
		b.length = n
		// b.firstPageOffset 保持不变

		newBuf.pages = append(make([]*Chunk, 0, len(originalPages)-splitPageIdx), originalPages[splitPageIdx:]...)
		newBuf.firstPageOffset = 0 // 边界对齐，位移为 0
		newBuf.length = originalLen - n
	}

	return newBuf
}

// Free 释放内存
func (b *Buffer) Free() {
	// 注意：在 Split 场景下，多个 Buffer 可能引用同一个 Chunk
	// 这里简单的 Put 回池子仅适用于你确定该 Buffer 独占这些 Chunk 的情况
	// 如果需要严谨，需要引入引用计数。但在 gnet 解析完即销毁的场景，
	// 通常在处理完最后一个 Split 块后统一释放即可。
	for i := range b.pages {
		if b.pages[i] != nil {
			chunkPool.Put(b.pages[i])
			b.pages[i] = nil
		}
	}
	b.pages = b.pages[:0]
	b.length = 0
}

// Data 返回当前 Buffer 逻辑范围内所有物理块的切片引用。
// 这是一个零拷贝操作，返回的 []byte 直接指向内存池中的物理内存。
func (b *Buffer) Data() [][]byte {
	if b.length <= 0 {
		return nil
	}

	numPages := len(b.pages)
	result := make([][]byte, 0, numPages)

	remainingLen := b.length
	currOffset := b.firstPageOffset

	for i := 0; i < numPages && remainingLen > 0; i++ {
		// 计算当前页的有效起始和结束位置
		start := currOffset

		// 当前页能提供的有效长度
		canRead := ChunkSize - start
		actualRead := min(canRead, remainingLen)

		// 对物理 Chunk 进行切片引用
		// 注意：b.pages[i][start : start+actualRead] 不会产生内存拷贝
		result = append(result, b.pages[i][start:start+actualRead])

		// 后续页面的起始偏移必然是 0
		remainingLen -= actualRead
		currOffset = 0
	}

	return result
}

// Len 返回当前 Buffer 中逻辑有效的总字节数
func (b *Buffer) Len() int {
	return b.length
}

// At 返回逻辑偏移 index 处的单个字节
func (b *Buffer) At(index int) byte {
	if index < 0 || index >= b.length {
		panic(errors.New("index out of range"))
	}

	// 1. 核心公式：逻辑偏移转物理地址
	physicalOff := index + b.firstPageOffset

	// 2. 定位页和页内偏移
	pageIdx := physicalOff / ChunkSize
	innerOff := physicalOff % ChunkSize

	// 3. 直接从对应的物理页返回字节
	return b.pages[pageIdx][innerOff]
}

// Swap 交换两个 IndexedBuffer 的所有权
// 这是一个 O(1) 操作，仅交换指针和逻辑位移
func (b *Buffer) Swap(other *Buffer) {
	if b == nil || other == nil {
		return
	}

	// 交换页表切片 (指针和长度)
	b.pages, other.pages = other.pages, b.pages

	// 交换逻辑长度
	b.length, other.length = other.length, b.length

	// 交换起始偏移量
	b.firstPageOffset, other.firstPageOffset = other.firstPageOffset, b.firstPageOffset
}

// Bytes 将视图内容合并为一个连续的切片（涉及内存拷贝）
// 建议仅在必须对接只接收 []byte 的第三方 API 时使用
func (b *Buffer) Bytes() []byte {
	res := make([]byte, b.length)
	slices := b.Data()
	off := 0
	for _, s := range slices {
		copy(res[off:], s)
		off += len(s)
	}
	return res
}

// BufferView 是对 IndexedBuffer 部分片段的只读视图。
// 它通过引用原 Buffer 的页表并记录逻辑偏移来实现零拷贝操作。
type BufferView struct {
	// pages 引用了 IndexedBuffer 的物理页表片段
	// 注意：这里共享指针，不产生物理内存拷贝
	pages []*Chunk
	// length 是该视图的逻辑总长度
	length int
	// firstPageOffset 是该视图在第一页（pages[0]）中的起始物理偏移
	firstPageOffset int
}

// Len 返回视图的逻辑数据长度
func (v *BufferView) Len() int {
	return v.length
}

// IsEmpty 返回视图是否为空
func (v *BufferView) IsEmpty() bool {
	return v.length == 0
}

// At 支持随机访问，返回逻辑索引 index 处的字节
func (v *BufferView) At(index int) byte {
	if index < 0 || index >= v.length {
		panic("view index out of range")
	}

	// 物理地址 = 视图起始偏移 + 逻辑索引
	physicalOff := index + v.firstPageOffset
	pageIdx := physicalOff / ChunkSize
	innerOff := physicalOff % ChunkSize

	return v.pages[pageIdx][innerOff]
}

// Data 将视图转换为不连续的字节切片列表
// 常用于 net.Buffers 或 writev 系统调用
func (v *BufferView) Data() [][]byte {
	if v.length <= 0 {
		return nil
	}

	numPages := len(v.pages)
	result := make([][]byte, 0, numPages)

	remaining := v.length
	currOff := v.firstPageOffset

	for i := 0; i < numPages && remaining > 0; i++ {
		// 当前页可供读取的长度
		canRead := ChunkSize - currOff
		actualRead := min(canRead, remaining)

		// 引用物理页内存
		result = append(result, v.pages[i][currOff:currOff+actualRead])

		remaining -= actualRead
		currOff = 0 // 从第二页开始，偏移量重置为 0
	}
	return result
}

// Bytes 将视图内容合并为一个连续的切片（涉及内存拷贝）
// 建议仅在必须对接只接收 []byte 的第三方 API 时使用
func (v *BufferView) Bytes() []byte {
	res := make([]byte, v.length)
	slices := v.Data()
	off := 0
	for _, s := range slices {
		copy(res[off:], s)
		off += len(s)
	}
	return res
}

// Slice 在当前视图基础上再次切片 v[n:m]
func (v *BufferView) Slice(n, m int) *BufferView {
	if n < 0 || m < n || m > v.length {
		panic(fmt.Errorf("view index out of range [%d:%d] with length %d", n, m, v.length))
	}

	length := m - n
	physicalStart := n + v.firstPageOffset
	startPageIdx := physicalStart / ChunkSize
	newFirstOff := physicalStart % ChunkSize

	return &BufferView{
		pages:           v.pages[startPageIdx:],
		length:          length,
		firstPageOffset: newFirstOff,
	}
}

// Tail 返回从 n 到末尾的视图，等价于 Go 切片 v[n:].
func (v *BufferView) Tail(n int) *BufferView {
	return v.Slice(n, v.Len())
}

// GetBuffer 从池中获取一个干净的 Buffer 实例
func GetBuffer() *Chunk {
	return chunkPool.Get().(*Chunk)
}

// PutBuffer 释放 Buffer 持有的物理内存并将其归还至对象池
func PutBuffer(b *Chunk) {
	chunkPool.Put(b)
}
