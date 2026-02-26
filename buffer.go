package redcon

import (
	"errors"
	"fmt"
	"sync"
)

const (
	// SmallChunkSize 表示小页（第一页）的字节大小。
	SmallChunkSize = 256
	// bigShift 表示大页大小的 2 次幂指数：$2^{12}=4096$。
	bigShift = 12
	// ChunkSize 表示大页（Chunk）的字节大小。
	ChunkSize = 1 << bigShift
	// bigMask 用于等价替代 `% ChunkSize`。
	bigMask = 1<<bigShift - 1
)

// 注意：Buffer 采用“第一页小页、后续大页”的策略：
// - 第 0 页固定为 256B 小页
// - 当超过 256B 后，后续追加 4KB 大页

// Chunk 是 Buffer 使用的固定大小大页（从对象池复用）。
type Chunk [ChunkSize]byte

var chunkPool = sync.Pool{
	New: func() interface{} { return new(Chunk) },
}

func getBigChunk() *Chunk { return chunkPool.Get().(*Chunk) }
func putBigChunk(c *Chunk) {
	if c != nil {
		chunkPool.Put(c)
	}
}

// SmallChunk 是 Buffer 使用的固定大小小页（从对象池复用）。
type SmallChunk [SmallChunkSize]byte

var smallChunkPool = sync.Pool{
	New: func() interface{} { return new(SmallChunk) },
}

func getSmallChunk() *SmallChunk { return smallChunkPool.Get().(*SmallChunk) }
func putSmallChunk(c *SmallChunk) {
	if c != nil {
		smallChunkPool.Put(c)
	}
}

// Buffer 是基于固定大小页的可增长字节缓冲区，支持零拷贝 Slice。
type Buffer struct {
	// hasSmall 表示该 Buffer 的起始页是否为 small（256B）。
	// NewBuffer 创建的 Buffer 恒为 true；Split 得到的 remainder 可能为 false（零拷贝所需）。
	hasSmall        bool
	firstPageOffset int // 第一页的起始有效数据偏移（0 ~ pageSize-1）
	// small 仅用于第一页（256B）。当 Buffer 的起始页为大页时 small==nil。
	small *SmallChunk
	// big 保存后续所有 4KB 页；当 Buffer 起始页为大页时，big[0] 即第一页。
	big []*Chunk

	length int // 逻辑上的总有效数据长度

}

// NewBuffer 创建一个空 Buffer。
func NewBuffer() *Buffer { return &Buffer{hasSmall: true} }

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func atByte(hasSmall bool, small *SmallChunk, big []*Chunk, firstPageOffset int, index int) byte {
	physicalOff := index + firstPageOffset
	if hasSmall {
		if physicalOff < SmallChunkSize {
			return small[physicalOff]
		}
		off2 := physicalOff - SmallChunkSize
		pageIdx := off2 >> bigShift
		innerOff := off2 & bigMask
		return big[pageIdx][innerOff]
	}
	pageIdx := physicalOff >> bigShift
	innerOff := physicalOff & bigMask
	return big[pageIdx][innerOff]
}

func dataSlices(hasSmall bool, small *SmallChunk, big []*Chunk, firstPageOffset, length int) [][]byte {
	if length <= 0 {
		return nil
	}

	result := make([][]byte, 0, 1+len(big))
	remaining := length
	currOff := firstPageOffset

	// Optional small first page.
	if hasSmall && small == nil {
		return nil
	}
	if hasSmall {
		canRead := SmallChunkSize - currOff
		actualRead := min(canRead, remaining)
		result = append(result, small[currOff:currOff+actualRead])
		remaining -= actualRead
		currOff = 0
	}

	// Big pages: shared loop for both hasSmall==true/false.
	for i := 0; i < len(big) && remaining > 0; i++ {
		start := currOff // only non-zero for the first big page when hasSmall==false
		canRead := ChunkSize - start
		actualRead := min(canRead, remaining)
		result = append(result, big[i][start:start+actualRead])
		remaining -= actualRead
		currOff = 0
	}
	return result
}

func sliceFromPhysical(hasSmall bool, small *SmallChunk, big []*Chunk, physicalStart, length int) *BufferView {
	if hasSmall {
		if physicalStart < SmallChunkSize {
			return &BufferView{
				hasSmall:        true,
				small:           small,
				big:             big,
				length:          length,
				firstPageOffset: physicalStart,
			}
		}
		off2 := physicalStart - SmallChunkSize
		startBigIdx := off2 >> bigShift
		newFirstOff := off2 & bigMask
		return &BufferView{
			hasSmall:        false,
			small:           nil,
			big:             big[startBigIdx:],
			length:          length,
			firstPageOffset: newFirstOff,
		}
	}

	startBigIdx := physicalStart >> bigShift
	newFirstOff := physicalStart & bigMask
	return &BufferView{
		hasSmall:        false,
		small:           nil,
		big:             big[startBigIdx:],
		length:          length,
		firstPageOffset: newFirstOff,
	}
}

// makeSuffixFirstPage allocates a new first page and copies suffix bytes into the END part of that page.
// It never reuses the boundary page from the old buffer.
func makeSuffixFirstPage(suffix []byte) (hasSmall bool, small *SmallChunk, big []*Chunk, firstOff int) {
	if len(suffix) <= SmallChunkSize {
		ns := getSmallChunk()
		firstOff = SmallChunkSize - len(suffix)
		copy(ns[firstOff:], suffix)
		return true, ns, nil, firstOff
	}
	nb := getBigChunk()
	firstOff = ChunkSize - len(suffix)
	copy(nb[firstOff:], suffix)
	return false, nil, []*Chunk{nb}, firstOff
}

// Write 向 Buffer 末尾追加数据
func (b *Buffer) Write(p []byte) (int, error) {
	total := len(p)
	srcOff := 0

	// If this is the very first write and it's already larger than the small page,
	// start with a big page and skip allocating the small page.
	if b.length == 0 && b.firstPageOffset == 0 && b.small == nil && len(b.big) == 0 && b.hasSmall {
		if total > SmallChunkSize {
			b.hasSmall = false
		}
	}

	for srcOff < total {
		physicalLen := b.length + b.firstPageOffset
		prefix := 0
		if b.hasSmall {
			prefix = SmallChunkSize
		}

		// First page is small when hasSmall==true. Try to write into the small page first.
		if b.hasSmall && physicalLen < SmallChunkSize {
			if b.small == nil {
				b.small = getSmallChunk()
			}
			innerOff := physicalLen
			canWrite := SmallChunkSize - innerOff
			copyLen := min(canWrite, total-srcOff)
			copy(b.small[innerOff:], p[srcOff:srcOff+copyLen])

			srcOff += copyLen
			b.length += copyLen
			continue
		}

		// Write into big pages (shared for hasSmall==true/false).
		bigPos := physicalLen - prefix
		pageIdx := bigPos >> bigShift
		innerOff := bigPos & bigMask
		for pageIdx >= len(b.big) {
			b.big = append(b.big, getBigChunk())
		}
		currPage := b.big[pageIdx]
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

	length := m - n
	physicalStart := n + b.firstPageOffset
	return sliceFromPhysical(b.hasSmall, b.small, b.big, physicalStart, length)
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
			small:           b.small,
			big:             b.big,
			length:          b.length,
			firstPageOffset: b.firstPageOffset,
			hasSmall:        b.hasSmall,
		}
		b.small, b.big, b.length, b.firstPageOffset, b.hasSmall = nil, nil, 0, 0, true
		return newBuf
	}
	if n >= b.length {
		// 情况：n 超过长度，b 保留所有，返回一个空的 Buffer
		return NewBuffer()
	}

	originalSmall := b.small
	originalBig := b.big
	originalLen := b.length
	originalFPO := b.firstPageOffset
	originalHasSmall := b.hasSmall

	splitPos := n + originalFPO
	newBuf := NewBuffer()
	newLen := originalLen - n

	// prefix is the size of the first page in bytes (small page when present).
	prefix := 0
	if originalHasSmall {
		prefix = SmallChunkSize
	}

	if originalHasSmall {
		// Split in small page
		if splitPos < SmallChunkSize {
			if originalSmall == nil {
				// Shouldn't happen: data exists but small nil. Be defensive.
				originalSmall = getSmallChunk()
				b.small = originalSmall
			}
			// b keeps the original small page (no copy); newBuf gets a fresh small page for suffix.
			b.small = originalSmall
			b.big = nil
			b.hasSmall = true
			b.length = n
			// b.firstPageOffset unchanged

			// newBuf: allocate a new small page and copy the suffix bytes into the END part.
			suffixLen := min(SmallChunkSize-splitPos, newLen)
			suffix := originalSmall[splitPos : splitPos+suffixLen]
			newHasSmall, newSmall, newBigFirst, newFirstOff := makeSuffixFirstPage(suffix)
			newBuf.hasSmall = newHasSmall
			newBuf.small = newSmall
			newBuf.firstPageOffset = newFirstOff
			if len(newBigFirst) > 0 {
				// Should never happen since suffixLen <= SmallChunkSize, but keep consistent.
				newBuf.big = append(newBigFirst, originalBig...)
			} else {
				newBuf.big = originalBig
			}
			newBuf.length = newLen
			return newBuf
		}

		// Boundary right after small page
		if splitPos == SmallChunkSize {
			b.small = originalSmall
			b.big = nil
			b.hasSmall = true
			b.length = n

			newBuf.small = nil
			newBuf.big = originalBig
			newBuf.hasSmall = false
			newBuf.firstPageOffset = 0
			newBuf.length = newLen
			return newBuf
		}
	}

	// Split in big pages (shared for hasSmall=true and hasSmall=false).
	bigSplitPos := splitPos - prefix
	splitBigIdx := bigSplitPos >> bigShift
	innerOff := bigSplitPos & bigMask

	// b keeps pages up to the boundary page; remainder never reuses the boundary page.
	b.small = originalSmall
	b.hasSmall = originalHasSmall
	if innerOff > 0 {
		b.big = append([]*Chunk(nil), originalBig[:splitBigIdx+1]...)
	} else {
		b.big = append([]*Chunk(nil), originalBig[:splitBigIdx]...)
	}
	b.length = n
	// b.firstPageOffset unchanged

	if innerOff == 0 {
		// Boundary on big page edge: remainder can take pages without copying.
		newBuf.small = nil
		newBuf.hasSmall = false
		newBuf.big = originalBig[splitBigIdx:]
		newBuf.firstPageOffset = 0
		newBuf.length = newLen
		return newBuf
	}

	// Split within a big page: copy suffix to a fresh first page for newBuf.
	suffixInThisPage := min(ChunkSize-innerOff, newLen)
	suffix := originalBig[splitBigIdx][innerOff : innerOff+suffixInThisPage]
	newHasSmall, newSmall, newBigFirst, newFirstOff := makeSuffixFirstPage(suffix)
	newBuf.hasSmall = newHasSmall
	newBuf.small = newSmall
	newBuf.firstPageOffset = newFirstOff
	if splitBigIdx+1 < len(originalBig) {
		if len(newBigFirst) > 0 {
			newBuf.big = append(newBigFirst, originalBig[splitBigIdx+1:]...)
		} else {
			newBuf.big = originalBig[splitBigIdx+1:]
		}
	} else {
		newBuf.big = newBigFirst
	}
	newBuf.length = newLen
	return newBuf
}

// Free 释放内存
func (b *Buffer) Free() {
	// 注意：在 Split 场景下，多个 Buffer 可能引用同一个 Chunk
	// 这里简单的 Put 回池子仅适用于你确定该 Buffer 独占这些 Chunk 的情况
	// 如果需要严谨，需要引入引用计数。但在 gnet 解析完即销毁的场景，
	// 通常在处理完最后一个 Split 块后统一释放即可。
	if b.hasSmall && b.small != nil {
		putSmallChunk(b.small)
	}
	b.small = nil
	for i := range b.big {
		putBigChunk(b.big[i])
	}
	b.big = nil
	b.length = 0
	b.firstPageOffset = 0
}

// Data 返回当前 Buffer 逻辑范围内所有物理块的切片引用。
// 这是一个零拷贝操作，返回的 []byte 直接指向内存池中的物理内存。
func (b *Buffer) Data() [][]byte {
	return dataSlices(b.hasSmall, b.small, b.big, b.firstPageOffset, b.length)
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
	return atByte(b.hasSmall, b.small, b.big, b.firstPageOffset, index)
}

// Swap 交换两个 IndexedBuffer 的所有权
// 这是一个 O(1) 操作，仅交换指针和逻辑位移
func (b *Buffer) Swap(other *Buffer) {
	if b == nil || other == nil {
		return
	}

	b.small, other.small = other.small, b.small
	b.big, other.big = other.big, b.big
	b.hasSmall, other.hasSmall = other.hasSmall, b.hasSmall

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
	small    *SmallChunk
	big      []*Chunk
	hasSmall bool
	// length 是该视图的逻辑总长度
	length int
	// firstPageOffset 是该视图在第一页中的起始物理偏移
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
	return atByte(v.hasSmall, v.small, v.big, v.firstPageOffset, index)
}

// Data 将视图转换为不连续的字节切片列表
// 常用于 net.Buffers 或 writev 系统调用
func (v *BufferView) Data() [][]byte {
	return dataSlices(v.hasSmall, v.small, v.big, v.firstPageOffset, v.length)
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
	return sliceFromPhysical(v.hasSmall, v.small, v.big, physicalStart, length)
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

// GetSmallBuffer 从池中获取一个 256B 小页。
func GetSmallBuffer() *SmallChunk {
	return smallChunkPool.Get().(*SmallChunk)
}

// PutSmallBuffer 将小页归还至对象池。
func PutSmallBuffer(b *SmallChunk) {
	smallChunkPool.Put(b)
}
