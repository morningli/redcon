package redcon

import (
	"bufio"
	"errors"
	"fmt"
	"io"
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
	bigMask          = 1<<bigShift - 1
	initBigPageCount = 8
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
func NewBuffer() *Buffer {
	return &Buffer{
		hasSmall: true,
		big:      make([]*Chunk, 0, initBigPageCount),
	}
}

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
		physicalOff -= SmallChunkSize
	}
	pageIdx := physicalOff >> bigShift
	innerOff := physicalOff & bigMask
	return big[pageIdx][innerOff]
}

func dataSlice(hasSmall bool, small *SmallChunk, big []*Chunk, firstPageOffset, length int) []byte {
	if length <= 0 {
		return nil
	}

	// --- 1. 快速路径：判断是否为单页连续数据 (0 分配) ---
	if hasSmall {
		// 数据完全在 SmallChunk 内部 (0 ~ 255)
		if firstPageOffset+length <= SmallChunkSize {
			return small[firstPageOffset : firstPageOffset+length]
		}
	} else if len(big) > 0 {
		// 数据完全在第一个 BigChunk 内部 (0 ~ 4095)
		if firstPageOffset+length <= ChunkSize {
			return big[0][firstPageOffset : firstPageOffset+length]
		}
	}

	// --- 2. 慢速路径：跨页场景 (必须分配并合并) ---
	// 此时 pprof 中的 mallocgc 无法避免，但仅在处理大包或极端跨页时触发
	res := make([]byte, length)
	remaining := length
	currOff := firstPageOffset
	destOff := 0

	// 处理 SmallChunk 剩余部分
	if hasSmall {
		canRead := SmallChunkSize - currOff
		actualRead := min(canRead, remaining)
		copy(res[destOff:], small[currOff:currOff+actualRead])

		remaining -= actualRead
		destOff += actualRead
		currOff = 0 // 后续 BigChunk 从 0 开始读
	}

	// 顺序拷贝 BigChunks
	for i := 0; i < len(big) && remaining > 0; i++ {
		start := currOff
		canRead := ChunkSize - start
		actualRead := min(canRead, remaining)
		copy(res[destOff:], big[i][start:start+actualRead])

		remaining -= actualRead
		destOff += actualRead
		currOff = 0
	}

	return res
}

func dataSlices(hasSmall bool, small *SmallChunk, big []*Chunk, firstPageOffset, length int) [][]byte {
	if length <= 0 {
		return nil
	}

	remaining := length
	currOff := firstPageOffset

	// --- 核心优化：精确计算需要的切片数量 ---
	needed := 0
	tmpRemaining := length
	tmpOff := firstPageOffset

	if hasSmall {
		canRead := SmallChunkSize - tmpOff
		actualRead := min(canRead, tmpRemaining)
		needed++
		tmpRemaining -= actualRead
		tmpOff = 0
	}

	if tmpRemaining > 0 {
		// 计算跨越了多少个 Big Page
		// 第一页能读多少
		firstBigCanRead := ChunkSize - tmpOff
		if tmpRemaining <= firstBigCanRead {
			needed++
		} else {
			// 剩余长度 / 每页长度，向上取整
			needed += 1 + (tmpRemaining-firstBigCanRead+ChunkSize-1)/ChunkSize
		}
	}

	// 此时 make 的 capacity 是精确的，且对于小包（1-2页），needed 会很小
	// 编译器更容易对这种小规模分配进行优化
	result := make([][]byte, 0, needed)

	// --- 后续逻辑保持不变，确保正确性 ---
	if hasSmall {
		canRead := SmallChunkSize - currOff
		actualRead := min(canRead, remaining)
		result = append(result, small[currOff:currOff+actualRead])
		remaining -= actualRead
		currOff = 0
	}

	for i := 0; i < len(big) && remaining > 0; i++ {
		start := currOff
		canRead := ChunkSize - start
		actualRead := min(canRead, remaining)
		result = append(result, big[i][start:start+actualRead])
		remaining -= actualRead
		currOff = 0
	}
	return result
}

func sliceFromPhysical(hasSmall bool, small *SmallChunk, big []*Chunk, physicalStart, length int) BufferView {
	if hasSmall {
		if physicalStart < SmallChunkSize {
			return BufferView{
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
		return BufferView{
			hasSmall:        false,
			small:           nil,
			big:             big[startBigIdx:], // 仅拷贝切片头(24字节)，不重分配底层数组
			length:          length,
			firstPageOffset: newFirstOff,
		}
	}

	startBigIdx := physicalStart >> bigShift
	newFirstOff := physicalStart & bigMask
	return BufferView{
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

func (b *Buffer) Write(p []byte) (int, error) {
	total := len(p)
	srcOff := 0

	// 一次性保证目标容量够（避免多次 append）
	b.ensureCapacity(total)

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

//go:inline Slice 模拟 Go 原生切片操作 b[n:m],n: 起始位移 (inclusive),m: 结束位移 (exclusive)
func (b *Buffer) Slice(n, m int) BufferView {
	if n < 0 || m < n || m > b.length {
		panic(fmt.Errorf("index out of range [%d:%d] with length %d", n, m, b.length))
	}

	length := m - n
	physicalStart := n + b.firstPageOffset
	return sliceFromPhysical(b.hasSmall, b.small, b.big, physicalStart, length)
}

//go:inline Tail 返回从 n 到末尾的视图，等价于 Go 切片 b[n:].
func (b *Buffer) Tail(n int) BufferView {
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

//go:inline Data 返回当前 Buffer 逻辑范围内所有物理块的切片引用。这是一个零拷贝操作，返回的 []byte 直接指向内存池中的物理内存。
func (b *Buffer) Data() [][]byte {
	return dataSlices(b.hasSmall, b.small, b.big, b.firstPageOffset, b.length)
}

// Len 返回当前 Buffer 中逻辑有效的总字节数
func (b *Buffer) Len() int {
	return b.length
}

//go:inline At 返回逻辑偏移 index 处的单个字节
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

//go:inline Bytes 将视图内容合并为一个连续的切片（涉及内存拷贝）建议仅在必须对接只接收 []byte 的第三方 API 时使用
func (b *Buffer) Bytes() []byte {
	return dataSlice(b.hasSmall, b.small, b.big, b.firstPageOffset, b.length)
}

// ensureCapacity 确保至少还能写 n 字节（自动扩容多页）
func (b *Buffer) ensureCapacity(n int) {
	// If this is the very first write and it's already larger than the small page,
	// start with a big page and skip allocating the small page.
	if b.length == 0 && b.firstPageOffset == 0 && b.small == nil && len(b.big) == 0 && b.hasSmall {
		if n > SmallChunkSize {
			b.hasSmall = false
		}
	}

	physicalLen := b.length + b.firstPageOffset
	need := physicalLen + n

	totalCap := 0
	if b.hasSmall {
		if b.small == nil {
			b.small = getSmallChunk()
		}
		totalCap += SmallChunkSize
		if totalCap > need {
			return
		}
	}

	totalCap += len(b.big) * ChunkSize

	// 增加足够的新页
	for need > totalCap {
		b.big = append(b.big, getBigChunk())
		totalCap += ChunkSize
	}
}

// Reserve 预留 n 字节，返回当前页中的可写 slice。
// 若当前页剩余空间不足，会自动扩页。
func (b *Buffer) Reserve(n int) []byte {
	b.ensureCapacity(n)

	physLen := b.length + b.firstPageOffset
	prefix := 0
	if b.hasSmall {
		prefix = SmallChunkSize
	}

	if b.hasSmall && physLen < SmallChunkSize {
		// 当前写在 small page
		if b.small == nil {
			b.small = getSmallChunk()
		}
		innerOff := physLen
		remain := SmallChunkSize - innerOff

		// 若当前页空间足够，直接返回剩余切片，否则只返回可写区域
		if remain < n {
			// 返回当前页剩余部分（让上层分多次调用 Reserve）
			return b.small[innerOff:SmallChunkSize]
		}
		return b.small[innerOff : innerOff+n]
	}

	// 写在 big page
	bigPos := physLen - prefix
	pageIdx := bigPos >> bigShift
	innerOff := bigPos & bigMask
	if innerOff == ChunkSize {
		pageIdx++
		innerOff = 0
	}
	currPage := b.big[pageIdx]

	remain := ChunkSize - innerOff

	if remain < n {
		// 只返回当前页可写部分，下次 Reserve 再进下一页
		return currPage[innerOff:ChunkSize]
	}
	return currPage[innerOff : innerOff+n]
}

// Advance 前进写指针 n 字节（告诉 Buffer 实际写入了多少）
func (b *Buffer) Advance(n int) {
	b.length += n
}

// ReadFull 直接从 bufio.Reader 灌入，绕过 io.Reader 接口
func (b *Buffer) ReadFull(rd *bufio.Reader, n int) error {
	b.ensureCapacity(n)

	read := 0
	for read < n {
		// 获取当前页剩余的连续物理空间
		dest := b.Reserve(n - read)

		// 关键点：直接调用结构体方法，消除 assertI2I2 耗时
		nr, err := rd.Read(dest)
		if nr > 0 {
			b.Advance(nr)
			read += nr
		}
		if err != nil {
			return err
		}
	}
	return nil
}

// ReadFrom 尽可能多地从 bufio 缓冲区读取数据
// 即使数据跨越了多个物理 Chunk (4KB)，也能通过循环一次性读完
func (b *Buffer) ReadFrom(rd *bufio.Reader) (int64, error) {
	// 1. 获取当前 bufio 缓冲区中已有的字节数（无系统调用开销）
	n := rd.Buffered()
	if n <= 0 {
		// 如果缓冲区为空，这里可以选择直接返回 0，或者尝试一次阻塞 Read
		// 建议直接返回，让外层调度器决定何时进行下一次 Syscall
		return 0, nil
	}

	// 2. 预先确保 Buffer 有足够的物理 Chunk 承载这些数据
	b.ensureCapacity(n)

	var total int64
	remaining := n

	// 3. 循环填充：因为 Reserve 只能返回单页空间
	for remaining > 0 {
		// 获取当前物理页剩余空间 (max 4KB)
		dest := b.Reserve(remaining)
		if len(dest) == 0 {
			break // 理论上不应发生，因为已执行 ensureCapacity
		}

		// 4. 关键：直接调用 *bufio.Reader.Read (Inline 友好)
		// 由于数据已在 bufio 缓冲区，这里仅执行 runtime.memmove
		nr, err := rd.Read(dest)
		if nr > 0 {
			b.Advance(nr)
			total += int64(nr)
			remaining -= nr
		}

		if err != nil {
			// 在读取 Buffered 数据时通常不会有 err，除非底层连接断开
			return total, err
		}
	}

	return total, nil
}

func (b *Buffer) WriteTo(wr io.Writer) (int64, error) {
	if b.length <= 0 {
		return 0, nil
	}

	var total int64
	remaining := b.length
	currOff := b.firstPageOffset

	// 1. 处理 SmallChunk (如果有)
	if b.hasSmall {
		actualRead := min(SmallChunkSize-currOff, remaining)
		n, err := wr.Write(b.small[currOff : currOff+actualRead])
		total += int64(n)
		if err != nil {
			return total, err
		}
		remaining -= actualRead
		currOff = 0
	}

	// 2. 优化 BigChunk 循环：固定 4KB 逻辑简化
	for i := 0; i < len(b.big) && remaining > 0; i++ {
		// 利用固定 ChunkSize 简化计算
		canRead := ChunkSize - currOff
		if canRead > remaining {
			canRead = remaining
		}

		n, err := wr.Write(b.big[i][currOff : currOff+canRead])
		total += int64(n)
		if err != nil {
			return total, err
		}

		remaining -= canRead
		currOff = 0
	}
	return total, nil
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
func (v BufferView) Len() int {
	return v.length
}

// IsEmpty 返回视图是否为空
func (v BufferView) IsEmpty() bool {
	return v.length == 0
}

//go:inline At 支持随机访问，返回逻辑索引 index 处的字节
func (v BufferView) At(index int) byte {
	if index < 0 || index >= v.length {
		panic("view index out of range")
	}
	return atByte(v.hasSmall, v.small, v.big, v.firstPageOffset, index)
}

//go:inline Data 将视图转换为不连续的字节切片列表。常用于 net.Buffers 或 writev 系统调用
func (v BufferView) Data() [][]byte {
	return dataSlices(v.hasSmall, v.small, v.big, v.firstPageOffset, v.length)
}

//go:inline Bytes 将视图内容转换为连续的切片。优化：针对单页场景返回底层引用（0 拷贝），仅在跨页时执行分配与合并。
func (v BufferView) Bytes() []byte {
	return dataSlice(v.hasSmall, v.small, v.big, v.firstPageOffset, v.length)
}

//go:inline Slice 在当前视图基础上再次切片 v[n:m]
func (v BufferView) Slice(n, m int) BufferView {
	if n < 0 || m < n || m > v.length {
		panic(fmt.Errorf("view index out of range [%d:%d] with length %d", n, m, v.length))
	}

	length := m - n
	physicalStart := n + v.firstPageOffset
	return sliceFromPhysical(v.hasSmall, v.small, v.big, physicalStart, length)
}

//go:inline Tail 返回从 n 到末尾的视图，等价于 Go 切片 v[n:].
func (v BufferView) Tail(n int) BufferView {
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
