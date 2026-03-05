package redcon

import (
	"bufio"
	"bytes"
	"io"
	"sync"
)

const (
	// SmallChunkSize 表示小页（第一页）的字节大小。
	SmallChunkSize = 256
	// bigShift 表示大页大小的 2 次幂指数：$2^{12}=4096$。
	bigShift = 15
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
	// length 逻辑上的总有效数据长度
	length int
	// firstPageOffset 第一页的起始有效数据偏移（0 ~ pageSize-1）
	firstPageOffset int
	// capacity 缓存当前 Buffer 总物理容量（包含 small 和所有 big）
	capacity int
	// hasSmall 表示该 Buffer 的起始页是否为 small（256B）。NewBuffer 创建的 Buffer 恒为 true；Split 得到的 remainder 可能为 false（零拷贝所需）。
	hasSmall bool
	// big 保存后续所有 4KB 页；当 Buffer 起始页为大页时，big[0] 即第一页。
	big []*Chunk
	// small 仅用于第一页（256B）
	small *SmallChunk
}

// NewBuffer 创建一个空 Buffer。
func NewBuffer() *Buffer {
	return &Buffer{
		hasSmall: true,
		capacity: 0, //
		big:      make([]*Chunk, 0, initBigPageCount),
	}
}

//go:inline
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

	// 极简 Fast Path：只保留最核心的“单页判断”
	// 减少变量声明，直接在 if 中计算，降低 AST cost
	if hasSmall {
		if small != nil && firstPageOffset+length <= SmallChunkSize {
			return small[firstPageOffset : firstPageOffset+length]
		}
	} else if len(big) > 0 {
		if firstPageOffset+length <= ChunkSize {
			return big[0][firstPageOffset : firstPageOffset+length]
		}
	}

	// 所有跨页、分配、循环逻辑全部剥离
	return dataSliceSlow(hasSmall, small, big, firstPageOffset, length)
}

//go:noinline
func dataSliceSlow(hasSmall bool, small *SmallChunk, big []*Chunk, fpo, length int) []byte {
	res := make([]byte, length)
	rem := length
	currOff := fpo
	destOff := 0

	if hasSmall && small != nil {
		canRead := SmallChunkSize - currOff
		if canRead > rem {
			canRead = rem
		}
		copy(res[destOff:], small[currOff:currOff+canRead])
		rem -= canRead
		destOff += canRead
		currOff = 0
	}

	for i := 0; i < len(big) && rem > 0; i++ {
		canRead := ChunkSize - currOff
		if canRead > rem {
			canRead = rem
		}
		copy(res[destOff:], big[i][currOff:currOff+canRead])
		rem -= canRead
		destOff += canRead
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
	// 1. 处理 Small 区域逻辑
	if hasSmall && physicalStart < SmallChunkSize {
		return BufferView{
			hasSmall:        true,
			small:           small,
			big:             big,
			length:          length,
			firstPageOffset: physicalStart,
		}
	}

	// 2. 统一处理 Big 区域逻辑 (合并 hasSmall 为 true/false 的 Big 偏移计算)
	offset := physicalStart
	if hasSmall {
		offset -= SmallChunkSize
	}

	// 提前计算索引，减少 BufferView 初始化时的逻辑权重
	idx := offset >> bigShift
	return BufferView{
		hasSmall:        false,
		small:           nil,
		big:             big[idx:], // 仅 Slice Header 拷贝，0 分配
		length:          length,
		firstPageOffset: offset & bigMask,
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
	n := len(p)
	if n == 0 {
		return 0, nil
	}

	// 1. Fast Path: 针对小数据包，直接写入当前 Small 或 Big 页
	// 减少 ensureCapacity 的调用开销
	pLen := b.length + b.firstPageOffset

	// 情况 A: 还在 SmallChunk 范围内
	if b.hasSmall && pLen < SmallChunkSize {
		canWrite := SmallChunkSize - pLen
		if n <= canWrite {
			if b.small == nil {
				b.small = getSmallChunk()
			}
			copy(b.small[pLen:], p)
			b.length += n
			return n, nil
		}
		// 跨页了，交给 Slow Path
	} else if !b.hasSmall && len(b.big) > 0 {
		// 情况 B: 还在当前 BigChunk 最后一页的剩余空间内
		innerOff := pLen & bigMask
		canWrite := ChunkSize - innerOff
		if n <= canWrite {
			copy(b.big[len(b.big)-1][innerOff:], p)
			b.length += n
			return n, nil
		}
	}

	// 2. Slow Path: 大数据或跨多页写入
	return b.writeSlow(p)
}

// go:noinline
func (b *Buffer) writeSlow(p []byte) (int, error) {
	total := len(p)
	b.ensureCapacity(total) // 一次性扩容

	srcOff := 0
	for srcOff < total {
		pLen := b.length + b.firstPageOffset

		// 优先处理 SmallChunk
		if b.hasSmall && pLen < SmallChunkSize {
			if b.small == nil {
				b.small = getSmallChunk()
			}
			copyLen := min(SmallChunkSize-pLen, total-srcOff)
			copy(b.small[pLen:], p[srcOff:srcOff+copyLen])
			b.length += copyLen
			srcOff += copyLen
			continue
		}

		// 处理 BigChunk
		prefix := 0
		if b.hasSmall {
			prefix = SmallChunkSize
		}

		bigPos := pLen - prefix
		pageIdx := bigPos >> bigShift
		innerOff := bigPos & bigMask

		// 批量补充页面（由 ensureCapacity 保证，这里其实仅需索引访问）
		for pageIdx >= len(b.big) {
			b.big = append(b.big, getBigChunk())
		}

		copyLen := min(ChunkSize-innerOff, total-srcOff)
		copy(b.big[pageIdx][innerOff:], p[srcOff:srcOff+copyLen])

		b.length += copyLen
		srcOff += copyLen
	}
	return total, nil
}

//go:inline Slice 模拟 Go 原生切片操作 b[n:m],n: 起始位移 (inclusive),m: 结束位移 (exclusive)
func (b *Buffer) Slice(n, m int) BufferView {
	// 1. 使用 uint 技巧一次性检查 n < 0, m < n, m > length
	// 这与 Go 编译器处理切片的底层逻辑一致，节点数最少
	if uint(n) > uint(m) || uint(m) > uint(b.length) {
		panic("Buffer: slice index out of range")
	}

	length := m - n
	physicalStart := n + b.firstPageOffset
	// 2. 调用已优化的 sliceFromPhysical
	return sliceFromPhysical(b.hasSmall, b.small, b.big, physicalStart, length)
}

//go:inline Tail 返回从 n 到末尾的视图，等价于 Go 切片 b[n:].
func (b *Buffer) Tail(n int) BufferView {
	return b.Slice(n, b.length)
}

// Split 在 n 位置切断数据。
// 原有的 b 将保留 [0, n) 字节（完整指令，通过拷贝分割点实现物理隔离）。
// 返回的新 Buffer 将承接 [n, length) 字节（剩余流，通过位移实现零平移）。
func (b *Buffer) Split(n int, newBuf *Buffer) {
	if n <= 0 {
		// 情况：n=0，原 b 变为空，所有数据移交给新返回的 Buffer
		b.Swap(newBuf)
		return
	}

	if n >= b.length {
		// 情况：n 超过长度，b 保留所有，返回一个空的 Buffer
		return
	}

	originalSmall := b.small
	originalBig := b.big
	originalLen := b.length
	originalFPO := b.firstPageOffset
	originalHasSmall := b.hasSmall

	splitPos := n + originalFPO
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
			b.capacity = SmallChunkSize // b 仅剩 small

			// newBuf: allocate a new small page and copy the suffix bytes into the END part.
			suffixLen := min(SmallChunkSize-splitPos, newLen)
			suffix := originalSmall[splitPos : splitPos+suffixLen]
			newHasSmall, newSmall, newBigFirst, newFirstOff := makeSuffixFirstPage(suffix)
			newBuf.hasSmall = newHasSmall
			newBuf.small = newSmall
			newBuf.firstPageOffset = newFirstOff

			// 继承后续大页
			if len(newBigFirst) > 0 {
				// Should never happen since suffixLen <= SmallChunkSize, but keep consistent.
				newBuf.big = append(newBigFirst, originalBig...)
			} else {
				newBuf.big = originalBig
			}
			newBuf.length = newLen
			// 更新 newBuf.capacity: 计算方式同 ensureCapacity
			newBuf.capacity = calculateCapacity(newBuf.hasSmall, len(newBuf.big))
			return
		}

		// Boundary right after small page
		if splitPos == SmallChunkSize {
			b.small = originalSmall
			b.big = nil
			b.hasSmall = true
			b.length = n
			b.capacity = SmallChunkSize

			newBuf.small = nil
			newBuf.big = originalBig
			newBuf.hasSmall = false
			newBuf.firstPageOffset = 0
			newBuf.length = newLen
			newBuf.capacity = calculateCapacity(false, len(newBuf.big))
			return
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
		b.capacity = prefix + (splitBigIdx+1)*ChunkSize
	} else {
		b.big = append([]*Chunk(nil), originalBig[:splitBigIdx]...)
		b.capacity = prefix + (splitBigIdx)*ChunkSize
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
		newBuf.capacity = len(newBuf.big) * ChunkSize
		return
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
	newBuf.capacity = calculateCapacity(newBuf.hasSmall, len(newBuf.big))
	return
}

// ShiftTo 从 b 的头部切出 n 字节转移到 target，b 仅保留剩余部分。
// 这是解析 Redis 命令时提取 Raw 原始报文的高效路径。
func (b *Buffer) ShiftTo(n int, newBuf *Buffer) {
	if n <= 0 {
		// 情况：n=0，b 保留所有，返回一个空的 Buffer
		return
	}

	if n >= b.length {
		// 情况：n 超过长度，原 b 变为空，所有数据移交给新返回的 Buffer
		b.Swap(newBuf)
		return
	}

	originalSmall := b.small
	originalBig := b.big
	originalLen := b.length
	originalFPO := b.firstPageOffset
	originalHasSmall := b.hasSmall

	splitPos := n + originalFPO
	newLen := originalLen - n

	// prefix is the size of the first page in bytes (small page when present).
	prefix := 0
	if originalHasSmall {
		prefix = SmallChunkSize
	}

	if originalHasSmall {
		// Split in small page
		if splitPos < SmallChunkSize {
			// b keeps the original small page (no copy); newBuf gets a fresh small page for suffix.
			newBuf.small = originalSmall
			newBuf.big = nil
			newBuf.hasSmall = true
			newBuf.length = n
			// b.firstPageOffset unchanged
			newBuf.firstPageOffset = originalFPO
			newBuf.capacity = SmallChunkSize // b 仅剩 small

			// newBuf: allocate a new small page and copy the suffix bytes into the END part.
			suffixLen := min(SmallChunkSize-splitPos, newLen)
			suffix := originalSmall[splitPos : splitPos+suffixLen]
			newHasSmall, newSmall, newBigFirst, newFirstOff := makeSuffixFirstPage(suffix)
			b.hasSmall = newHasSmall
			b.small = newSmall
			b.firstPageOffset = newFirstOff

			// 继承后续大页
			if len(newBigFirst) > 0 {
				// Should never happen since suffixLen <= SmallChunkSize, but keep consistent.
				b.big = append(newBigFirst, originalBig...)
			} else {
				b.big = originalBig
			}
			b.length = newLen
			// 更新 newBuf.capacity: 计算方式同 ensureCapacity
			b.capacity = calculateCapacity(newBuf.hasSmall, len(newBuf.big))
			return
		}

		// Boundary right after small page
		if splitPos == SmallChunkSize {
			newBuf.small = originalSmall
			newBuf.big = nil
			newBuf.hasSmall = true
			newBuf.length = n
			newBuf.capacity = SmallChunkSize
			newBuf.firstPageOffset = originalFPO

			b.small = nil
			b.big = originalBig
			b.hasSmall = false
			b.firstPageOffset = 0
			b.length = newLen
			b.capacity = calculateCapacity(false, len(newBuf.big))
			return
		}
	}

	// Split in big pages (shared for hasSmall=true and hasSmall=false).
	bigSplitPos := splitPos - prefix
	splitBigIdx := bigSplitPos >> bigShift
	innerOff := bigSplitPos & bigMask

	// b keeps pages up to the boundary page; remainder never reuses the boundary page.
	newBuf.small = originalSmall
	newBuf.hasSmall = originalHasSmall
	if innerOff > 0 {
		newBuf.big = append([]*Chunk(nil), originalBig[:splitBigIdx+1]...)
		newBuf.capacity = prefix + (splitBigIdx+1)*ChunkSize
	} else {
		newBuf.big = append([]*Chunk(nil), originalBig[:splitBigIdx]...)
		newBuf.capacity = prefix + (splitBigIdx)*ChunkSize
	}
	newBuf.length = n
	newBuf.firstPageOffset = originalFPO

	if innerOff == 0 {
		// Boundary on big page edge: remainder can take pages without copying.
		b.small = nil
		b.hasSmall = false
		b.big = originalBig[splitBigIdx:]
		b.firstPageOffset = 0
		b.length = newLen
		b.capacity = len(newBuf.big) * ChunkSize
		return
	}

	// Split within a big page: copy suffix to a fresh first page for newBuf.
	suffixInThisPage := min(ChunkSize-innerOff, newLen)
	suffix := originalBig[splitBigIdx][innerOff : innerOff+suffixInThisPage]
	newHasSmall, newSmall, newBigFirst, newFirstOff := makeSuffixFirstPage(suffix)
	b.hasSmall = newHasSmall
	b.small = newSmall
	b.firstPageOffset = newFirstOff
	if splitBigIdx+1 < len(originalBig) {
		if len(newBigFirst) > 0 {
			b.big = append(newBigFirst, originalBig[splitBigIdx+1:]...)
		} else {
			b.big = originalBig[splitBigIdx+1:]
		}
	} else {
		b.big = newBigFirst
	}
	b.length = newLen
	b.capacity = calculateCapacity(newBuf.hasSmall, len(newBuf.big))
	return
}

// Discard 从 Buffer 头部直接丢弃 n 字节数据，并立即释放不再被引用的物理大页。
func (b *Buffer) Discard(n int) {
	if n <= 0 {
		return
	}
	if n >= b.length {
		b.Free()
		return
	}

	// 1. 记录原始状态
	origFPO := b.firstPageOffset
	origHasSmall := b.hasSmall
	origBig := b.big

	// 计算物理上的总偏移點
	splitPos := n + origFPO
	b.length -= n

	if origHasSmall {
		// --- 情況 A: 丢弃后新起点仍在 Small 页內 ---
		if splitPos < SmallChunkSize {
			b.firstPageOffset = splitPos
			// 確保 hasSmall 保持為 true
			b.hasSmall = true
			return
		}

		// --- 情況 B: 跨过 Small 页進入 Big 区域 ---
		// 必須減去 SmallChunkSize。
		bigSplitPos := splitPos - SmallChunkSize
		splitBigIdx := bigSplitPos >> bigShift
		innerOff := bigSplitPos & bigMask

		// 物理回收被完全跳过的大页
		for i := 0; i < splitBigIdx; i++ {
			putBigChunk(origBig[i])
		}

		b.hasSmall = false
		b.firstPageOffset = innerOff
		b.big = origBig[splitBigIdx:]
		b.capacity = len(b.big) * ChunkSize
	} else {
		// --- 情況 C: 本來就在 Big 区域 ---
		splitBigIdx := splitPos >> bigShift
		innerOff := splitPos & bigMask

		for i := 0; i < splitBigIdx; i++ {
			putBigChunk(origBig[i])
		}

		b.firstPageOffset = innerOff
		b.big = origBig[splitBigIdx:]
		b.capacity = len(b.big) * ChunkSize
	}
}

// 辅助函数，避免重复逻辑。建议内联。
func calculateCapacity(hasSmall bool, bigCount int) int {
	cap := bigCount * ChunkSize
	if hasSmall {
		cap += SmallChunkSize
	}
	return cap
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
	b.hasSmall = true
	b.capacity = 0
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
	// 1. 极简边界检查：利用 uint 一次性判定 index < 0 || index >= v.length
	if uint(index) >= uint(b.length) {
		panic("Buffer: slice index out of range")
	}
	// 2. 调用物理索引函数（确保 atByte 也是内联的）
	return atByte(b.hasSmall, b.small, b.big, b.firstPageOffset, index)
}

// Swap 交换两个 IndexedBuffer 的所有权
// 这是一个 O(1) 操作，仅交换指针和逻辑位移
func (b *Buffer) Swap(other *Buffer) {
	if b == nil || other == nil {
		return
	}

	// 1. 物理交换小页数组 (触发 256 字节拷贝)
	b.small, other.small = other.small, b.small

	// 2. 交换大页切片引用 (仅交换指针和长度/容量信息)
	b.big, other.big = other.big, b.big

	// 3. 交换状态位与长度
	b.hasSmall, other.hasSmall = other.hasSmall, b.hasSmall
	b.length, other.length = other.length, b.length
	b.firstPageOffset, other.firstPageOffset = other.firstPageOffset, b.firstPageOffset

	// 4. 【核心优化】交换预计算的物理容量
	// 确保 Swap 后，ensureCapacity 的内联检查依然准确
	b.capacity, other.capacity = other.capacity, b.capacity
}

//go:inline Bytes 将视图内容合并为一个连续的切片（涉及内存拷贝）建议仅在必须对接只接收 []byte 的第三方 API 时使用
func (b *Buffer) Bytes() []byte {
	return dataSlice(b.hasSmall, b.small, b.big, b.firstPageOffset, b.length)
}

// ensureCapacity 确保至少还能写 n 字节（自动扩容多页）
func (b *Buffer) ensureCapacity(n int) {
	// Fast Path: 只要物理剩余空间够，立刻返回。
	// 这里直接用 b.capacity 比较，减少加法节点的生成。
	if n <= b.capacity-(b.length+b.firstPageOffset) {
		return
	}

	// 慢速路径：交给非内联函数处理扩容、页面申请等重逻辑
	b.grow(n)
}

//go:noinline
func (b *Buffer) grow(n int) {
	// 计算当前物理占用
	physPos := b.length + b.firstPageOffset
	totalNeeded := physPos + n

	// 1. 特殊场景：首次写入且极大，直接放弃 Small 模式节省内存
	if physPos == 0 && b.small == nil && len(b.big) == 0 && b.hasSmall {
		if totalNeeded > SmallChunkSize {
			b.hasSmall = false
		}
	}

	// 2. 确保 SmallChunk 存在（如果在 Small 模式下）
	if b.hasSmall && b.small == nil {
		b.small = getSmallChunk()
		b.capacity += SmallChunkSize
	}

	// 3. 循环补充 BigChunks
	// 这里的 b.capacity 必须代表物理总容量（Small + 所有 Big）
	for b.capacity < totalNeeded {
		b.big = append(b.big, getBigChunk())
		b.capacity += ChunkSize
	}
}

func (b *Buffer) reserve() []byte {
	pLen := b.length + b.firstPageOffset

	// Fast Path 1: 还在 SmallChunk 范围内且已分配
	if b.hasSmall && b.small != nil && pLen < SmallChunkSize {
		return b.small[pLen:]
	}

	// Fast Path 2: 已经在 BigChunk 且当前页未满
	if !b.hasSmall && len(b.big) > 0 {
		innerOff := pLen & bigMask
		// 只有在当前页还有剩余空间时才内联返回
		if innerOff < ChunkSize {
			return b.big[len(b.big)-1][innerOff:]
		}
	}

	// 复杂情况：Small未分配、跨页、需分配新页等全部交给 Slow Path
	return b.reserveSlow(pLen)
}

//go:noinline
func (b *Buffer) reserveSlow(pLen int) []byte {
	if b.hasSmall && pLen < SmallChunkSize {
		if b.small == nil {
			b.small = getSmallChunk()
		}
		return b.small[pLen:]
	}

	prefix := 0
	if b.hasSmall {
		prefix = SmallChunkSize
	}

	bigPos := pLen - prefix
	idx := bigPos >> bigShift
	off := bigPos & bigMask

	// 确保页面存在（逻辑应由调用方通过 ensureCapacity 保证，这里做安全检查）
	if idx >= len(b.big) {
		return nil
	}
	return b.big[idx][off:]
}

// Reserve 预留 n 字节，返回当前页中的可写 slice。
// 若当前页剩余空间不足，会自动扩页。
func (b *Buffer) Reserve(n int) []byte {
	b.ensureCapacity(n)
	left := b.reserve()
	if len(left) > n {
		left = left[:n]
	}
	return left
}

// Advance 前进写指针 n 字节（告诉 Buffer 实际写入了多少）
func (b *Buffer) Advance(n int) {
	b.length += n
}

// ReadFull 直接从 bufio.Reader 灌入，绕过 io.Reader 接口
func (b *Buffer) ReadFull(rd io.Reader, n int) error {
	// 1. 一次性保证物理空间，避免循环内多次判断 capacity
	b.ensureCapacity(n)

	read := 0
	for read < n {
		pLen := b.length + b.firstPageOffset
		var dest []byte
		var canWrite int

		// 2. 定位当前物理写入点（直接内联逻辑，不调用外部函数）
		if b.hasSmall && pLen < SmallChunkSize {
			if b.small == nil {
				b.small = getSmallChunk()
			}
			canWrite = SmallChunkSize - pLen
			// 限制本次读取长度，不可跨越 Small 到 Big 的物理边界
			limit := n - read
			if limit > canWrite {
				limit = canWrite
			}
			dest = b.small[pLen : pLen+limit]
		} else {
			prefix := 0
			if b.hasSmall {
				prefix = SmallChunkSize
			}

			bigPos := pLen - prefix
			pageIdx := bigPos >> bigShift
			innerOff := bigPos & bigMask

			canWrite = ChunkSize - innerOff
			limit := n - read
			if limit > canWrite {
				limit = canWrite
			}
			// 确保索引安全（由 ensureCapacity 保证）
			dest = b.big[pageIdx][innerOff : innerOff+limit]
		}

		// 3. 执行物理读取
		nr, err := rd.Read(dest)
		if nr > 0 {
			b.length += nr
			read += nr
		}
		if err != nil {
			// ReadFull 语义：读不到期望长度即报错
			return err
		}

		// 如果 nr < limit，说明 bufio 缓存暂时读完了，下一轮循环会从新 pLen 继续
	}
	return nil
}

// ReadBuffered 尽可能多地从 bufio 缓冲区读取数据
// 即使数据跨越了多个物理 Chunk (4KB)，也能通过循环一次性读完
func (b *Buffer) ReadBuffered(rd *bufio.Reader) (int64, error) {
	// 1. 极速路径：优先排空当前已有的 Pipeline 积压数据
	n := rd.Buffered()
	if n > 0 {
		// 调用 ReadFull 搬运当前缓冲区内的全部数据
		err := b.ReadFull(rd, n)
		return int64(n), err
	}
	// 2. 缓冲区为空，进入 Slow Path 触发物理读取并循环收割
	return b.readBufferedSlow(rd)
}

//go:noinline
func (b *Buffer) readBufferedSlow(rd *bufio.Reader) (int64, error) {
	var total int64
	for {
		// 1. 确保物理空间（至少 1 字节触发扩容或分配）
		b.ensureCapacity(1)

		// 2. 定位当前物理写入点
		pLen := b.length + b.firstPageOffset
		var dest []byte
		var limit int

		if b.hasSmall && pLen < SmallChunkSize {
			// 在 SmallChunk 区域
			if b.small == nil {
				b.small = getSmallChunk()
			}
			limit = SmallChunkSize - pLen
			dest = b.small[pLen:SmallChunkSize]
		} else {
			// 在 BigChunk 区域
			prefix := 0
			if b.hasSmall {
				prefix = SmallChunkSize
			}
			bigPos := pLen - prefix
			pageIdx := bigPos >> bigShift
			innerOff := bigPos & bigMask

			// 如果当前页已满，跳转到下一页
			if innerOff >= ChunkSize {
				pageIdx++
				innerOff = 0
			}
			limit = ChunkSize - innerOff
			dest = b.big[pageIdx][innerOff:ChunkSize]
		}

		// 3. 执行读取决策
		buffered := rd.Buffered()
		if buffered > 0 {
			// 缓冲区有数据（来自上一轮物理 Read 的预读），执行“收割”
			if limit > buffered {
				limit = buffered
			}
			// 这里的 Read 实际上是内存拷贝 (memmove)
			nr, _ := rd.Read(dest[:limit])
			if nr > 0 {
				b.length += nr
				total += int64(nr)
			}
		} else {
			// 缓冲区完全为空（首轮循环或已排空），执行物理 Read 触发 fill
			nr, err := rd.Read(dest)
			if nr > 0 {
				b.length += nr
				total += int64(nr)
			}
			if err != nil {
				return total, err
			}
		}

		// 4. 结束判定：如果 bufio 缓冲区已排空且没有更多预读数据，则退出
		if rd.Buffered() == 0 {
			break
		}
		// 否则继续循环，下一轮会进入“收割”逻辑或跨越物理页边界
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
	// 1. 极简边界检查：利用 uint 一次性判定 index < 0 || index >= v.length
	if uint(index) >= uint(v.length) {
		panic("Buffer: slice index out of range")
	}
	// 2. 调用物理索引函数（确保 atByte 也是内联的）
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
	// 1. 使用 uint 技巧一次性检查 n < 0, m < n, m > length
	// 这与 Go 编译器处理切片的底层逻辑一致，节点数最少
	if uint(n) > uint(m) || uint(m) > uint(v.length) {
		panic("Buffer: slice index out of range")
	}

	length := m - n
	physicalStart := n + v.firstPageOffset
	return sliceFromPhysical(v.hasSmall, v.small, v.big, physicalStart, length)
}

//go:inline Tail 返回从 n 到末尾的视图，等价于 Go 切片 v[n:].
func (v BufferView) Tail(n int) BufferView {
	return v.Slice(n, v.length)
}

func (b *Buffer) Reset() {
	b.length = 0
	b.hasSmall = true
	b.capacity = calculateCapacity(b.hasSmall, len(b.big))
	// 不要清空 b.big，让已申请的 Chunk 留在切片里供下一轮 Reserve 直接使用
	// 这样 Reserve(len) 内部就会直接返回 b.big[0][0:len]，实现真正的 0 分配
}

// indexByteGeneric 抽象了物理页扫描逻辑，专为内联优化设计
func indexByteGeneric(hasSmall bool, small *SmallChunk, big []*Chunk, fpo, length int, c byte, start int) int {
	// 1. 唯一边界检查
	if uint(start) >= uint(length) {
		return -1
	}

	pLen := fpo + start
	var page []byte

	// 2. Fast Path: 定位当前起始页 (内联友好)
	if hasSmall && pLen < SmallChunkSize {
		page = small[pLen:SmallChunkSize]
	} else {
		offset := pLen
		if hasSmall {
			offset -= SmallChunkSize
		}
		idx := offset >> bigShift
		// 注意：调用方需保证 big 索引安全
		page = big[idx][offset&bigMask : ChunkSize]
	}

	// 3. 计算本页可读长度
	remaining := length - start
	canRead := len(page)
	if canRead > remaining {
		canRead = remaining
	}

	// 4. 调用汇编优化的 IndexByte
	res := bytes.IndexByte(page[:canRead], c)
	if res >= 0 {
		return start + res
	}

	// 5. 跨页处理：仅当数据未读完时进入 Slow Path (非内联)
	if remaining > canRead {
		return indexByteSlow(hasSmall, small, big, fpo, length, c, start+canRead)
	}
	return -1
}

func (b *Buffer) IndexByte(c byte, start int) int {
	return indexByteGeneric(b.hasSmall, b.small, b.big, b.firstPageOffset, b.length, c, start)
}

func (v BufferView) IndexByte(c byte, start int) int {
	return indexByteGeneric(v.hasSmall, v.small, v.big, v.firstPageOffset, v.length, c, start)
}

//go:noinline
func indexByteSlow(hasSmall bool, small *SmallChunk, big []*Chunk, fpo, length int, c byte, start int) int {
	curr := start

	// 1. 如果起点在 SmallChunk 且没找完，先处理 SmallChunk（防御性逻辑）
	if hasSmall && (fpo+curr) < SmallChunkSize {
		canRead := SmallChunkSize - (fpo + curr)
		remaining := length - curr
		actual := canRead
		if actual > remaining {
			actual = remaining
		}

		idx := bytes.IndexByte(small[fpo+curr:fpo+curr+actual], c)
		if idx >= 0 {
			return curr + idx
		}
		curr += actual
	}

	// 2. 遍历后续所有 BigChunks
	prefix := 0
	if hasSmall {
		prefix = SmallChunkSize
	}

	for curr < length {
		// 计算物理坐标
		bigPos := (fpo + curr) - prefix
		pageIdx := bigPos >> bigShift
		innerOff := bigPos & bigMask

		// 安全检查：如果索引越界（逻辑错误），立即退出
		if pageIdx >= len(big) {
			break
		}

		page := big[pageIdx]
		canRead := ChunkSize - innerOff
		remaining := length - curr
		actual := canRead
		if actual > remaining {
			actual = remaining
		}

		// 在当前 BigChunk 页面内进行汇编级扫描
		idx := bytes.IndexByte(page[innerOff:innerOff+actual], c)
		if idx >= 0 {
			return curr + idx
		}

		// 步进到下一页
		curr += actual
	}

	return -1
}

// parseIntGeneric 从物理坐标开始解析连续的数字。
// 专为内联设计，不处理负号，负号由调用方通过 At(0) 判断。
func parseIntGeneric(hasSmall bool, small *SmallChunk, big []*Chunk, fpo, length int) (int, bool) {
	if length <= 0 {
		return 0, false
	}

	n := 0
	for i := 0; i < length; i++ {
		// 直接内联 atByte 逻辑或使用 At() 的展开逻辑
		physPos := fpo + i
		var c byte
		if hasSmall && physPos < SmallChunkSize {
			c = small[physPos]
		} else {
			off := physPos
			if hasSmall {
				off -= SmallChunkSize
			}
			c = big[off>>bigShift][off&bigMask]
		}

		if c < '0' || c > '9' {
			return 0, false
		}
		n = n*10 + int(c-'0')
	}
	return n, true
}

// BufferView 的成员函数 (内联)
func (v BufferView) ParseInt() (int, bool) {
	return parseIntGeneric(v.hasSmall, v.small, v.big, v.firstPageOffset, v.length)
}

// Buffer 的成员函数 (内联)
func (b *Buffer) ParseInt(start, end int) (int, bool) {
	// 简单边界检查
	if uint(start) >= uint(end) || uint(end) > uint(b.length) {
		return 0, false
	}
	// 传入物理偏移和目标段长度
	return parseIntGeneric(b.hasSmall, b.small, b.big, b.firstPageOffset+start, end-start)
}
