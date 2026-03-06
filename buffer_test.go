package redcon

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"net"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestBuffer_Swap(t *testing.T) {
	buf := NewBuffer()
	wr := buf.NewWriter()
	wr.Write([]byte("hello"))
	buf2 := NewBuffer()
	wr2 := buf2.NewWriter()
	wr2.Write([]byte("world"))
	buf.Swap(buf2)
	require.Equal(t, []byte("hello"), buf2.Bytes())
	require.Equal(t, []byte("world"), buf.Bytes())
}

func TestBuffer_Tail(t *testing.T) {
	buf := NewBuffer()
	wr := buf.NewWriter()
	wr.Write([]byte("hello world"))

	v := buf.Tail(6)
	require.Equal(t, []byte("world"), v.Bytes())

	v2 := v.Tail(3)
	require.Equal(t, []byte("ld"), v2.Bytes())
}

func TestBuffer_UpgradeFromSmallToBig(t *testing.T) {
	buf := NewBuffer()
	wr := buf.NewWriter()
	// 触发“大页追加”：写入超过 SmallChunkSize（第一页为小页，后续为大页）
	payload := make([]byte, SmallChunkSize+10)
	for i := range payload {
		payload[i] = byte(i)
	}
	_, _ = wr.Write(payload[:SmallChunkSize])
	_, _ = wr.Write(payload[SmallChunkSize:])
	require.True(t, buf.hasSmall)

	// 随机访问与切片必须正确
	require.Equal(t, payload[0], buf.At(0))
	require.Equal(t, payload[SmallChunkSize], buf.At(SmallChunkSize))

	view := buf.Slice(SmallChunkSize-5, SmallChunkSize+5)
	require.Equal(t, payload[SmallChunkSize-5:SmallChunkSize+5], view.Bytes())

	// Split 后两段内容必须正确
	left := buf.Slice(0, SmallChunkSize).Bytes()
	newBuf := NewBuffer()
	buf.Split(SmallChunkSize, newBuf)
	right := newBuf.Bytes()
	require.Equal(t, payload[:SmallChunkSize], left)
	require.Equal(t, payload[SmallChunkSize:], right)

	newBuf.Free()
	buf.Free()
}

func TestBuffer_Split_RemainderSuffixFitsSmall(t *testing.T) {
	// Build: small + one big page, but only use 10 bytes into the big page.
	buf := NewBuffer()
	wr := buf.NewWriter()
	payload := make([]byte, SmallChunkSize+10)
	for i := range payload {
		payload[i] = byte(i)
	}
	_, _ = wr.Write(payload[:SmallChunkSize])
	_, _ = wr.Write(payload[SmallChunkSize:])
	require.True(t, buf.hasSmall)

	// Split inside the big page so that remainder's first fragment is <=128.
	// Pick split such that only 5 bytes remain in the big page fragment.
	splitAt := SmallChunkSize + 5
	newBuf := NewBuffer()
	buf.Split(splitAt, newBuf)

	// newBuf should start with a small page (optimization path)
	require.True(t, newBuf.hasSmall)
	require.NotNil(t, newBuf.small)
	require.Equal(t, payload[splitAt:], newBuf.Bytes())

	// old buf should keep correct left part
	require.Equal(t, payload[:splitAt], buf.Bytes())

	newBuf.Free()
	buf.Free()
}

func TestBuffer_FirstWriteLarge_UsesBigFirstPage(t *testing.T) {
	buf := NewBuffer()
	wr := buf.NewWriter()
	payload := make([]byte, SmallChunkSize+1)
	for i := range payload {
		payload[i] = byte(i)
	}
	_, _ = wr.Write(payload)

	require.False(t, buf.hasSmall)
	require.Nil(t, buf.small)
	require.GreaterOrEqual(t, len(buf.big), 1)
	require.Equal(t, payload, buf.Bytes())
	require.Equal(t, payload[0], buf.At(0))
	require.Equal(t, payload[len(payload)-1], buf.At(len(payload)-1))

	buf.Free()
}

func TestBuffer_FirstWriteSmall_UsesSmallFirstPage(t *testing.T) {
	buf := NewBuffer()
	wr := buf.NewWriter()
	payload := make([]byte, SmallChunkSize-1)
	for i := range payload {
		payload[i] = byte(i)
	}
	_, _ = wr.Write(payload)

	require.True(t, buf.hasSmall)
	require.NotNil(t, buf.small)
	require.Equal(t, 0, len(buf.big))
	require.Equal(t, payload, buf.Bytes())

	buf.Free()
}

func TestBuffer_Split_DoesNotShareBoundaryPageAndKeepsOldViews(t *testing.T) {
	// Build: small + one big page + some.
	buf := NewBuffer()
	wr := buf.NewWriter()
	payload := make([]byte, SmallChunkSize+ChunkSize+20)
	for i := range payload {
		payload[i] = byte(i)
	}
	_, _ = wr.Write(payload[:SmallChunkSize])
	_, _ = wr.Write(payload[SmallChunkSize:])
	require.True(t, buf.hasSmall)
	require.GreaterOrEqual(t, len(buf.big), 1)
	boundaryPage := buf.big[0]

	// Create a view into the first big page that should remain in the old buffer after split.
	view := buf.Slice(SmallChunkSize, SmallChunkSize+10)
	wantView := payload[SmallChunkSize : SmallChunkSize+10]
	require.Equal(t, wantView, view.Bytes())

	// Split inside the first big page.
	splitAt := SmallChunkSize + 5
	rem := NewBuffer()
	buf.Split(splitAt, rem)

	// Old buffer keeps boundary big page pointer.
	require.True(t, buf.hasSmall)
	require.GreaterOrEqual(t, len(buf.big), 1)
	require.Same(t, boundaryPage, buf.big[0])

	// Remainder must NOT reuse the boundary big page.
	if rem.hasSmall {
		require.NotNil(t, rem.small)
	} else {
		require.GreaterOrEqual(t, len(rem.big), 1)
		require.NotSame(t, boundaryPage, rem.big[0])
	}

	// Free remainder; view into old buffer must remain valid.
	rem.Free()
	require.Equal(t, wantView, view.Bytes())

	buf.Free()
}

func TestBuffer_Split_BigOnly_RemainderSuffixFitsSmall(t *testing.T) {
	// First write > SmallChunkSize => big-only buffer.
	buf := NewBuffer()
	wr := buf.NewWriter()
	payload := make([]byte, ChunkSize+100)
	for i := range payload {
		payload[i] = byte(i)
	}
	_, _ = wr.Write(payload)
	require.False(t, buf.hasSmall)
	require.GreaterOrEqual(t, len(buf.big), 1)

	// Split near end of first big page so suffix <= SmallChunkSize.
	splitAt := ChunkSize - (SmallChunkSize / 2)
	rem := NewBuffer()
	buf.Split(splitAt, rem)
	require.True(t, rem.hasSmall)
	require.NotNil(t, rem.small)
	require.Equal(t, payload[splitAt:], rem.Bytes())

	rem.Free()
	buf.Free()
}

func TestBuffer_Split_BigOnly_RemainderSuffixNeedsBig(t *testing.T) {
	// big-only buffer.
	buf := NewBuffer()
	wr := buf.NewWriter()
	payload := make([]byte, ChunkSize+100)
	for i := range payload {
		payload[i] = byte(i)
	}
	_, _ = wr.Write(payload)
	require.False(t, buf.hasSmall)
	require.GreaterOrEqual(t, len(buf.big), 1)
	boundary := buf.big[0]

	// Split so suffix in this page > SmallChunkSize, which should force newBuf to allocate a big page.
	suffix := SmallChunkSize + 20
	splitAt := ChunkSize - suffix
	rem := NewBuffer()
	buf.Split(splitAt, rem)
	require.False(t, rem.hasSmall)
	require.GreaterOrEqual(t, len(rem.big), 1)
	require.NotSame(t, boundary, rem.big[0])
	require.Equal(t, payload[splitAt:], rem.Bytes())

	rem.Free()
	buf.Free()
}

func TestBuffer_Reserve_ContinuousWrite(t *testing.T) {
	b := NewBuffer()           // 假设初始化
	data := make([]byte, 4097) // 超过一个 ChunkSize(4096)

	written := 0
	for written < len(data) {
		// 1. 预留剩余所需空间
		need := len(data) - written
		buf := b.Reserve(need)

		// 2. 检查：返回的切片长度必须 > 0，否则会死循环
		if len(buf) == 0 {
			t.Fatal("Reserve returned an empty slice, potential deadloop")
		}

		// 3. 模拟写入并更新长度
		n := copy(buf, data[written:])
		b.length += n
		written += n

		t.Logf("Wrote %d bytes, total length: %d", n, b.length)
	}

	// 验证最终长度
	if b.length != 4097 {
		t.Errorf("Expected length 5000, got %d", b.length)
	}
}

func TestBuffer_Reserve_PprofOptimization(t *testing.T) {
	// 模拟 ChunkSize 为 4096
	b := &Buffer{
		hasSmall: false,
		big:      []*Chunk{getBigChunk()},
		// 假设初始状态
	}

	// 场景：刚好在边界
	b.length = 4096

	// 测试：如果刚好写满，Reserve 是否能直接给出第二页
	// 而不是返回一个 len=0 的第一页切片导致上层 dataSlices 逻辑复杂化
	p := b.Reserve(100)

	if len(p) == 0 {
		t.Errorf("Reserve 应该自动跳过已满页，避免返回空切片引发 runtime.makeslice")
	}

	if cap(p) < 100 {
		t.Errorf("返回空间不足")
	}
}

func TestSliceFromPhysical_WithConstants(t *testing.T) {
	// 引用你的常量
	// SmallChunkSize = 256
	// ChunkSize = 4096 (1 << 12)

	small := &SmallChunk{}
	big := make([]*Chunk, 5)
	for i := range big {
		big[i] = &Chunk{}
	}

	t.Run("StayInSmall", func(t *testing.T) {
		// 必须小于 256 才能留在 Small 页
		v := sliceFromPhysical(true, small, big, 100, 50)
		if !v.hasSmall {
			t.Errorf("应该在 Small 页内，实际 hasSmall=%v", v.hasSmall)
		}
		if v.firstPageOffset != 100 {
			t.Errorf("偏移错误: %d", v.firstPageOffset)
		}
	})

	t.Run("RolloverToBigPage0", func(t *testing.T) {
		// physicalStart = 500
		// 由于 500 > 256 (SmallChunkSize)，进入 Big 逻辑
		// off2 = 500 - 256 = 244
		// startBigIdx = 244 >> 12 = 0 (落在第一个 Big Chunk)
		// newFirstOff = 244 & 4095 = 244
		v := sliceFromPhysical(true, small, big, 500, 100)

		if v.hasSmall {
			t.Error("应该已切换到 Big 模式")
		}
		if v.firstPageOffset != 244 {
			t.Errorf("预期偏移 244, 实际得到 %d", v.firstPageOffset)
		}
		if len(v.big) != 5 {
			t.Errorf("应该引用全部 5 个 Big 页, 实际 %d", len(v.big))
		}
	})

	t.Run("RolloverToDeepBigPage", func(t *testing.T) {
		// 模拟跨越 Small(256) 并进入第 2 个 Big 页 (索引 1)
		// 目标：第 2 个 Big 页的偏移 500 处
		// physicalStart = 256 (Small) + 4096 (Big0) + 500 = 4852
		v := sliceFromPhysical(true, small, big, 4852, 100)

		if v.firstPageOffset != 500 {
			t.Errorf("深层偏移计算错误: %d", v.firstPageOffset)
		}
		// startBigIdx 应为 1 (因为 4596 >> 12 = 1)
		// v.big 应为 big[1:]，长度为 4
		if len(v.big) != 4 {
			t.Errorf("Big 切片裁剪错误: 预期长度 4, 实际 %d", len(v.big))
		}
	})
}

func TestBufferView_Bytes_Precision(t *testing.T) {
	// 准备物理数据
	small := &SmallChunk{}
	for i := range small {
		small[i] = 's'
	}

	big0 := &Chunk{}
	for i := range big0 {
		(*big0)[i] = '0'
	}
	big1 := &Chunk{}
	for i := range big1 {
		(*big1)[i] = '1'
	}
	big := []*Chunk{big0, big1}

	t.Run("FastPath_Small", func(t *testing.T) {
		// 落在 Small 内 (250~255)
		v := BufferView{hasSmall: true, small: small, big: big, firstPageOffset: 250, length: 5}
		res := v.Bytes()
		if len(res) != 5 || res[0] != 's' {
			t.Errorf("Small FastPath 错误: %s", string(res))
		}
		// 验证是否是引用：修改原数据看 res 是否变化（仅限测试验证引用）
		small[250] = 'X'
		if res[0] != 'X' {
			t.Error("期望是引用，实际发生了拷贝")
		}
	})

	t.Run("SlowPath_Cross_Small_to_Big", func(t *testing.T) {
		// 跨越 256 边界：Small 剩 6 字节 + Big0 拿 4 字节
		v := BufferView{hasSmall: true, small: small, big: big, firstPageOffset: 250, length: 10}
		res := v.Bytes()
		if len(res) != 10 || string(res) != "XXXXX s0000" { // 前 5 个是 X(已改), 1个s, 4个0
			// 注意：此处逻辑需根据你的物理填充修正，核心是看长度和内容合并
		}
	})

	t.Run("FastPath_Big", func(t *testing.T) {
		// 跳过 Small，落在 Big0 内部 (Offset 500, len 100)
		v := BufferView{hasSmall: false, big: big, firstPageOffset: 500, length: 100}
		res := v.Bytes()
		if len(res) != 100 || res[0] != '0' {
			t.Errorf("Big FastPath 错误")
		}
	})
}

func TestBuffer_ReadFrom(t *testing.T) {
	// 1. 准备测试数据：构造一个大于单页 (4KB) 的数据量，例如 10KB
	// 这将跨越至少 3 个物理 Chunk
	const dataSize = 10 * 1024
	testData := make([]byte, dataSize)
	for i := 0; i < dataSize; i++ {
		testData[i] = byte(i%('Z'-'A'+1) + 'A')
	}

	// 2. 初始化 bufio.Reader
	// 注意：bufio 默认缓冲区通常是 4KB，我们手动设为 16KB 以确保一次能 Buffered 更多数据
	rawReader := bytes.NewReader(testData)
	brd := bufio.NewReaderSize(rawReader, 16*1024)

	// 预填充 bufio 的缓冲区（执行一次 Peek 或 Read 触发底层填充）
	_, _ = brd.Peek(dataSize)
	if brd.Buffered() != dataSize {
		t.Fatalf("bufio 缓冲区未填充完毕: 期望 %d, 实际 %d", dataSize, brd.Buffered())
	}

	// 3. 初始化你的内存池 Buffer
	// 假设 NewBuffer 是你的构造函数
	buf := NewBuffer()
	wr := buf.NewWriter()

	// 4. 执行测试逻辑
	n, err := wr.CopyBufferedTo(brd)
	if err != nil && err != io.EOF {
		t.Fatalf("ReadFromBuffered 执行失败: %v", err)
	}

	// 5. 验证结果
	require.Equal(t, int64(dataSize), n)
	require.Equal(t, dataSize, buf.Len())
	require.Equal(t, testData, buf.Bytes())
}

// 可选：增加一个边界测试，验证当 Buffer 已有部分数据且 offset 不在页首时的情况
func TestBuffer_ReadFrom_WithOffset(t *testing.T) {
	buf := NewBuffer()
	wr := buf.NewWriter()

	wr.Write([]byte("hello world")) // 11

	extraData := bytes.Repeat([]byte("a"), 5000)

	// 【关键修复】设置缓冲区为 10KB，确保 5000 字节能被 Buffered()
	brd := bufio.NewReaderSize(bytes.NewReader(extraData), 10240)

	// 预读触发填充
	_, _ = brd.Peek(5000)

	t.Logf("Before: Buffered = %d", brd.Buffered()) // 这里应该打印 5000

	n, err := wr.CopyBufferedTo(brd)
	if err != nil {
		t.Fatal(err)
	}

	t.Logf("After: n = %d, buf.Len = %d", n, buf.Len())

	if buf.Len() != 5011 {
		t.Errorf("期望 5011, 实际 %d", buf.Len())
	}
}

func TestBuffer_ReadFrom_WithOffset2(t *testing.T) {
	init := []byte("hello world")
	buf := NewBuffer()
	wr := buf.NewWriter()
	wr.Write(init) // 11

	extraData := bytes.Repeat([]byte("a"), 5000)

	// 【关键修复】设置缓冲区为 10KB，确保 5000 字节能被 Buffered()
	brd := bufio.NewReaderSize(bytes.NewReader(extraData), 10240)

	t.Logf("Before: Buffered = %d", brd.Buffered()) // 这里应该打印 5000

	n, err := wr.CopyBufferedTo(brd)
	if err != nil {
		t.Fatal(err)
	}

	require.Equal(t, 5011, buf.Len())
	require.Equal(t, extraData, buf.Bytes()[11:])
	require.Equal(t, init, buf.Bytes()[:11])
	require.Equal(t, int64(5000), n)
}

func TestBuffer_ReadFull_MultiPage(t *testing.T) {
	// 1. 构造 10KB 数据 (10240 字节)，将跨越：
	// SmallChunk(if exists) + BigChunk0 + BigChunk1 + BigChunk2
	const dataSize = 10 * 1024
	testData := make([]byte, dataSize)
	for i := 0; i < dataSize; i++ {
		testData[i] = byte(i % 256)
	}

	// 2. 初始化 Buffer 并制造一个起始偏移量 (例如 11 字节)
	// 这样可以测试物理页不是从 0 开始填充的情况
	buf := NewBuffer()
	wr := buf.NewWriter()

	prefix := []byte("head_offset") // 11 字节
	wr.Write(prefix)

	// 3. 构造 bufio.Reader
	brd := bufio.NewReaderSize(bytes.NewReader(testData), 16*1024)

	// 4. 执行 ReadFull
	err := wr.CopyN(brd, dataSize)
	if err != nil {
		t.Fatalf("ReadFull 失败: %v", err)
	}

	// 5. 验证长度
	expectedTotal := len(prefix) + dataSize // 11 + 10240 = 10251
	if buf.Len() != expectedTotal {
		t.Errorf("总长度不符: 期望 %d, 实际 %d", expectedTotal, buf.Len())
	}

	// 6. 验证数据完整性 (最关键的一步)
	// 检查每一个字节是否正确，特别是跨越 4096 字节边界的地方
	allData := buf.Bytes()
	if !bytes.Equal(allData[:len(prefix)], prefix) {
		t.Error("头部前缀数据损坏")
	}
	if !bytes.Equal(allData[len(prefix):], testData) {
		t.Error("ReadFull 灌入的数据内容不一致，可能在跨页切换时发生了索引计算错误")

		// 找出第一个出错的字节位置
		for i := 0; i < dataSize; i++ {
			if allData[len(prefix)+i] != testData[i] {
				t.Errorf("第一个错误发生在偏移量 %d (逻辑位置 %d), 期望 %d, 实际 %d",
					i, len(prefix)+i, testData[i], allData[len(prefix)+i])
				break
			}
		}
	}
}

func TestBuffer_WriteTo(t *testing.T) {
	buf := NewBuffer()
	wr_ := buf.NewWriter()

	// 1. 强制检查写入过程
	input := []byte("hello world")
	_, err := wr_.Write(input)
	require.NoError(t, err)

	// 2. 检查内部状态（调试打印）
	fmt.Printf("Buffer State: hasSmall=%v, len=%d, offset=%d\n", buf.hasSmall, buf.length, buf.firstPageOffset)

	// 3. 使用标准方式读取结果
	wr := new(bytes.Buffer)
	n, err := buf.WriteTo(wr)

	require.NoError(t, err)
	require.Equal(t, int64(len(input)), n)

	// 4. 重点：检查 wr.Bytes() 而不是原始切片
	require.Equal(t, input, wr.Bytes(), "数据内容不匹配，检查 Write 或 WriteTo 的拷贝逻辑")
}

func TestBuffer_ReadFrom2(t *testing.T) {
	buf := NewBuffer()
	wr := buf.NewWriter()

	input := []byte("hello world")
	rd := bufio.NewReaderSize(bytes.NewReader(input), 1024)
	n, err := wr.CopyBufferedTo(rd)
	require.NoError(t, err)
	require.Equal(t, input, buf.Bytes())
	require.Equal(t, int64(len(input)), int64(n))
}

func TestBuffer_WriteTo_Complex(t *testing.T) {
	// 1. 初始化 Buffer，构造跨越 SmallChunk 和多个 BigChunk 的数据
	buf := NewBuffer() // 假设初始 hasSmall 为 true
	wr := buf.NewWriter()

	// 写入一些数据制造偏移。例如先写 10 字节。
	// 这会使得 firstPageOffset = 0, length = 10 (在 SmallChunk 中)
	initialData := []byte("0123456789")
	wr.Write(initialData)

	// 此时模拟从中间开始写，人为调整 firstPageOffset (模拟之前的 Read 操作留下的偏移)
	// 比如我们只关心从第 5 个字节开始的数据
	const offset = 5
	buf.firstPageOffset = offset
	buf.length -= offset
	// 此时有效数据是 "56789"，长度 5

	// 2. 灌入大量数据跨越多个 BigChunk (4KB * 2 + 500 字节)
	extraSize := ChunkSize*2 + 500
	extraData := make([]byte, extraSize)
	for i := 0; i < extraSize; i++ {
		extraData[i] = byte('A' + (i % 26))
	}
	wr.Write(extraData)

	// 计算预期总数据
	expectedData := append([]byte("56789"), extraData...)
	expectedTotal := int64(len(expectedData))

	// 3. 执行 WriteTo
	// 使用 bytes.Buffer 接收输出，模拟生产环境中的 bufio.Writer
	output := new(bytes.Buffer)
	n, err := buf.WriteTo(output)

	// 4. 验证
	if err != nil {
		t.Fatalf("WriteTo 失败: %v", err)
	}

	if n != expectedTotal {
		t.Errorf("返回的写入长度不符: 期望 %d, 实际 %d", expectedTotal, n)
	}

	if int64(output.Len()) != expectedTotal {
		t.Errorf("Writer 接收到的数据长度不符: 期望 %d, 实际 %d", expectedTotal, output.Len())
	}

	if !bytes.Equal(output.Bytes(), expectedData) {
		t.Error("写入的数据内容不一致！可能在 Chunk 切换或 currOff 重置时发生了错误")

		// 辅助调试：定位第一个坏字节
		res := output.Bytes()
		for i := 0; i < len(expectedData); i++ {
			if res[i] != expectedData[i] {
				t.Errorf("第一个错误发生在索引 %d, 期望 %x, 实际 %x", i, expectedData[i], res[i])
				break
			}
		}
	}
}

var payload = [100]byte{}

func BenchmarkBuffer_Write(b *testing.B) {
	for i := 0; i < b.N; i++ {
		buf := NewBuffer()
		wr := buf.NewWriter()
		for j := 0; j < 200; j++ {
			_, _ = wr.Write(payload[:])
		}
		buf.Free()
	}
}

func BenchmarkBuffer_SliceWrite(b *testing.B) {
	for i := 0; i < b.N; i++ {
		var buf []byte
		for j := 0; j < 200; j++ {
			buf = append(buf, payload[:]...)
		}
	}
}

func BenchmarkCompare_WriteMethods(b *testing.B) {
	size := 32768 // 32KB, 跨 8 个 BigChunk (4KB)
	data := make([]byte, size)
	for i := 0; i < size; i++ {
		data[i] = byte(i % 256)
	}

	buf := NewBuffer()
	wr := buf.NewWriter()

	wr.Write(data)
	dw := io.Discard

	// 方案 1: 重构后的 WriteTo + bufio
	b.Run("Stream-WriteTo-Bufio", func(b *testing.B) {
		bw := bufio.NewWriterSize(dw, 32<<10) // 足够大的缓冲区
		b.ResetTimer()
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			buf.WriteTo(bw)
			bw.Flush()
		}
	})

	// 方案 2: 标准库 net.Buffers
	b.Run("Net-Buffers-Writev", func(b *testing.B) {
		b.ResetTimer()
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			// 构造 net.Buffers (注意：这里会产生 [][]byte 的分配)
			var netBuf net.Buffers
			if buf.hasSmall {
				netBuf = append(netBuf, buf.small[:buf.length]) // 简化逻辑
			} else {
				for _, chunk := range buf.big {
					netBuf = append(netBuf, chunk[:])
				}
			}
			netBuf.WriteTo(dw)
		}
	})

	// 方案 3: 原生 Bytes() 全量拷贝 (Baseline)
	b.Run("Native-Bytes-Copy", func(b *testing.B) {
		b.ResetTimer()
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			temp := buf.Bytes()
			dw.Write(temp)
		}
	})
}

const (
	HotCacheSize = 2048 // 针对 3 万并发建议的大小
)

// --- 方案 A: 原生 sync.Pool ---
var rawPool = sync.Pool{
	New: func() interface{} { return new(SmallChunk) },
}

// --- 方案 B: 优化后的二级池 (Hot Cache + sync.Pool) ---
var (
	hotCache  = make(chan *SmallChunk, HotCacheSize)
	smartPool = sync.Pool{
		New: func() interface{} { return new(SmallChunk) },
	}
)

func BenchmarkPoolContention(b *testing.B) {
	b.SetParallelism(128) // 模拟极高并发

	b.Run("Raw_sync.Pool", func(b *testing.B) {
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				c := rawPool.Get().(*SmallChunk)

				// 强制内存写入，防止编译器优化
				c[0] = byte(1)
				if c[SmallChunkSize-1] != 0 {
					_ = c[0]
				}

				rawPool.Put(c)
			}
		})
	})

	b.Run("Optimized_TwoLayerPool", func(b *testing.B) {
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				var c *SmallChunk
				select {
				case c = <-hotCache:
				default:
					c = smartPool.Get().(*SmallChunk)
				}

				// 强制内存写入
				c[0] = byte(1)
				if c[SmallChunkSize-1] != 0 {
					_ = c[0]
				}

				select {
				case hotCache <- c:
				default:
					smartPool.Put(c)
				}
			}
		})
	})
}

func TestBuffer_ShiftTo(t *testing.T) {
	// 初始化 Buffer 并写入跨页数据 (Small + 1.5 Big Chunks)
	b := NewBuffer()
	wr := b.NewWriter()

	p1 := make([]byte, SmallChunkSize) // 256B
	p2 := make([]byte, ChunkSize)      // 4096B
	p3 := make([]byte, 500)            // 500B
	for i := range p1 {
		p1[i] = 'a'
	}
	for i := range p2 {
		p2[i] = 'b'
	}
	for i := range p3 {
		p3[i] = 'c'
	}

	wr.Write(p1)
	wr.Write(p2)
	wr.Write(p3) // 总长度: 256 + 4096 + 500 = 4852

	t.Run("ShiftSmall", func(t *testing.T) {
		target := NewBuffer()
		b.ShiftTo(100, target) // 从头部切出 100B (还在 Small 区域)

		require.Equal(t, 100, target.Len())
		require.True(t, target.hasSmall)
		require.Equal(t, byte('a'), target.Bytes()[0])

		require.Equal(t, 4752, b.Len())
		require.Equal(t, 100, b.firstPageOffset)
		require.True(t, b.hasSmall)
	})

	t.Run("ShiftToBoundary", func(t *testing.T) {
		target := NewBuffer()
		// 此时 b 剩余 4752，前 156B 在 Small，后面是大页
		b.ShiftTo(156, target) // 刚好切完 Small 剩余部分

		require.Equal(t, 156, target.Len())
		require.Equal(t, byte('a'), target.Bytes()[0])

		require.Equal(t, 4596, b.Len())
		require.False(t, b.hasSmall) // b 应该不再持有 small
		require.Equal(t, 0, b.firstPageOffset)
		require.Equal(t, byte('b'), b.Bytes()[0])
	})

	t.Run("ShiftInBigPage", func(t *testing.T) {
		target := NewBuffer()
		// 此时 b 起始是大页，长度 4596 (1 Big + 500B)
		b.ShiftTo(1000, target) // 在第一个大页中间切分

		require.Equal(t, 1000, target.Len())
		require.False(t, target.hasSmall)
		require.Equal(t, 3596, b.Len())
		require.Equal(t, 1000, b.firstPageOffset)
		require.Equal(t, byte('b'), b.Bytes()[0])
	})
}

func TestBuffer_Discard(t *testing.T) {
	// 为了确保测试覆盖 hasSmall=true 的场景，我们需要分两次写入
	// 第一次写一个小数据占用 small，第二次写大数据触发 big
	b := NewBuffer()
	wr := b.NewWriter()

	wr.Write([]byte("init")) // 占用 small，此时 b.hasSmall 恒为 true

	// 构造后续的大页数据
	data := make([]byte, SmallChunkSize+ChunkSize*2)
	wr.Write(data)

	require.True(t, b.hasSmall, "应该持有 small 页")

	t.Run("DiscardWithinSmall", func(t *testing.T) {
		// 初始 "init" 占 4 字节，FPO 为 0
		b.Discard(2)
		require.Equal(t, 2, b.firstPageOffset)
		require.True(t, b.hasSmall)
	})

	t.Run("DiscardCrossPageAndVerifyRelease", func(t *testing.T) {
		// 此时 b.hasSmall 为 true，FPO 为 2
		// 我们要丢弃剩余的 small (254字节) + 整个 Big1 (4096字节) + Big2 的前 10 字节
		initialBigCount := len(b.big)

		discardLen := (SmallChunkSize - 2) + ChunkSize + 10
		b.Discard(discardLen)

		require.False(t, b.hasSmall, "跨过 small 后 hasSmall 应为 false")
		require.Equal(t, 10, b.firstPageOffset, "FPO 应该是相对于 Big2 的 10")
		require.Equal(t, initialBigCount-1, len(b.big), "Big1 应该已被物理释放")
	})

	t.Run("DiscardAll", func(t *testing.T) {
		b.Discard(b.Len())
		require.Equal(t, 0, b.Len())
		require.True(t, b.hasSmall, "Reset 后应恢复 hasSmall")
		// 如果你的 Reset 实现是将 big 置为 nil：
		require.Nil(t, b.big)
		require.Equal(t, 0, b.capacity)
	})
}

func TestBuffer_Discard2(t *testing.T) {
	t.Run("DiscardWithinSmall", func(t *testing.T) {
		b := NewBuffer()
		wr := b.NewWriter()

		wr.Write([]byte("0123456789")) // 10字节，在small页
		b.Discard(4)

		require.Equal(t, 6, b.Len())
		require.Equal(t, 4, b.firstPageOffset)
		require.True(t, b.hasSmall)
		require.Equal(t, []byte("456789"), b.Bytes())
	})

	t.Run("DiscardCrossSmallToBig", func(t *testing.T) {
		b := NewBuffer()
		wr := b.NewWriter()

		// 构造：Small(256B) + Big(4096B)
		p1 := make([]byte, SmallChunkSize)
		p2 := make([]byte, 100)
		for i := range p1 {
			p1[i] = 'a'
		}
		for i := range p2 {
			p2[i] = 'b'
		}
		wr.Write(p1)
		wr.Write(p2)

		// 丢弃全部 Small + Big 的前 10 字节
		b.Discard(SmallChunkSize + 10)

		require.Equal(t, 90, b.Len())
		require.False(t, b.hasSmall, "应该已经禁用 small 页")
		require.Equal(t, 10, b.firstPageOffset, "偏移量应相对于 Big 页起始位")
		require.Equal(t, byte('b'), b.Bytes()[0])
	})

	t.Run("DiscardBigOnlyMode", func(t *testing.T) {
		// 模拟大对象直接禁用 small 的情况
		b := NewBuffer()
		wr := b.NewWriter()

		payload := make([]byte, ChunkSize+500)
		wr.Write(payload) // 此时 b.hasSmall 应为 false
		require.False(t, b.hasSmall)

		b.Discard(ChunkSize + 10)
		require.Equal(t, 490, b.Len())
		require.Equal(t, 10, b.firstPageOffset)
		require.Equal(t, 1, len(b.big), "第一页 Big 应该已被物理回收")
	})
}

func TestBuffer_Discard_BigOnly_RemainderSuffixFitsSmall(t *testing.T) {
	// 1. 构造 Big-only 模式 (首次写入 > 256B)
	buf := NewBuffer()
	wr := buf.NewWriter()

	payload := make([]byte, ChunkSize+100)
	for i := range payload {
		payload[i] = byte(i % 256)
	}
	wr.Write(payload)
	require.False(t, buf.hasSmall)

	// 2. 丢弃到第一页大页的末尾，使得剩余数据 <= SmallChunkSize
	// 剩余数据长度设为 SmallChunkSize / 2 (128B)
	suffixLen := SmallChunkSize / 2
	discardLen := ChunkSize - suffixLen

	buf.Discard(discardLen)

	// 验证：丢弃后 b 应该进入了第一页大页的后半段
	require.False(t, buf.hasSmall, "Discard 不应主动开启 hasSmall，除非 Reset")
	require.Equal(t, suffixLen+100, buf.Len())
	require.Equal(t, discardLen, buf.firstPageOffset)
	require.Equal(t, payload[discardLen:], buf.Bytes())

	buf.Free()
}

func TestBuffer_DataIntegrityAfterDiscard(t *testing.T) {
	b := NewBuffer()
	wr := b.NewWriter()

	raw := []byte("0123456789")
	// 循环写入使其跨页
	for i := 0; i < 500; i++ {
		wr.Write(raw)
	}

	total := b.Bytes()

	// 随机丢弃一段长度
	discardLen := 300
	b.Discard(discardLen)

	require.Equal(t, total[discardLen:], b.Bytes())
}

func TestBuffer_ShiftTo_BigOnly_RemainderSuffixFitsSmall(t *testing.T) {
	// 1. 构造 Big-only 模式
	buf := NewBuffer()
	wr := buf.NewWriter()

	payload := make([]byte, ChunkSize+500) // 4096 + 500
	for i := range payload {
		payload[i] = byte(i % 256)
	}
	wr.Write(payload)
	require.False(t, buf.hasSmall)

	// 2. 将数据切分给 target，切分点选在第一页大页的末尾
	// 使得 target 拿到的数据量 <= SmallChunkSize (例如 128B)
	shiftLen := ChunkSize + 500 - SmallChunkSize/2
	target := NewBuffer()

	// 执行移出操作
	buf.ShiftTo(shiftLen, target)

	// 验证 target (接收了头部的 128B)
	require.False(t, target.hasSmall)
	require.Equal(t, shiftLen, target.Len())
	require.Equal(t, payload[:shiftLen], target.Bytes())
	require.Equal(t, 0, target.firstPageOffset)

	// 验证 buf (保留了剩余部分)
	require.Equal(t, 128, buf.firstPageOffset)
	require.Equal(t, ChunkSize+500-shiftLen, buf.Len())
	require.Equal(t, payload[shiftLen:], buf.Bytes())
	require.True(t, buf.hasSmall, "小段数据移出应优先填充 target 的 small 数组")

	target.Free()
	buf.Free()
}

func TestBuffer_ShiftTo2(t *testing.T) {
	t.Run("ShiftSmallToTarget", func(t *testing.T) {
		b := NewBuffer()
		wr := b.NewWriter()

		wr.Write([]byte("header_body")) // 11字节

		target := NewBuffer()
		b.ShiftTo(6, target) // 切出 "header"

		require.Equal(t, 6, target.Len())
		require.Equal(t, []byte("header"), target.Bytes())
		require.True(t, target.hasSmall)

		require.Equal(t, 5, b.Len())
		require.Equal(t, []byte("_body"), b.Bytes())
	})

	t.Run("ShiftBigSuffixToTarget", func(t *testing.T) {
		// 构造 Big-Only 模式
		b := NewBuffer()
		wr := b.NewWriter()

		payload := make([]byte, ChunkSize+200)
		for i := range payload {
			payload[i] = byte(i % 256)
		}
		wr.Write(payload)

		// 此时 b.hasSmall 为 false, 拥有 2 个 Big Chunk
		require.False(t, b.hasSmall)

		target := NewBuffer()
		// 从头部切掉 100 字节
		b.ShiftTo(100, target)

		require.Equal(t, 100, target.Len())
		require.False(t, target.hasSmall)
		require.Equal(t, 0, target.firstPageOffset)

		require.Equal(t, ChunkSize+100, b.Len())
		require.Equal(t, 100, b.firstPageOffset)
		require.Equal(t, payload[100:], b.Bytes())
	})

	t.Run("ShiftAllToTarget", func(t *testing.T) {
		b := NewBuffer()
		wr := b.NewWriter()

		wr.Write([]byte("full_data"))

		target := NewBuffer()
		b.ShiftTo(b.Len(), target)

		require.Equal(t, 0, b.Len())
		require.Equal(t, 9, target.Len())
		require.Equal(t, []byte("full_data"), target.Bytes())
	})
}

func TestBuffer_ComplexOperations(t *testing.T) {
	b := NewBuffer()
	wr := b.NewWriter()

	// 填充 10KB 数据
	data := make([]byte, 10240)
	for i := range data {
		data[i] = byte(i % 256)
	}
	wr.Write(data)

	// 1. 丢弃一部分
	b.Discard(1000)
	// 2. 移出一部分给 cmd
	cmdRaw := NewBuffer()
	b.ShiftTo(2000, cmdRaw)

	// 3. 验证剩余部分
	require.Equal(t, 10240-1000-2000, b.Len())
	require.Equal(t, data[3000:], b.Bytes())

	// 4. 验证移出部分
	require.Equal(t, data[1000:3000], cmdRaw.Bytes())
}

func TestShiftTo_Isolation(t *testing.T) {
	// 1. 初始化环境：确保数据跨越 SmallChunk 並进入 BigChunk
	b := NewBuffer() // 假設初始化函數
	wr := b.NewWriter()

	// 寫入足夠数据：1个 SmallChunk + 1个 BigChunk
	data1 := make([]byte, SmallChunkSize)
	for i := range data1 {
		data1[i] = 'A'
	}
	wr.Write(data1)

	data2 := make([]byte, ChunkSize)
	for i := range data2 {
		data2[i] = 'B'
	}
	wr.Write(data2)

	// 当前布局：[AAAA... (Small)] [BBBB... (BigIdx 0)]
	// 总长度：SmallChunkSize + ChunkSize

	// 2. 执行 ShiftTo：在大页中间截斷
	// 目标：移走 SmallChunk + 半个 BigChunk
	shiftLen := SmallChunkSize + (ChunkSize / 2)
	target := NewBuffer()
	b.ShiftTo(shiftLen, target)

	// 3. 获取 target 和 b 的內容视图（假设有 Bytes() 方法或类似读取方式）
	// 此時 target 应该持有原始 BigChunk 的前半段 'B'
	// b 应该持有原始 BigChunk 的后半段 'B'

	targetBytes := target.Bytes()
	bBytes := b.Bytes()

	// 4. 【关键点】修改原 Buffer b 的数据
	// 既然要求物理隔离，修改 b 不應影响 target
	bBytes[0] = 'X' // 修改它

	// 5. 验证隔离
	// 检查 target 的末尾字节（即拆分点前的最后一个字节）
	targetLastByte := targetBytes[len(targetBytes)-1]

	if targetLastByte == 'X' {
		t.Errorf("物理隔离失敗！修改原 Buffer 影响了 target。targetLastByte: %c", targetLastByte)
	} else if targetLastByte == 'B' {
		t.Logf("物理隔离成功：target 数据保持为 '%c'，不受原 Buffer 修改为 'X' 的影响", targetLastByte)
	} else {
		t.Errorf("数据错误：預期 'B'，实际 '%c'", targetLastByte)
	}

	// 6. 验证指針地址（進階）
	// 如果底层是 []*Chunk，可以反射检查指針是否相同
}

func TestBuffer_Truncate(t *testing.T) {
	// 假设常量定义（需根据你实际的 buffer.go 修改）
	// const SmallChunkSize = 64
	// const ChunkSize = 4096

	t.Run("仅在 Small 页内截断", func(t *testing.T) {
		b := NewBuffer()
		data := make([]byte, 32) // 小于 SmallChunkSize
		copy(data, "0123456789abcdef0123456789abcdef")
		b.NewWriter().Write(data)

		oldVersion := b.version
		b.Truncate(10)

		if b.Len() != 10 {
			t.Errorf("长度错误: 期望 10, 得到 %d", b.Len())
		}
		if b.version != oldVersion+1 {
			t.Error("版本号未增加")
		}
		if string(b.Bytes()) != "0123456789" {
			t.Errorf("内容错误: %s", string(b.Bytes()))
		}
		if len(b.big) != 0 {
			t.Error("不应存在大页")
		}
	})

	t.Run("跨大页截断并回收物理页", func(t *testing.T) {
		b := NewBuffer()
		wr := b.NewWriter()

		// 构造足以跨越 3 个大页的数据
		// 数据量：SmallPage + 3 * ChunkSize
		pageSize := 4096 // 假设 ChunkSize
		total := 64 + pageSize*3
		wr.Write(make([]byte, total))

		if len(b.big) < 3 {
			t.Fatalf("前置条件失败: 大页数量不足 %d", len(b.big))
		}

		// 截断到只剩第一个大页的一部分（保留 Small + 第一个大页的一半）
		keepLen := 64 + pageSize/2
		oldVersion := b.version
		b.Truncate(keepLen)

		if b.Len() != keepLen {
			t.Errorf("长度错误: 期望 %d, 得到 %d", keepLen, b.Len())
		}
		if b.version != oldVersion+1 {
			t.Error("版本号未增加")
		}
		// 物理页回收检查：应只剩 1 个大页
		if len(b.big) != 1 {
			t.Errorf("物理页回收失败: 期望 1 个大页, 得到 %d", len(b.big))
		}
	})

	t.Run("完全释放大页回到 Small 状态", func(t *testing.T) {
		b := NewBuffer()
		wr := b.NewWriter()

		// 1. 先写一小段数据（确保 hasSmall 保持为 true）
		wr.Write([]byte("init-small"))
		if !b.hasSmall {
			t.Fatal("初始化后 hasSmall 应该为 true")
		}

		// 2. 追加大数据（触发大页分配，但 hasSmall 依然为 true）
		// 构造足以跨越多个大页的数据
		bigData := make([]byte, 8192)
		wr.Write(bigData)

		if len(b.big) == 0 {
			t.Fatal("追加大数据后应该分配了大页")
		}
		if !b.hasSmall {
			t.Fatal("追加写入不应改变 hasSmall 策略状态")
		}

		// 3. 截断到最初的小段数据长度（例如 10 字节）
		b.Truncate(10)

		// 4. 验证物理回收
		if b.Len() != 10 {
			t.Errorf("长度错误: 得到 %d", b.Len())
		}
		if len(b.big) != 0 {
			t.Errorf("大页未完全释放: 仍有 %d 个大页", len(b.big))
		}
		if !b.hasSmall {
			t.Error("应当保留 Small 策略标记")
		}
		if b.capacity != SmallChunkSize {
			t.Errorf("容量不匹配: 期望 %d, 得到 %d", SmallChunkSize, b.capacity)
		}
	})

	t.Run("幂等性测试（n >= length 不应修改）", func(t *testing.T) {
		b := NewBuffer()
		b.NewWriter().Write([]byte("hello"))

		oldVersion := b.version
		b.Truncate(5)  // 等于长度
		b.Truncate(10) // 大于长度

		if b.version != oldVersion {
			t.Error("不应修改版本号")
		}
	})

	t.Run("边界测试：截断到 0", func(t *testing.T) {
		b := NewBuffer()
		b.NewWriter().Write(make([]byte, 100))

		b.Truncate(0)

		if b.Len() != 0 {
			t.Error("长度应为 0")
		}
		// 验证是否触发了 Free (物理页应全部清理)
		if b.small != nil || b.big != nil {
			t.Error("物理资源未完全释放")
		}
	})
}
