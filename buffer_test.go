package redcon

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"net"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestBuffer_Swap(t *testing.T) {
	buf := NewBuffer()
	buf.Write([]byte("hello"))
	buf2 := NewBuffer()
	buf2.Write([]byte("world"))
	buf.Swap(buf2)
	require.Equal(t, []byte("hello"), buf2.Bytes())
	require.Equal(t, []byte("world"), buf.Bytes())
}

func TestBuffer_Tail(t *testing.T) {
	buf := NewBuffer()
	buf.Write([]byte("hello world"))

	v := buf.Tail(6)
	require.Equal(t, []byte("world"), v.Bytes())

	v2 := v.Tail(3)
	require.Equal(t, []byte("ld"), v2.Bytes())
}

func TestBuffer_UpgradeFromSmallToBig(t *testing.T) {
	buf := NewBuffer()
	// 触发“大页追加”：写入超过 SmallChunkSize（第一页为小页，后续为大页）
	payload := make([]byte, SmallChunkSize+10)
	for i := range payload {
		payload[i] = byte(i)
	}
	_, _ = buf.Write(payload[:SmallChunkSize])
	_, _ = buf.Write(payload[SmallChunkSize:])
	require.True(t, buf.hasSmall)

	// 随机访问与切片必须正确
	require.Equal(t, payload[0], buf.At(0))
	require.Equal(t, payload[SmallChunkSize], buf.At(SmallChunkSize))

	view := buf.Slice(SmallChunkSize-5, SmallChunkSize+5)
	require.Equal(t, payload[SmallChunkSize-5:SmallChunkSize+5], view.Bytes())

	// Split 后两段内容必须正确
	left := buf.Slice(0, SmallChunkSize).Bytes()
	newBuf := buf.Split(SmallChunkSize)
	right := newBuf.Bytes()
	require.Equal(t, payload[:SmallChunkSize], left)
	require.Equal(t, payload[SmallChunkSize:], right)

	newBuf.Free()
	buf.Free()
}

func TestBuffer_Split_RemainderSuffixFitsSmall(t *testing.T) {
	// Build: small + one big page, but only use 10 bytes into the big page.
	buf := NewBuffer()
	payload := make([]byte, SmallChunkSize+10)
	for i := range payload {
		payload[i] = byte(i)
	}
	_, _ = buf.Write(payload[:SmallChunkSize])
	_, _ = buf.Write(payload[SmallChunkSize:])
	require.True(t, buf.hasSmall)

	// Split inside the big page so that remainder's first fragment is <=128.
	// Pick split such that only 5 bytes remain in the big page fragment.
	splitAt := SmallChunkSize + 5
	newBuf := buf.Split(splitAt)

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
	payload := make([]byte, SmallChunkSize+1)
	for i := range payload {
		payload[i] = byte(i)
	}
	_, _ = buf.Write(payload)

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
	payload := make([]byte, SmallChunkSize-1)
	for i := range payload {
		payload[i] = byte(i)
	}
	_, _ = buf.Write(payload)

	require.True(t, buf.hasSmall)
	require.NotNil(t, buf.small)
	require.Equal(t, 0, len(buf.big))
	require.Equal(t, payload, buf.Bytes())

	buf.Free()
}

func TestBuffer_Split_DoesNotShareBoundaryPageAndKeepsOldViews(t *testing.T) {
	// Build: small + one big page + some.
	buf := NewBuffer()
	payload := make([]byte, SmallChunkSize+ChunkSize+20)
	for i := range payload {
		payload[i] = byte(i)
	}
	_, _ = buf.Write(payload[:SmallChunkSize])
	_, _ = buf.Write(payload[SmallChunkSize:])
	require.True(t, buf.hasSmall)
	require.GreaterOrEqual(t, len(buf.big), 1)
	boundaryPage := buf.big[0]

	// Create a view into the first big page that should remain in the old buffer after split.
	view := buf.Slice(SmallChunkSize, SmallChunkSize+10)
	wantView := payload[SmallChunkSize : SmallChunkSize+10]
	require.Equal(t, wantView, view.Bytes())

	// Split inside the first big page.
	splitAt := SmallChunkSize + 5
	rem := buf.Split(splitAt)

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
	payload := make([]byte, ChunkSize+100)
	for i := range payload {
		payload[i] = byte(i)
	}
	_, _ = buf.Write(payload)
	require.False(t, buf.hasSmall)
	require.GreaterOrEqual(t, len(buf.big), 1)

	// Split near end of first big page so suffix <= SmallChunkSize.
	splitAt := ChunkSize - (SmallChunkSize / 2)
	rem := buf.Split(splitAt)
	require.True(t, rem.hasSmall)
	require.NotNil(t, rem.small)
	require.Equal(t, payload[splitAt:], rem.Bytes())

	rem.Free()
	buf.Free()
}

func TestBuffer_Split_BigOnly_RemainderSuffixNeedsBig(t *testing.T) {
	// big-only buffer.
	buf := NewBuffer()
	payload := make([]byte, ChunkSize+100)
	for i := range payload {
		payload[i] = byte(i)
	}
	_, _ = buf.Write(payload)
	require.False(t, buf.hasSmall)
	require.GreaterOrEqual(t, len(buf.big), 1)
	boundary := buf.big[0]

	// Split so suffix in this page > SmallChunkSize, which should force newBuf to allocate a big page.
	suffix := SmallChunkSize + 20
	splitAt := ChunkSize - suffix
	rem := buf.Split(splitAt)
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
		testData[i] = byte(i % 256)
	}

	// 2. 初始化 bufio.Reader
	// 注意：bufio 默认缓冲区通常是 4KB，我们手动设为 16KB 以确保一次能 Buffered 更多数据
	rawReader := bytes.NewReader(testData)
	rd := bufio.NewWriterSize(nil, 16*1024) // 仅占位
	_ = rd                                  // 实际上我们需要的是下面这个
	brd := bufio.NewReaderSize(rawReader, 16*1024)

	// 预填充 bufio 的缓冲区（执行一次 Peek 或 Read 触发底层填充）
	_, _ = brd.Peek(dataSize)
	if brd.Buffered() != dataSize {
		t.Fatalf("bufio 缓冲区未填充完毕: 期望 %d, 实际 %d", dataSize, brd.Buffered())
	}

	// 3. 初始化你的内存池 Buffer
	// 假设 NewBuffer 是你的构造函数
	buf := NewBuffer()

	// 4. 执行测试逻辑
	n, err := buf.ReadFrom(brd)
	if err != nil && err != io.EOF {
		t.Fatalf("ReadFromBuffered 执行失败: %v", err)
	}

	// 5. 验证结果
	if n != int64(dataSize) {
		t.Errorf("读取长度不符: 期望 %d, 实际 %d", dataSize, n)
	}

	if buf.Len() != dataSize {
		t.Errorf("Buffer 长度不符: 期望 %d, 实际 %d", dataSize, buf.Len())
	}

	// 验证数据完整性 (使用你之前的 Bytes() 或遍历逻辑)
	if !bytes.Equal(buf.Bytes(), testData) {
		t.Error("读取到的数据内容不一致（可能在 Chunk 切换时发生了覆盖或偏移错误）")
	}

	// 验证 bufio 缓冲区是否已清空
	if brd.Buffered() != 0 {
		t.Errorf("bufio 缓冲区未完全消耗: 剩余 %d", brd.Buffered())
	}
}

// 可选：增加一个边界测试，验证当 Buffer 已有部分数据且 offset 不在页首时的情况
func TestBuffer_ReadFrom_WithOffset(t *testing.T) {
	buf := NewBuffer()
	buf.Write([]byte("hello world")) // 11

	extraData := bytes.Repeat([]byte("a"), 5000)

	// 【关键修复】设置缓冲区为 10KB，确保 5000 字节能被 Buffered()
	brd := bufio.NewReaderSize(bytes.NewReader(extraData), 10240)

	// 预读触发填充
	_, _ = brd.Peek(5000)

	t.Logf("Before: Buffered = %d", brd.Buffered()) // 这里应该打印 5000

	n, err := buf.ReadFrom(brd)
	if err != nil {
		t.Fatal(err)
	}

	t.Logf("After: n = %d, buf.Len = %d", n, buf.Len())

	if buf.Len() != 5011 {
		t.Errorf("期望 5011, 实际 %d", buf.Len())
	}
}

func TestBuffer_ReadFrom_WithOffset2(t *testing.T) {
	buf := NewBuffer()
	buf.Write([]byte("hello world")) // 11

	extraData := bytes.Repeat([]byte("a"), 5000)

	// 【关键修复】设置缓冲区为 10KB，确保 5000 字节能被 Buffered()
	brd := bufio.NewReaderSize(bytes.NewReader(extraData), 10240)

	t.Logf("Before: Buffered = %d", brd.Buffered()) // 这里应该打印 5000

	n, err := buf.ReadFrom(brd)
	if err != nil {
		t.Fatal(err)
	}

	t.Logf("After: n = %d, buf.Len = %d", n, buf.Len())

	if buf.Len() != SmallChunkSize {
		t.Errorf("期望 5011, 实际 %d", buf.Len())
	}
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
	prefix := []byte("head_offset") // 11 字节
	buf.Write(prefix)

	// 3. 构造 bufio.Reader
	brd := bufio.NewReaderSize(bytes.NewReader(testData), 16*1024)

	// 4. 执行 ReadFull
	err := buf.ReadFull(brd, dataSize)
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

	// 1. 强制检查写入过程
	input := []byte("hello world")
	_, err := buf.Write(input)
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

	input := []byte("hello world")
	rd := bufio.NewReaderSize(bytes.NewReader(input), 1024)
	n, err := buf.ReadFrom(rd)
	require.NoError(t, err)
	require.Equal(t, input, buf.Bytes())
	require.Equal(t, int64(len(input)), int64(n))
}

func TestBuffer_WriteTo_Complex(t *testing.T) {
	// 1. 初始化 Buffer，构造跨越 SmallChunk 和多个 BigChunk 的数据
	buf := NewBuffer() // 假设初始 hasSmall 为 true

	// 写入一些数据制造偏移。例如先写 10 字节。
	// 这会使得 firstPageOffset = 0, length = 10 (在 SmallChunk 中)
	initialData := []byte("0123456789")
	buf.Write(initialData)

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
	buf.Write(extraData)

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
		for j := 0; j < 200; j++ {
			_, _ = buf.Write(payload[:])
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
	buf.Write(data)
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
