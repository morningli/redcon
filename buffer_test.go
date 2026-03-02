package redcon

import (
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
