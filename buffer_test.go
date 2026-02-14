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
