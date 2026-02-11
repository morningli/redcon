package redcon

import (
	"github.com/stretchr/testify/require"
	"testing"
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
