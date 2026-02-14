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
