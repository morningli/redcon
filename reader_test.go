package redcon

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNewReader(t *testing.T) {
	rd := NewReader(bytes.NewReader([]byte("*2\r\n$7\r\nhgetall\r\n$4\r\nkey0\r\n*2\r\n$7\r\nhgetall\r\n$4\r\nkey0\r\n")))
	cmds, err := rd.readCommands(nil)
	require.NoError(t, err)
	require.Len(t, cmds, 2)
	require.Equal(t, []byte("hgetall"), cmds[0].Args[0].Bytes())
	require.Equal(t, []byte("hgetall"), cmds[1].Args[0].Bytes())
}
