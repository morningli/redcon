package redcon

import (
	"github.com/stretchr/testify/require"
	"testing"
)

func TestRespond_Data(t *testing.T) {
	rsp := NewRespond()
	defer rsp.Free()
	rsp.WriteBulkString("bulk")
	require.Len(t, rsp.Data(), 1)
	for _, d := range rsp.Data() {
		t.Log(string(d))
	}
}
