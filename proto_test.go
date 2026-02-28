package redcon

import (
	"bufio"
	"github.com/stretchr/testify/require"
	"strings"
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

func TestRespond_Type(t *testing.T) {
	rsp := NewRespond()
	defer rsp.Free()
	ty := rsp.Type()
	require.Zero(t, ty)
}

func TestReadUntilCRLF(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		bufSize int // 模拟 bufio 的缓冲区大小
		want    string
		wantErr bool
	}{
		{
			name:    "标准简单行",
			input:   "PING\r\n",
			bufSize: 1024,
			want:    "PING\r\n",
		},
		{
			name:    "包含孤立的n但不是rn",
			input:   "HELLO\nWORLD\r\n",
			bufSize: 1024,
			want:    "HELLO\nWORLD\r\n",
		},
		{
			name:    "超长行：触发ErrBufferFull",
			input:   strings.Repeat("A", 20) + "\r\n",
			bufSize: 10, // 强制 10 字节就溢出
			want:    strings.Repeat("A", 20) + "\r\n",
		},
		{
			name:    "极端情况：r和n被切分在两个包/缓冲区",
			input:   "DATA...\r\n",
			bufSize: 8, // 恰好让 \r 落在缓冲区末尾
			want:    "DATA...\r\n",
		},
		{
			name:    "空行读取",
			input:   "\r\n",
			bufSize: 1024,
			want:    "\r\n",
		},
		{
			name:    "只有n没有r的情况(持续读取直至遇到rn)",
			input:   "line1\nline2\nline3\r\n",
			bufSize: 1024,
			want:    "line1\nline2\nline3\r\n",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := NewRespond()
			// 使用自定义大小的 Reader 模拟边界
			reader := bufio.NewReaderSize(strings.NewReader(tt.input), tt.bufSize)

			got, err := r.readUntilCRLF(reader)
			if (err != nil) != tt.wantErr {
				t.Fatalf("unexpected error: %v", err)
			}

			if string(got.Bytes()) != tt.want {
				t.Errorf("got %q, want %q", string(got.Bytes()), tt.want)
			}
		})
	}
}
