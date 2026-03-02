package redcon

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"github.com/stretchr/testify/require"
	"io"
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

const (
	ErrorInfoLogParse = "redis: not invalid array of bulk string type, but %c"
)

var (
	ErrLongRespLine           error = errors.New("redis: ERR long resp line")
	ErrShortRespLine          error = errors.New("redis: ERR short resp line")
	ErrBadBulkStringFormat    error = errors.New("redis: ERR bad bulk string format")
	ErrUnexpectedResponseLine error = errors.New("redis: ERR unexpected response line")
	ErrBadRespLineTerminator  error = errors.New("redis: ERR bad resp line terminator")
	ErrmalformedLength        error = errors.New("redis: ERR malformed length")
	ErrmalformedInteger       error = errors.New("redis: ERR malformed integer")
	ErrIllegalBytesInLength   error = errors.New("redis: ERR illegal bytes in length")
)

const (
	OK   = "OK"
	PONG = "PONG"
)

var (
	OkReply   interface{} = OK
	PongReply interface{} = PONG
)

type TestReader struct {
	br *bufio.Reader
}

func NewTestReader(br *bufio.Reader) *TestReader {
	return &TestReader{
		br: br,
	}
}

// Parse RESP

func (reader *TestReader) Parse() (interface{}, error) {
	line, err := testReadLine(reader.br)
	if err != nil {
		return nil, err
	}
	if len(line) == 0 {
		return nil, ErrShortRespLine
	}
	switch line[0] {
	case '+':
		switch {
		case len(line) == 3 && line[1] == 'O' && line[2] == 'K':
			// Avoid allocation for frequent "+OK" response.
			return OkReply, nil
		case len(line) == 5 && line[1] == 'P' && line[2] == 'O' && line[3] == 'N' && line[4] == 'G':
			// Avoid allocation in PING command benchmarks :)
			return PongReply, nil
		default:
			return string(line[1:]), nil
		}
	case '-':
		return errors.New(string(line[1:])), nil
	case ':':
		n, err := testParseInt(line[1:])
		return n, err
	case '$':
		n, err := testParseLen(line[1:])
		if n < 0 || err != nil {
			return nil, err
		}
		p := make([]byte, n)
		_, err = io.ReadFull(reader.br, p)
		if err != nil {
			return nil, err
		}
		if line, err := testReadLine(reader.br); err != nil {
			return nil, err
		} else if len(line) != 0 {
			return nil, ErrBadBulkStringFormat
		}
		return p, nil
	case '*':
		n, err := testParseLen(line[1:])
		if n < 0 || err != nil {
			return nil, err
		}
		r := make([]interface{}, n)
		for i := range r {
			r[i], err = reader.Parse()
			if err != nil {
				return nil, err
			}
		}
		return r, nil
	}
	return nil, ErrUnexpectedResponseLine
}

// Parse client -> server command request, must be array of bulk strings

func (reader *TestReader) ParseRequest() ([][]byte, error) {
	line, err := testReadLine(reader.br)
	if err != nil {
		return nil, err
	}
	if len(line) == 0 {
		return nil, ErrShortRespLine
	}
	switch line[0] {
	case '*':
		n, err := testParseLen(line[1:])
		if n < 0 || err != nil {
			return nil, err
		}
		r := make([][]byte, n)
		for i := range r {
			r[i], err = testParseBulk(reader.br)
			if err != nil {
				return nil, err
			}
		}
		return r, nil
	default:
		return nil, fmt.Errorf(ErrorInfoLogParse, line[0])
	}
}

// Parse bulk string and write it with writer w

func (reader *TestReader) ParseBulkTo(w io.Writer) error {
	line, err := testReadLine(reader.br)
	if err != nil {
		return err
	}
	if len(line) == 0 {
		return ErrShortRespLine
	}

	switch line[0] {
	case '-':
		return errors.New(string(line[1:]))
	case '$':
		n, err := testParseLen(line[1:])
		if n < 0 || err != nil {
			return err
		}

		var nn int64
		if nn, err = io.CopyN(w, reader.br, int64(n)); err != nil {
			return err
		} else if nn != int64(n) {
			return io.ErrShortWrite
		}

		if line, err := testReadLine(reader.br); err != nil {
			return err
		} else if len(line) != 0 {
			return ErrBadBulkStringFormat
		}
		return nil
	default:
		return fmt.Errorf(ErrorInfoLogParse, line[0])
	}
}

func (reader *TestReader) Buffered() int {
	return reader.br.Buffered()
}

func testReadLine(br *bufio.Reader) ([]byte, error) {
	p, err := br.ReadSlice('\n')
	if err == bufio.ErrBufferFull {
		return nil, ErrLongRespLine
	}
	if err != nil {
		return nil, err
	}
	i := len(p) - 2
	if i < 0 {
		return nil, ErrBadRespLineTerminator
	}
	if p[i] != '\r' {
		res, err := testReadLine(br)
		if err != nil {
			return nil, err
		}
		line := make([]byte, 0, 2*len(p))
		line = append(line, p...)
		line = append(line, res...)
		return line, nil
	}
	return p[:i], nil
}

// parseLen parses bulk string and array lengths.

func testParseLen(p []byte) (int, error) {
	if len(p) == 0 {
		return -1, ErrmalformedLength
	}

	if p[0] == '-' && len(p) == 2 && p[1] == '1' {
		// handle $-1 and $-1 null replies.
		return -1, nil
	}

	var n int
	for _, b := range p {
		n *= 10
		if b < '0' || b > '9' {
			return -1, ErrIllegalBytesInLength
		}
		n += int(b - '0')
	}

	return n, nil
}

// parseInt parses an integer reply.

func testParseInt(p []byte) (int64, error) {
	if len(p) == 0 {
		return 0, ErrmalformedInteger
	}

	var negate bool
	if p[0] == '-' {
		negate = true
		p = p[1:]
		if len(p) == 0 {
			return 0, ErrmalformedInteger
		}
	}

	var n int64
	for _, b := range p {
		n *= 10
		if b < '0' || b > '9' {
			return 0, ErrIllegalBytesInLength
		}
		n += int64(b - '0')
	}

	if negate {
		n = -n
	}
	return n, nil
}

func testParseBulk(br *bufio.Reader) ([]byte, error) {
	line, err := testReadLine(br)
	if err != nil {
		return nil, err
	}
	if len(line) == 0 {
		return nil, ErrShortRespLine
	}
	switch line[0] {
	case '$':
		n, err := testParseLen(line[1:])
		if n < 0 || err != nil {
			return nil, err
		}
		p := make([]byte, n)
		_, err = io.ReadFull(br, p)
		if err != nil {
			return nil, err
		}
		if line, err := testReadLine(br); err != nil {
			return nil, err
		} else if len(line) != 0 {
			return nil, ErrBadBulkStringFormat
		}
		return p, nil
	default:
		return nil, fmt.Errorf(ErrorInfoLogParse, line[0])
	}
}

func TestFastParseInt_Simple(t *testing.T) {
	// 模拟一個 BufferView (单页 Fast-Path 覆盖)
	small := &SmallChunk{}
	copy(small[10:], "-12345")
	v := BufferView{hasSmall: true, small: small, firstPageOffset: 10, length: 6}

	val, err := fastParseInt(v)
	if err != nil {
		t.Fatalf("解析失败: %v", err)
	}
	if val != -12345 {
		t.Errorf("期望 -12345, 得到 %d", val)
	}
}

func BenchmarkRespond_ReadFrom(b *testing.B) {
	buf := bytes.NewBuffer(nil)
	buf.WriteString("*200\r\n")
	for i := 0; i < 200; i++ {
		buf.WriteString("$100\r\n")
		buf.Write(payload[:])
		buf.WriteString("\r\n")
	}
	resp := buf.Bytes()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		r := NewRespond()
		reader := bufio.NewReaderSize(bytes.NewBuffer(resp), 1024)
		_, err := r.ReadFrom(reader)
		require.NoError(b, err)
		r.Free()
	}
}

func BenchmarkRespond_ReadFrom2(b *testing.B) {
	buf := bytes.NewBuffer(nil)
	buf.WriteString("*200\r\n")
	for i := 0; i < 200; i++ {
		buf.WriteString("$100\r\n")
		buf.Write(payload[:])
		buf.WriteString("\r\n")
	}
	resp := buf.Bytes()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		reader := bufio.NewReaderSize(bytes.NewBuffer(resp), 1024)
		rd := NewTestReader(reader)
		_, err := rd.Parse()
		require.NoError(b, err)
	}
}

func TestReadUntilCRLF_Boundary(t *testing.T) {
	// 构造一個模拟的 bufio.Reader
	// 数据： "FIRST\r" 然后是 "\nSECOND\r\n"
	data := []byte("FIRST\r\n")
	rd := bufio.NewReaderSize(bytes.NewReader(data), 6) // 故意设小缓冲区

	r := &Respond{Buffer: NewBuffer()}

	view, err := r.readUntilCRLF(rd)
	if err != nil {
		t.Fatalf("解析失败: %v", err)
	}

	// 验证內容
	res := view.Bytes()
	if string(res) != "FIRST\r\n" {
		t.Errorf("期望 FIRST\\r\\n, 得到 %q", string(res))
	}

	// 验证 Buffer 长度
	if r.Buffer.Len() != 7 {
		t.Errorf("Buffer 长度错误: %d", r.Buffer.Len())
	}
}
