package redcon

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
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
			wr := r.NewWriter()
			// 使用自定义大小的 Reader 模拟边界
			reader := bufio.NewReaderSize(strings.NewReader(tt.input), tt.bufSize)

			got, err := r.CopyLineTo(reader, wr)
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

func TestDecodeStream_DeepArray(t *testing.T) {
	// 構造一個嵌套 Array: *2\r\n$3\r\nGET\r\n*1\r\n$4\r\nINFO\r\n
	data := []byte("*2\r\n$3\r\nGET\r\n*1\r\n$4\r\nINFO\r\n")
	rd := bufio.NewReader(bytes.NewReader(data))
	r := NewRespond()

	err := r.decodeStream(rd, nil)
	if err != nil {
		t.Fatalf("解析失敗: %v", err)
	}

	// 驗證 Buffer 最終內容
	if string(r.Buffer.Bytes()) != string(data) {
		t.Errorf("內容不一致\n期望: %q\n得到: %q", data, r.Buffer.Bytes())
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

	r := NewRespond()
	wr := r.NewWriter()

	view, err := r.CopyLineTo(rd, wr)
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

func TestRespond_ReadFrom(t *testing.T) {
	buf := NewRespond()

	input := []byte("*1\r\n$4\r\nkeys\r\n")
	rd := bufio.NewReaderSize(bytes.NewReader(input), 1024)
	n, err := buf.ReadFrom(rd)
	require.NoError(t, err)
	require.Equal(t, input, buf.Bytes())
	require.Equal(t, int64(len(input)), int64(n))
}

func BenchmarkRespond_ReadUntilCRLF(b *testing.B) {
	shortLine := []byte("PING\r\n")
	longLine := append(bytes.Repeat([]byte("a"), 5120), []byte("\r\n")...)

	// 注意：我们将 runBenchmarkReadUntil 的逻辑直接内联或重构，以支持对象复用
	b.Run("Short-Line-12B", func(b *testing.B) {
		runBenchmarkReadUntil(b, shortLine)
	})

	b.Run("Long-Line-5KB", func(b *testing.B) {
		runBenchmarkReadUntil(b, longLine)
	})
}

func runBenchmarkReadUntil(b *testing.B, data []byte) {
	// 1. 【关键】将对象初始化移出循环，模拟真实的 3w 连接池化场景
	r := &Respond{
		Buffer: NewBuffer(),
	}
	// 预分配一个足够大的 Reader 供复用
	rd := bufio.NewReaderSize(nil, 16384)
	// 预准备数据源，避免在循环内分配 bytes.Reader
	readerSource := bytes.NewReader(data)

	b.ReportAllocs()
	b.ResetTimer() // 2. 【关键】重置计时器，只测量 readUntilCRLF 逻辑

	for i := 0; i < b.N; i++ {
		// 3. 【关键】复用 Buffer
		// 确保你的 Buffer.Reset() 只是将 length 设为 0，而不释放已有的 BigChunks
		r.Buffer.Reset()
		wr := r.NewWriter()

		// 4. 重置数据源和 Reader 指针
		readerSource.Reset(data)
		rd.Reset(readerSource)

		_, err := r.CopyLineTo(rd, wr)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkRespond_DecodeStream(b *testing.B) {
	// 準備不同類型的 Redis 協議數據
	cases := []struct {
		name string
		data []byte
	}{
		// 修正后的测试数据
		{"Ping", []byte("+PONG\r\n")},                                         // 必须带 '+'
		{"Bulk-1K", []byte("$1024\r\n" + strings.Repeat("a", 1024) + "\r\n")}, // 结尾必须有 \r\n
		{"Array-MGet", []byte("*3\r\n$3\r\nGET\r\n$4\r\nkey1\r\n$4\r\nkey2\r\n")},
	}

	for _, tc := range cases {
		b.Run(tc.name, func(b *testing.B) {
			// 1. 初始化環境，確保對象複用 (0 Alloc)
			r := NewRespond()
			rd := bufio.NewReaderSize(nil, 16384)
			src := bytes.NewReader(tc.data)

			b.ReportAllocs()
			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				// 2. 重置狀態，不重新分配物理 Chunk
				r.Buffer.Reset()
				r.wr.Sync()
				src.Reset(tc.data)
				rd.Reset(src)

				// 3. 執行重構後的 FillStreaming 解析
				err := r.decodeStream(rd, nil)
				if err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func TestRespond_TryGetArraySize(t *testing.T) {
	t.Run("non_array_returns_1", func(t *testing.T) {
		var r = NewRespond()
		defer r.Free()
		r.WriteRaw([]byte("+OK\r\n"))
		n, err := r.TryGetArraySize()
		if err != nil {
			t.Fatalf("unexpected err: %v", err)
		}
		if n != 1 {
			t.Fatalf("expected 1, got %d", n)
		}
	})

	t.Run("array_returns_count", func(t *testing.T) {
		var r = NewRespond()
		defer r.Free()
		r.WriteRaw([]byte("*2\r\n$1\r\na\r\n$1\r\nb\r\n"))
		n, err := r.TryGetArraySize()
		if err != nil {
			t.Fatalf("unexpected err: %v", err)
		}
		if n != 2 {
			t.Fatalf("expected 2, got %d", n)
		}
	})

	t.Run("array_incomplete_returns_error", func(t *testing.T) {
		var r = NewRespond()
		defer r.Free()
		r.WriteRaw([]byte("*2\r")) // missing LF
		_, err := r.TryGetArraySize()
		if err == nil {
			t.Fatalf("expected error, got nil")
		}
	})
}

func TestRespond_Type2(t *testing.T) {
	t.Run("empty", func(t *testing.T) {
		b := NewRespond()
		ty := b.Type()
		require.Zero(t, ty)
		b.Free()
	})

	t.Run("String", func(t *testing.T) {
		b := NewRespond()
		b.WriteString("foo")
		ty := b.Type()
		require.Equal(t, String, ty)
		b.Free()
	})

	t.Run("Bulk", func(t *testing.T) {
		b := NewRespond()
		b.WriteBulk([]byte("foo"))
		ty := b.Type()
		require.Equal(t, Bulk, ty)
		b.Free()
	})

	t.Run("Error", func(t *testing.T) {
		b := NewRespond()
		b.WriteError("foo")
		ty := b.Type()
		require.Equal(t, Error, ty)
		b.Free()
	})

	t.Run("Array", func(t *testing.T) {
		b := NewRespond()
		b.WriteArray(-1)
		ty := b.Type()
		require.Equal(t, Array, ty)
		b.Free()
	})

	t.Run("Integer", func(t *testing.T) {
		b := NewRespond()
		b.WriteInt(1)
		ty := b.Type()
		require.Equal(t, Integer, ty)
		b.Free()
	})
}

func TestRequest_WriteArray(t *testing.T) {
	r := NewRequest()
	defer r.Free()
	r.WriteArray(1)
	r.WriteBulk([]byte("ping"))
	require.Equal(t, []byte("*1\r\n$4\r\nping\r\n"), r.Raw.Bytes())
	require.Equal(t, 1, len(r.Args))
	require.Equal(t, []byte("ping"), r.Args[0].Bytes())
}
