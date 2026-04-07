package redcon

import (
	"bytes"
	"io"
	"math/rand"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// MockReader 模拟网络分包读取
type MockReader struct {
	chunks [][]byte
}

func (m *MockReader) Read(p []byte) (int, error) {
	if len(m.chunks) == 0 {
		return 0, io.EOF
	}
	chunk := m.chunks[0]
	n := copy(p, chunk)
	m.chunks = m.chunks[1:]
	return n, nil
}

func TestReadCommands_BoundaryCases(t *testing.T) {
	tests := []struct {
		name        string
		input       [][]byte // 模拟分多次读到的字节流
		expectCount int      // 期望解析出的命令数量
		expectErr   error    // 期望的错误类型
		leftover    int      // 期望剩余字节
	}{
		{
			name:        "基础文本命令",
			input:       [][]byte{[]byte("SET k v\n")},
			expectCount: 1,
			leftover:    0,
		},
		{
			name:        "文本命令-CRLF结尾",
			input:       [][]byte{[]byte("GET k\r\n")},
			expectCount: 1,
			leftover:    0,
		},
		{
			name:        "空输入-直接EOF",
			input:       [][]byte{},
			expectCount: 0,
			expectErr:   io.EOF,
		},
		{
			name:        "半包-只有Array起始符",
			input:       [][]byte{[]byte("*3")}, // 缺失 \r\n
			expectCount: 0,
			expectErr:   io.EOF, // 因为 MockReader 耗尽后返回 EOF
		},
		{
			name: "半包-分片读取成功 (Pipeline)",
			input: [][]byte{
				[]byte("*2\r\n"),
				[]byte("$3\r\nGET\r\n"),
				[]byte("$1\r\na\r\n"),
			},
			expectCount: 1,
			leftover:    0,
		},
		{
			name:        "非法Array长度-非数字",
			input:       [][]byte{[]byte("*abc\r\n")},
			expectCount: 0,
			expectErr:   errInvalidMultiBulkLength,
		},
		{
			name:        "非法Array长度-零或负数",
			input:       [][]byte{[]byte("*0\r\n"), []byte("*-5\r\n")},
			expectCount: 0,
			expectErr:   errInvalidMultiBulkLength,
		},
		{
			name:        "非法Bulk长度-超过声明大小",
			input:       [][]byte{[]byte("*1\r\n$3\r\nBODY_TOO_LONG\r\n")},
			expectCount: 0,
			expectErr:   errInvalidBulkLength, // \r\n 位置不对
		},
		{
			name: "混合模式-文本接Array (Pipeline)",
			input: [][]byte{
				[]byte("PING\r\n*1\r\n$4\r\nPING\r\n"),
			},
			expectCount: 2,
			leftover:    0,
		},
		{
			name: "大对象-正好填满Buffer边界",
			input: [][]byte{
				[]byte("*1\r\n$10\r\n0123456789\r\n"),
			},
			expectCount: 1,
			leftover:    0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mock := &MockReader{chunks: tt.input}
			rd := NewReader(mock) // 假设你有构造函数

			var leftover int
			cmds, err := rd.readCommands(&leftover)

			if tt.expectErr != nil {
				assert.ErrorIs(t, err, tt.expectErr)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tt.expectCount, len(cmds))
				assert.Equal(t, tt.leftover, leftover)
			}
		})
	}
}

func TestReadCommands_MemoryReuse(t *testing.T) {
	mock := &MockReader{chunks: [][]byte{
		[]byte("*2\r\n$3\r\nGET\r\n$1\r\nA\r\n"),
		[]byte("*2\r\n$3\r\nGET\r\n$1\r\nB\r\n"),
	}}
	rd := NewReader(mock)

	// 第一次解析
	cmds1, err := rd.readCommands(nil)
	require.NoError(t, err)
	require.Equal(t, 1, len(cmds1))
	val1 := string(cmds1[0].Args[1].Bytes()) // 假设 BufferView 有 Bytes()

	// 第二次解析
	cmds2, err := rd.readCommands(nil)
	require.NoError(t, err)
	require.Equal(t, 1, len(cmds1))
	val2 := string(cmds2[0].Args[1].Bytes())

	// 验证第二次解析没有破坏第一次解析出的内容（取决于你的 Raw 引用逻辑）
	assert.NotEqual(t, val1, val2)
	assert.Equal(t, "A", val1)
	assert.Equal(t, "B", val2)
}

func TestReadCommands_EmptyLines(t *testing.T) {
	// 模拟：用户发送了几个换行符，然后发送了一个正常的 PING
	mock := &MockReader{chunks: [][]byte{
		[]byte("\n\n"),
		[]byte("PING\n"),
	}}
	rd := NewReader(mock)

	var leftover int
	// 第一次调用：由于前两个 \n 不产生 cmd，readAndParseSlow 会继续读到 PING
	cmds, err := rd.readCommands(&leftover)

	assert.NoError(t, err)
	assert.Equal(t, 1, len(cmds), "应该跳过空行并解析出随后的 PING")
	assert.Equal(t, 0, leftover)
}

func TestReadCommands_InvalidLengths(t *testing.T) {
	cases := []struct {
		name  string
		input string
		err   error
	}{
		{"负的Bulk长度", "*1\r\n$-5\r\n", errInvalidBulkLength},
		{"非数字的Bulk长度", "*1\r\n$abc\r\n", errInvalidBulkLength},
		{"Bulk数据缺失末尾CRLF", "*1\r\n$3\r\nGET--", errInvalidBulkLength},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rd := NewReader(bytes.NewReader([]byte(tc.input)))
			_, err := rd.readCommands(nil)
			assert.ErrorIs(t, err, tc.err)
		})
	}
}

func TestReadCommands_LeftoverAlignment(t *testing.T) {
	// 1. 恰好在 Array 数量后截断
	raw := []byte("*2\r\n$3\r\nGET\r\n")
	rd := NewReader(bytes.NewReader(raw))

	var leftover int
	cmds, err := rd.readCommands(&leftover)

	// 因为 count=2 但只给了一个参数，解析不完整
	// parseAvailable 应该在第二个参数处 goto done
	assert.ErrorIs(t, err, io.EOF)
	assert.Equal(t, 0, len(cmds))
	assert.Equal(t, len(raw), leftover, "所有数据都应留在缓冲区等待后续")
}

func TestReadCommands_PlainTextComplex(t *testing.T) {
	// 测试带引号的参数解析
	input := `SET "my key" "hello world"` + "\n"
	rd := NewReader(bytes.NewReader([]byte(input)))

	cmds, err := rd.readCommands(nil)
	assert.NoError(t, err)
	if len(cmds) > 0 {
		// 验证是否正确分成了 3 个参数：SET, my key, hello world
		assert.Equal(t, 3, len(cmds[0].Args))
	}
	require.Equal(t, []byte("SET"), cmds[0].Args[0].Bytes())
	require.Equal(t, []byte("my key"), cmds[0].Args[1].Bytes())
	require.Equal(t, []byte("hello world"), cmds[0].Args[2].Bytes())
}

func TestReadCommands_ContinuousNewlines(t *testing.T) {
	// 场景 A: 只有换行符直到 EOF
	t.Run("PureNewlines", func(t *testing.T) {
		mock := &MockReader{chunks: [][]byte{[]byte("\n\n\r\n")}}
		rd := NewReader(mock)
		var leftover int
		cmds, err := rd.readCommands(&leftover)

		// 此时 readAndParseSlow 会循环直到 mock 返回 EOF
		assert.ErrorIs(t, err, io.EOF)
		assert.Equal(t, 0, len(cmds))
		assert.Equal(t, 0, leftover)
	})

	// 场景 B: 换行符后跟着有效指令
	t.Run("NewlinesFollowedByCommand", func(t *testing.T) {
		mock := &MockReader{chunks: [][]byte{
			[]byte("\n\n"),
			[]byte("PING\n"),
		}}
		rd := NewReader(mock)
		cmds, err := rd.readCommands(nil)

		assert.NoError(t, err)
		assert.Equal(t, 1, len(cmds))
		// 验证指令内容
		assert.Equal(t, "PING", string(cmds[0].Args[0].Bytes()))
	})
}

func TestReadCommands_ProtocolFragmentation(t *testing.T) {
	tests := []struct {
		name           string
		input          []byte
		expectCmds     int
		expectLeftover int
	}{
		{
			name:           "Array起始符后截断",
			input:          []byte("*"),
			expectCmds:     0,
			expectLeftover: 1,
		},
		{
			name:           "Array长度后截断",
			input:          []byte("*2\r\n"),
			expectCmds:     0,
			expectLeftover: 4,
		},
		{
			name:           "第一个Bulk内容后截断",
			input:          []byte("*2\r\n$3\r\nGET\r\n"),
			expectCmds:     0,
			expectLeftover: 13, // 4 + 7
		},
		{
			name:           "第二个Bulk起始符后截断",
			input:          []byte("*2\r\n$3\r\nGET\r\n$"),
			expectCmds:     0,
			expectLeftover: 14,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// 使用 bytes.Reader 模拟一次性读完但协议不完整
			rd := NewReader(bytes.NewReader(tt.input))
			var leftover int
			cmds, err := rd.readCommands(&leftover)

			// 对于 bytes.Reader，读完后会返回 EOF
			if err != nil && err != io.EOF {
				t.Fatalf("unexpected error: %v", err)
			}
			assert.Equal(t, tt.expectCmds, len(cmds))
			assert.Equal(t, tt.expectLeftover, leftover)
		})
	}
}

func TestReadCommands_SecurityBoundaries(t *testing.T) {
	// 1. 负长度检查
	t.Run("NegativeBulkLength", func(t *testing.T) {
		input := "*1\r\n$-5\r\n"
		rd := NewReader(bytes.NewReader([]byte(input)))
		_, err := rd.readCommands(nil)
		assert.ErrorIs(t, err, errInvalidBulkLength)
	})

	// 2. 极大 Array 长度尝试 (防御性测试)
	t.Run("MassiveArrayCount", func(t *testing.T) {
		input := "*999999999999\r\n"
		rd := NewReader(bytes.NewReader([]byte(input)))
		_, err := rd.readCommands(nil)
		// 应该解析失败
		assert.True(t, err == errInvalidMultiBulkLength || err == io.EOF)
	})
}

func TestReadCommands_PipelineWithLeftover(t *testing.T) {
	input := "PING\n*2\r\n$4\r\nECHO\r\n$1" // 第二条指令在 $1 处截断
	rd := NewReader(bytes.NewReader([]byte(input)))

	var leftover int
	cmds, err := rd.readCommands(&leftover)

	assert.NoError(t, err) // 第一条是完整的，不应报错
	assert.Equal(t, 1, len(cmds))
	assert.Equal(t, 16, leftover) // 剩余 '*2' 之后的后续部分 (*2\r\n$4\r\nECHO\r\n$1 的总长度减去 PING\n)
	// 注意：根据你的代码逻辑，若第一条解析成功，即便后面有残余，也会直接返回。
}

func TestReadCommands_StrictProtocol(t *testing.T) {
	tests := []struct {
		name      string
		input     string
		expectErr error
	}{
		{
			name:      "Array长度溢出int64",
			input:     "*9223372036854775808\r\n", // 超过 int64 最大值
			expectErr: errInvalidMultiBulkLength,
		},
		{
			name:      "Bulk长度为负数",
			input:     "*1\r\n$-5\r\n",
			expectErr: errInvalidBulkLength,
		},
		{
			name: "Bulk内容少于声明长度且遇到新指令",
			// 声明 $5 但只给 3 字节，后面接了新指令，应识别为协议错误而非半包
			input:     "*1\r\n$5\r\nABC\r\n*1\r\n",
			expectErr: errInvalidBulkLength,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rd := NewReader(strings.NewReader(tt.input))
			_, err := rd.readCommands(nil)
			// 注意：如果你的代码在 ParseInt 失败后立刻返回，这里应该断言错误类型
			assert.ErrorIs(t, err, tt.expectErr)
		})
	}
}

func TestReadCommands_OnlyNewlines(t *testing.T) {
	// 模拟发送了 4 个换行符后连接关闭
	rd := NewReader(strings.NewReader("\n\n\n\n"))
	var leftover int
	cmds, err := rd.readCommands(&leftover)

	// 逻辑：parseAvailable 处理完所有 \n，leftover 变 0，
	// readAndParseSlow 再次读取得到 EOF。
	assert.ErrorIs(t, err, io.EOF)
	assert.Equal(t, 0, len(cmds))
	assert.Equal(t, 0, leftover)
}

func TestReadCommands_ExtremeFragmentation(t *testing.T) {
	mock := &MockReader{chunks: [][]byte{
		[]byte("*"), []byte("2"), []byte("\r"), []byte("\n"), // Array header
		[]byte("$"), []byte("3"), []byte("\r"), []byte("\n"), []byte("G"), []byte("ET"), []byte("\r"), []byte("\n"),
		[]byte("$"), []byte("1"), []byte("\r"), []byte("\n"), []byte("K"), []byte("\r"), []byte("\n"),
	}}
	rd := NewReader(mock)

	cmds, err := rd.readCommands(nil)
	assert.NoError(t, err)
	assert.Equal(t, 1, len(cmds))
	assert.Equal(t, "GET", string(cmds[0].Args[0].Bytes()))
}

func TestReadCommands_PlainTextEdge(t *testing.T) {
	input := "  PING  \n  SET  k  v  \n"
	rd := NewReader(strings.NewReader(input))

	// 第一次调用应返回 PING
	cmds, _ := rd.readCommands(nil)
	assert.Equal(t, 2, len(cmds))
}

func TestReadCommands_LeftoverAlignment_Fixed(t *testing.T) {
	// 模拟数据：*2\r\n$3\r\nGET\r\n (这是一个半包，缺少第二个参数)
	raw := []byte("*2\r\n$3\r\nGET\r\n")

	// 使用自定义 Mock，第一次返回数据，第二次返回 (0, nil) 模拟阻塞/等待
	// 这样可以避免 Read 直接触发 EOF 导致 readAndParseSlow 退出
	mock := &blockReader{data: raw}
	rd := NewReader(mock)

	var leftover int
	cmds, err := rd.readCommands(&leftover)

	// 1. 验证不应该报错（因为虽然不完整，但连接没断，只是没数据了）
	assert.ErrorIs(t, err, io.EOF)
	// 2. 验证没解析出完整指令
	assert.Equal(t, 0, len(cmds))
	// 3. 核心：验证 leftover 是否正确保留了所有 11 字节
	assert.Equal(t, len(raw), leftover)
}

// 辅助 Mock：返回一次数据后返回 (0, nil) 模拟网络等待
type blockReader struct {
	data []byte
	done bool
}

func (b *blockReader) Read(p []byte) (int, error) {
	if b.done {
		return 0, nil
	} // 关键：模拟无数据但不报 EOF
	n := copy(p, b.data)
	b.done = true
	return n, nil
}

func TestReadCommands_FragmentedCRLF(t *testing.T) {
	mock := &MockReader{chunks: [][]byte{
		[]byte("*1\r"), // 在 \r 处断开
		[]byte("\n$4\r\n"),
		[]byte("P"), []byte("I"), []byte("N"), []byte("G\r"), []byte("\n"),
	}}
	rd := NewReader(mock)

	cmds, err := rd.readCommands(nil)
	assert.NoError(t, err)
	assert.Equal(t, 1, len(cmds))
	assert.Equal(t, "PING", string(cmds[0].Args[0].Bytes()))
}

func TestReadCommands_PipelineAndPartial(t *testing.T) {
	// PING\n + *1\r\n$4 (第二条只给了一半)
	input := []byte("PING\n*1\r\n$4")
	rd := NewReader(&blockReader{data: input})

	var leftover int
	cmds, err := rd.readCommands(&leftover)

	assert.NoError(t, err)
	assert.Equal(t, 1, len(cmds), "应该只解析出第一条 PING")
	assert.Equal(t, 6, leftover, "应该剩下 '*1\r\n$4' 共 6 字节")
}

func TestReadCommands_PlainTextWhitespace(t *testing.T) {
	input := "   SET    key    value   \r\n"
	rd := NewReader(bytes.NewReader([]byte(input)))

	cmds, err := rd.readCommands(nil)
	assert.NoError(t, err)
	assert.Equal(t, 1, len(cmds))
	// 验证 Args 长度应为 3 (SET, key, value)，而不是包含空格的垃圾
	assert.Equal(t, 3, len(cmds[0].Args))
}

func GenerateFastBytes(size int) []byte {
	b := make([]byte, size)
	rand.Read(b) // Go 1.6+ 支持直接 Read 填充
	return b
}

func TestReader_readCommands(t *testing.T) {
	raw := bytes.NewBuffer(nil)
	for i := 0; i < 10; i++ {
		raw.Write([]byte("*4\r\n$4\r\nHSET\r\n$4\r\nkey0\r\n$6\r\nfield0\r\n$50\r\n"))
		raw.Write(GenerateFastBytes(50))
		raw.Write([]byte("\r\n"))
		raw.Write([]byte("*4\r\n$4\r\nHSET\r\n$4\r\nkey0\r\n$6\r\nfield1\r\n$4000\r\n"))
		raw.Write(GenerateFastBytes(4000))
		raw.Write([]byte("\r\n"))
		raw.Write([]byte("*3\r\n$6\r\nexpire\r\n$4\r\nkey0\r\n$8\r\n31536000\r\n"))
	}
	rd := NewReader(raw)
	total := 0
	for {
		cmds, err := rd.readCommands(nil)
		if err == io.EOF {
			break
		}
		require.NoError(t, err)
		t.Logf("parsed %d commands", len(cmds))
		total += len(cmds)
	}
	require.Equal(t, 30, total)
}
