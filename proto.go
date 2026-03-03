package redcon

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"time"
)

// Request represent a command
type Request struct {
	ctx interface{}
	// Raw is a encoded RESP message.
	Raw *Buffer
	// Args is a series of arguments that make up the command.
	Args []BufferView

	ReceiveTime     time.Time
	ProcessTime     time.Time
	ProcessDoneTime time.Time
	FlushTime       time.Time
}

// NewRequest 创建一个空 Request，并初始化 Raw 缓冲区。
func NewRequest() *Request { return &Request{Raw: NewBuffer()} }

// Free 释放 Request 持有的 Raw 缓冲区。
func (r *Request) Free() { r.Raw.Free() }

// Context 返回与该请求关联的用户上下文。
func (r *Request) Context() interface{} { return r.ctx }

// SetContext 设置与该请求关联的用户上下文。
func (r *Request) SetContext(v interface{}) { r.ctx = v }

// WriteArray 向 Raw 追加一个 Array 头（元素数量为 count）。
func (r *Request) WriteArray(count int) {
	if count < 0 {
		AppendNullArray(r.Raw)
		return
	}
	AppendArray(r.Raw, count)
}

// WriteBulk 向 Raw 追加一个 Bulk，并把其 Data 视图追加到 Args。
func (r *Request) WriteBulk(bulk []byte) {
	v := AppendBulk(r.Raw, bulk)
	r.Args = append(r.Args, v)
}

// WriteRaw 直接向 Raw 追加字节（不做解析）。
func (r *Request) WriteRaw(data []byte) { _, _ = r.Raw.Write(data) }

func (r *Request) WriteTo(wr io.Writer) (int64, error) {
	if r.Raw.Len() == 0 {
		return 0, io.EOF
	}
	return r.Raw.WriteTo(wr)
}

// Parse parses a raw RESP message and returns a command.
func Parse(raw []byte) (*Request, error) {
	complete, args, _, leftover, err := ReadNextCommand(raw, nil)
	if err != nil {
		return nil, err
	}
	if !complete {
		return nil, errIncompleteCommand
	}
	if len(leftover) > 0 {
		return nil, errTooMuchData
	}
	cmd := &Request{Raw: NewBuffer()}
	wr := NewRespond()
	wr.WriteArray(len(args))
	for i := range args {
		v := wr.WriteBulk(args[i])
		cmd.Args = append(cmd.Args, v)
	}
	cmd.Raw.Swap(wr.Buffer)
	wr.Free()
	wr.Close()
	return cmd, nil
}

// Respond allows for writing RESP messages.
type Respond struct {
	*Buffer
}

// NewRespond creates a new RESP writer.
func NewRespond() *Respond {
	return &Respond{Buffer: NewBuffer()}
}

// Data returns the unflushed buffer. This is a copy so changes
// to the resulting []byte will not affect the writer.
func (r *Respond) Data() [][]byte {
	return r.Buffer.Data()
}

// Bytes 返回当前缓冲区内容拼接后的单个 []byte（会发生拷贝）。
func (r *Respond) Bytes() []byte {
	return r.Buffer.Bytes()
}

// ReadFrom 从 rd 中读取下一个完整的 RESP 报文。
// 仅在 Buffer 为空时有效，解析结果填充 Buffer 并维护内部 RESP 结构。
func (r *Respond) ReadFrom(rd *bufio.Reader) (int64, error) {
	if r.Buffer.Len() > 0 {
		return 0, errors.New("ReadFrom: buffer must be empty")
	}
	err := r.decodeStream(rd)
	return int64(r.Buffer.Len()), err
}

// parseLen parses bulk string and array lengths.
func fastParseLen(p BufferView) (int, error) {
	if p.Len() == 0 {
		return 0, errors.New("redis: ERR malformed integer")
	}
	raw := p.Bytes()

	if raw[0] == '-' && len(raw) == 2 && raw[1] == '1' {
		// handle $-1 and $-1 null replies.
		return -1, nil
	}

	var n int
	for _, b := range raw {
		n *= 10
		if b < '0' || b > '9' {
			return -1, errors.New("redis: ERR illegal bytes in length")
		}
		n += int(b - '0')
	}

	return n, nil
}

// parseInt parses an integer reply.
func fastParseInt(p BufferView) (int, error) {
	if p.Len() == 0 {
		return 0, errors.New("redis: ERR malformed integer")
	}

	var negate bool
	var n int64

	raw := p.Bytes()

	if raw[0] == '-' {
		negate = true
		raw = raw[1:]
	}

	for _, b := range raw {
		if b < '0' || b > '9' {
			return 0, errors.New("redis: ERR illegal bytes in length")
		}
		n *= 10
		n += int64(b - '0')
	}

	if negate {
		n = -n
	}
	return int(n), nil
}

func (r *Respond) decodeStream(rd *bufio.Reader) (err error) {
	// 1. 读取前缀
	prefix, err := rd.ReadByte()
	if err != nil {
		return err
	}
	_, _ = r.Buffer.Write([]byte{prefix})

	switch Type(prefix) {
	case String, Error, Integer:
		// 2. 读取到行尾，获取视图
		_, err := r.readUntilCRLF(rd)
		if err != nil {
			return err
		}
		return nil
	case Bulk:
		// 1. 读取长度行视图
		n, err := r.readRespLen(rd)
		if err != nil {
			return err
		}

		if n == -1 { // Null Bulk String "$-1\r\n"
			return nil
		}

		// 2. 读取主体数据 n + 2 字节 (\r\n)
		// 使用适配器流式灌入物理 Buffer
		if err := r.Buffer.ReadFull(rd, n+2); err != nil {
			return err
		}
		return nil
	case Array:
		// 1. 读取数量行视图
		count, err := r.readRespLen(rd)
		if err != nil {
			return err
		}

		if count <= 0 { // *0\r\n 或 *-1\r\n
			return nil
		}

		// 2. 递归读取子元素
		for i := 0; i < count; i++ {
			err := r.decodeStream(rd)
			if err != nil {
				return err
			}
		}
		return nil

	default:
		return fmt.Errorf("invalid resp type: %c", prefix)
	}
}

func (r *Respond) readUntilCRLF(rd *bufio.Reader) (BufferView, error) {
	start := r.Buffer.Len()
	var lastChar byte // 缓存上一个写入的字节，用于跨循环检查 \r\n

	for {
		// 1. 利用 bufio 的 ReadSlice 寻找行尾 \n
		line, err := rd.ReadSlice('\n')
		if err != nil && err != bufio.ErrBufferFull {
			return BufferView{}, err
		}

		nLine := len(line)
		if nLine > 0 {
			// 【核心优化】更新 lastChar。
			// 如果 nLine >= 2，lastChar 更新为 \n 前的那一位。
			// 如果 nLine == 1，lastChar 保持为上一轮循环写入 Buffer 的最后一个字节。
			if nLine >= 2 {
				lastChar = line[nLine-2]
			}

			// 2. 精准物理写入逻辑 (保持原有高效 Copy)
			remLine := line
			for len(remLine) > 0 {
				dest := r.Buffer.Reserve(len(remLine))
				n := copy(dest, remLine)
				r.Buffer.Advance(n)
				remLine = remLine[n:]
			}
		}

		// 3. RESP 协议匹配逻辑
		if err == nil {
			// 此时最后一位必定是 \n。
			// 无论 \r 是在当前 line 中，还是在上一轮循环的末尾，
			// 此时的 lastChar 寄存器里存的一定是 \n 前面的那一位。
			if lastChar == '\r' {
				return r.Buffer.Tail(start), nil
			}

			// 如果不是 \r\n，将 lastChar 更新为 \n，继续寻找
			lastChar = '\n'
		}
	}
}

// readRespLen 在将原始数据存入 Buffer 的同时，实时解析其代表的长度值
func (r *Respond) readRespLen(rd *bufio.Reader) (int, error) {
	var (
		n          int
		isNegative bool
		digitCount int
		lastChar   byte
	)

	for {
		line, err := rd.ReadSlice('\n')
		nLine := len(line)

		if nLine > 0 {
			// 1. 物理写入 Buffer
			remLine := line
			for len(remLine) > 0 {
				dest := r.Buffer.Reserve(len(remLine))
				c := copy(dest, remLine)
				r.Buffer.Advance(c)
				remLine = remLine[c:]
			}

			// 2. 实时解析 (利用本地变量 line 避免多次访问 Buffer)
			for i := 0; i < nLine; i++ {
				b := line[i]
				if b >= '0' && b <= '9' {
					n = n*10 + int(b-'0')
					digitCount++
				} else if b == '-' && digitCount == 0 {
					isNegative = true
					digitCount++
				} else if b == '\n' && lastChar == '\r' {
					// 匹配成功
					if isNegative {
						if n == 1 && digitCount == 2 {
							return -1, nil
						}
						return 0, errors.New("redis: ERR invalid length")
					}
					return n, nil
				}
				lastChar = b
			}
		}

		if err != nil {
			if err == bufio.ErrBufferFull {
				continue
			}
			return 0, err
		}
	}
}

// WriteNull writes a null to the client
func (r *Respond) WriteNull() {
	AppendNull(r.Buffer)
}

// WriteArray writes an array header. You must then write additional
// sub-responses to the client to complete the response.
// For example to write two strings:
//
//	c.WriteArray(2)
//	c.WriteBulkString("item 1")
//	c.WriteBulkString("item 2")
func (r *Respond) WriteArray(count int) {
	if count < 0 {
		AppendNullArray(r.Buffer)
	} else {
		AppendArray(r.Buffer, count)
	}
}

// WriteBulk writes bulk bytes to the client.
func (r *Respond) WriteBulk(bulk []byte) BufferView {
	return AppendBulk(r.Buffer, bulk)
}

// WriteBulkString writes a bulk string to the client.
func (r *Respond) WriteBulkString(bulk string) {
	AppendBulkString(r.Buffer, bulk)

}

// WriteError writes an error to the client.
func (r *Respond) WriteError(msg string) {
	AppendError(r.Buffer, msg)
}

// WriteString writes a string to the client.
func (r *Respond) WriteString(msg string) {
	AppendString(r.Buffer, msg)
}

// WriteInt writes an integer to the client.
func (r *Respond) WriteInt(num int) {
	r.WriteInt64(int64(num))
}

// WriteInt64 writes a 64-bit signed integer to the client.
func (r *Respond) WriteInt64(num int64) {
	AppendInt(r.Buffer, num)
}

// WriteUint64 writes a 64-bit unsigned integer to the client.
func (r *Respond) WriteUint64(num uint64) {
	AppendUint(r.Buffer, num)
}

// WriteRaw writes raw data to the client.
func (r *Respond) WriteRaw(data []byte) {
	if len(data) == 0 {
		return
	}
	_, _ = r.Buffer.Write(data)
}

// WriteAny writes any type to client.
//
//	nil             -> null
//	error           -> error (adds "ERR " when first word is not uppercase)
//	string          -> bulk-string
//	numbers         -> bulk-string
//	[]byte          -> bulk-string
//	bool            -> bulk-string ("0" or "1")
//	slice           -> array
//	map             -> array with key/value pairs
//	SimpleString    -> string
//	SimpleInt       -> integer
//	everything-else -> bulk-string representation using fmt.Sprint()
func (r *Respond) WriteAny(v interface{}) {
	AppendAny(r.Buffer, v)
}

func (r *Respond) Close() {
	r.Buffer.Free()
}

func (r *Respond) Swap(r_ *Respond) { r.Buffer.Swap(r_.Buffer) }

func (r *Respond) Reset() {
	r.Buffer.Free()
	r.Buffer = NewBuffer()
}

func (r *Respond) Type() Type {
	return GetType(r.Buffer)
}

func (r *Respond) TryGetArraySize() (int, error) {
	return GetArrayLength(r.Buffer)
}

func (r *Respond) GetResp() RESP {
	_, resp := ReadNextRESP(r.Buffer.Tail(0))
	return resp
}
