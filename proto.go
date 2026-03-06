package redcon

import (
	"bufio"
	"io"
	"time"
)

// Request represent a command
type Request struct {
	ctx interface{}
	// Raw is a encoded RESP message.
	Raw *Buffer
	wr  *BufferWriter
	// Args is a series of arguments that make up the command.
	Args []BufferView

	ReceiveTime     time.Time
	ProcessTime     time.Time
	ProcessDoneTime time.Time
	FlushTime       time.Time
}

// NewRequest 创建一个空 Request，并初始化 Raw 缓冲区。
func NewRequest() *Request {
	b := NewBuffer()
	return &Request{Raw: b, wr: b.NewWriter()}
}

// Free 释放 Request 持有的 Raw 缓冲区。
func (r *Request) Free() { r.Raw.Free() }

// Context 返回与该请求关联的用户上下文。
func (r *Request) Context() interface{} { return r.ctx }

// SetContext 设置与该请求关联的用户上下文。
func (r *Request) SetContext(v interface{}) { r.ctx = v }

// WriteArray 向 Raw 追加一个 Array 头（元素数量为 count）。
func (r *Request) WriteArray(count int) {
	if count < 0 {
		AppendNullArray(r.wr)
		return
	}
	AppendArray(r.wr, count)
}

// WriteBulk 向 Raw 追加一个 Bulk，并把其 Data 视图追加到 Args。
func (r *Request) WriteBulk(bulk []byte) {
	AppendBulk(r.wr, bulk)
	end := r.Raw.Len()
	r.Args = append(r.Args, r.Raw.Slice(end-len(bulk)-2, end-2))
}

// WriteRaw 直接向 Raw 追加字节（不做解析）。
func (r *Request) WriteRaw(data []byte) { _, _ = r.wr.Write(data) }

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
	cmd := NewRequest()
	cmd.WriteArray(len(args))
	for i := range args {
		cmd.WriteBulk(args[i])
	}
	return cmd, nil
}

// Respond allows for writing RESP messages.
type Respond struct {
	*Buffer
	wr *BufferWriter
}

// NewRespond creates a new RESP writer.
func NewRespond() *Respond {
	b := NewBuffer()
	return &Respond{Buffer: b, wr: b.NewWriter()}
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
	err := r.decodeStream(rd)
	return int64(r.Buffer.Len()), err
}

func (r *Respond) decodeStream(rd *bufio.Reader) (err error) {
	wr := r.NewWriter()
	// 1. 读取前缀
	prefix, err := rd.ReadByte()
	if err != nil {
		return err
	}
	wr.WriteByte(prefix)

	switch Type(prefix) {
	case String, Error, Integer:
		// 2. 读取到行尾，获取视图
		_, err := r.CopyLenTo(rd, wr)
		if err != nil {
			return err
		}
		return nil
	case Bulk:
		// 1. 读取长度行视图
		n, err := r.CopyLenTo(rd, wr)
		if err != nil {
			return err
		}

		if n == -1 { // Null Bulk String "$-1\r\n"
			return nil
		}

		// 2. 读取主体数据 n + 2 字节 (\r\n)
		// 使用适配器流式灌入物理 Buffer
		if err := wr.CopyN(rd, n+2); err != nil {
			return err
		}
		return nil
	case Array:
		// 1. 读取数量行视图
		count, err := r.CopyLenTo(rd, wr)
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
		return ErrInvalidRespType
	}
}

// CopyLineTo 从 rd 读取一行（直到 \r\n）并直接拷贝到写入器 w 中。
// 返回该行在缓冲区中的视图。
func (r *Respond) CopyLineTo(rd *bufio.Reader, w *BufferWriter) (BufferView, error) {
	// 记录起始逻辑位置，用于生成视图
	start := r.Len()

	// 从 bufio 的内部缓存查找换行符 \n
	line, err := rd.ReadSlice('\n')

	// 快速路径：99% 的 Redis 简单类型（Status/Error/Int）都在 bufio 缓存的一行内
	if err == nil && len(line) >= 2 && line[len(line)-2] == '\r' {
		// 直接通过 Writer 写入，内部已优化物理地址计算，避免了 b.Write 的全量寻址
		w.Write(line)
		return r.Tail(start), nil
	}

	// 慢速路径：处理跨缓存块、非 CRLF 结尾等复杂情况
	return r.copyLineToSlow(rd, w, start, line, err)
}

//go:noinline
func (r *Respond) copyLineToSlow(rd *bufio.Reader, w *BufferWriter, start int, line []byte, err error) (BufferView, error) {
	for {
		if len(line) > 0 {
			w.Write(line)
		}

		if err == nil {
			// 检查当前 Buffer 末尾是否已经构成 \r\n
			// 由于 \n 刚被写入，只需检查它前一个字节是否为 \r
			curLen := r.Len()
			if curLen >= 2 && r.At(curLen-2) == '\r' {
				return r.Tail(start), nil
			}
		} else if err != bufio.ErrBufferFull {
			// 遇到非缓冲区满的错误（如连接断开），直接返回
			return BufferView{}, err
		}

		// 缓冲区满或未读到 \n，继续读取下一段
		line, err = rd.ReadSlice('\n')
	}
}

// CopyLenTo 从 rd 读取 RESP 长度行（如 :10\r\n 或 $5\r\n），
// 在拷贝到 w 的同时解析并返回其代表的整数值。
func (r *Respond) CopyLenTo(rd *bufio.Reader, w *BufferWriter) (int, error) {
	// 从 bufio 读取到行尾
	line, err := rd.ReadSlice('\n')

	// Fast Path: 针对 RESP 最常见的短正整数（如 $5\r\n, :1024\r\n）
	// 这里的 line 已经包含了数字和末尾的 \r\n
	if err == nil && len(line) >= 3 && line[len(line)-2] == '\r' {
		// 极致内联优化：如果首位是数字，尝试快速解析
		if line[0] >= '0' && line[0] <= '9' {
			val := 0
			isNum := true
			// 解析数字部分（跳过末尾的 \r\n）
			for i := 0; i < len(line)-2; i++ {
				c := line[i]
				if c >= '0' && c <= '9' {
					val = val*10 + int(c-'0')
				} else {
					isNum = false
					break
				}
			}
			if isNum {
				// 使用优化的 Writer 顺序写入物理 Buffer
				w.Write(line)
				return val, nil
			}
		}
	}
	// Slow Path: 处理负数 ($-1), 跨缓存块读取, 或极端长数字
	return r.copyLenToSlow(rd, w, line, err)
}

//go:noinline
func (r *Respond) copyLenToSlow(rd *bufio.Reader, w *BufferWriter, line []byte, err error) (int, error) {
	var (
		n          int
		digitCount int
		isNegative bool
		lastChar   byte
	)

	for {
		if len(line) > 0 {
			// 每一段读到的数据都实时存入物理 Buffer，保持指针自增
			w.Write(line)

			for i := 0; i < len(line); i++ {
				char := line[i]
				if char >= '0' && char <= '9' {
					n = n*10 + int(char-'0')
					digitCount++
				} else if char == '-' && digitCount == 0 {
					isNegative = true
					digitCount++
				} else if char == '\n' && lastChar == '\r' {
					if isNegative {
						// 专门处理 RESP 的空值语义：$-1\r\n 或 *-1\r\n
						if n == 1 && digitCount == 2 {
							return -1, nil
						}
						return 0, ErrInvalidLength
					}
					return n, nil
				}
				lastChar = char
			}
		}

		if err != nil {
			if err == bufio.ErrBufferFull {
				line, err = rd.ReadSlice('\n')
				continue
			}
			return 0, err
		}
		line, err = rd.ReadSlice('\n')
	}
}

// WriteNull writes a null to the client
func (r *Respond) WriteNull() {
	AppendNull(r.wr)
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
		AppendNullArray(r.wr)
	} else {
		AppendArray(r.wr, count)
	}
}

// WriteBulk writes bulk bytes to the client.
func (r *Respond) WriteBulk(bulk []byte) {
	AppendBulk(r.wr, bulk)
}

// WriteBulkString writes a bulk string to the client.
func (r *Respond) WriteBulkString(bulk string) {
	AppendBulkString(r.wr, bulk)

}

// WriteError writes an error to the client.
func (r *Respond) WriteError(msg string) {
	AppendError(r.wr, msg)
}

// WriteString writes a string to the client.
func (r *Respond) WriteString(msg string) {
	AppendString(r.wr, msg)
}

// WriteInt writes an integer to the client.
func (r *Respond) WriteInt(num int) {
	r.WriteInt64(int64(num))
}

// WriteInt64 writes a 64-bit signed integer to the client.
func (r *Respond) WriteInt64(num int64) {
	AppendInt(r.wr, num)
}

// WriteUint64 writes a 64-bit unsigned integer to the client.
func (r *Respond) WriteUint64(num uint64) {
	AppendUint(r.wr, num)
}

// WriteRaw writes raw data to the client.
func (r *Respond) WriteRaw(data []byte) {
	if len(data) == 0 {
		return
	}
	_, _ = r.wr.Write(data)
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
	AppendAny(r.wr, v)
}

func (r *Respond) Free() {
	r.Buffer.Free()
}

func (r *Respond) Swap(r_ *Respond) {
	r.Buffer.Swap(r_.Buffer)
	r.wr.Sync()
}

func (r *Respond) Reset() {
	r.Buffer.Free()
	r.Buffer = NewBuffer()
	r.wr = r.Buffer.NewWriter()
}

func (r *Respond) Type() Type {
	if r.Len() == 0 {
		return Type(0)
	}
	return Type(r.At(0))
}

func (r *Respond) TryGetArraySize() (int, error) {
	if r.Type() != Array {
		return 1, nil
	}
	for i := 1; i < r.Len(); i++ {
		if r.At(i) == '\n' && r.At(i-1) == '\r' {
			return r.Slice(1, i-1).ParseInt()
		}
	}
	return 0, ErrUnexpectedEOF
}

func (r *Respond) GetResp() RESP {
	_, resp := ReadNextRESP(r.Buffer.Tail(0))
	return resp
}
