package redcon

import (
	"errors"
	"fmt"
	"io"
	"strconv"
	"time"
)

// Request represent a command
type Request struct {
	ctx interface{}
	// Raw is a encoded RESP message.
	Raw *Buffer
	// Args is a series of arguments that make up the command.
	Args []*BufferView

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

// ReadRESP 从 rd 中读取下一个完整的 RESP 报文。
// 仅在 Buffer 为空时有效，解析结果填充 Buffer 并维护内部 RESP 结构。
func (r *Respond) ReadRESP(rd io.Reader) error {
	if r.Buffer.Len() > 0 {
		return errors.New("ReadRESP: buffer must be empty")
	}

	err := r.decodeStream(rd)
	if err != nil {
		return err
	}
	return nil
}

func (r *Respond) decodeStream(rd io.Reader) error {
	// 1. 读取前缀
	prefixBuf := make([]byte, 1)
	if _, err := io.ReadFull(rd, prefixBuf); err != nil {
		return err
	}
	r.Buffer.Write(prefixBuf)

	prefix := Type(prefixBuf[0])
	switch prefix {
	case String, Error, Integer:
		// 2. 读取到行尾，获取视图
		_, err := r.readUntilCRLF(rd)
		if err != nil {
			return err
		}
		return nil
	case Bulk:
		// 1. 读取长度行视图
		lenLineView, err := r.readUntilCRLF(rd)
		if err != nil {
			return err
		}

		// 解析长度 (利用 BufferView 的 Bytes() 临时转 string 转换，或直接解析 ASCII)
		n, err := strconv.Atoi(string(lenLineView.Slice(0, lenLineView.Len()-2).Bytes()))
		if err != nil {
			return err
		}

		if n == -1 { // Null Bulk String "$-1\r\n"
			return nil
		}

		// 2. 读取主体数据 n + 2 字节 (\r\n)
		// 使用适配器流式灌入物理 Buffer
		if _, err := io.CopyN(r.Buffer, rd, int64(n+2)); err != nil {
			return err
		}
		return nil
	case Array:
		// 1. 读取数量行视图
		countLineView, err := r.readUntilCRLF(rd)
		if err != nil {
			return err
		}
		count, err := strconv.Atoi(string(countLineView.Slice(0, countLineView.Len()-2).Bytes()))
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

// readUntilCRLF 从 rd 读取数据直到 \r\n，同步写入物理 Buffer，并返回这一行的视图。
func (r *Respond) readUntilCRLF(rd io.Reader) (*BufferView, error) {
	start := r.Buffer.Len()
	var lastByte byte
	currByte := make([]byte, 1)

	for {
		_, err := io.ReadFull(rd, currByte)
		if err != nil {
			return nil, err
		}

		// 写入物理 Buffer (内存池)
		r.Buffer.Write(currByte)

		if lastByte == '\r' && currByte[0] == '\n' {
			break
		}
		lastByte = currByte[0]
	}

	// 返回这一行的视图 (包含 \r\n)
	return r.Buffer.Slice(start, r.Buffer.Len()), nil
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
func (r *Respond) WriteBulk(bulk []byte) *BufferView {
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
