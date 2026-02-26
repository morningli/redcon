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
	r.Args = append(r.Args, v.Data)
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
	resp  RESP
	stack []*RESP // 追踪嵌套 Array 的栈
}

// NewRespond creates a new RESP writer.
func NewRespond() *Respond {
	return &Respond{Buffer: NewBuffer()}
}

// GetRESP 返回当前维护的逻辑结构
func (r *Respond) GetRESP() RESP {
	return r.resp
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

	resp, err := r.decodeStream(rd)
	if err != nil {
		return err
	}

	r.resp = resp
	return nil
}

func (r *Respond) decodeStream(rd io.Reader) (RESP, error) {
	// 1. 读取前缀
	start := r.Buffer.Len()
	prefixBuf := make([]byte, 1)
	if _, err := io.ReadFull(rd, prefixBuf); err != nil {
		return RESP{}, err
	}
	r.Buffer.Write(prefixBuf)

	prefix := Type(prefixBuf[0])
	resp := RESP{Type: prefix}

	switch prefix {
	case String, Error, Integer:
		// 2. 读取到行尾，获取视图
		lineView, err := r.readUntilCRLF(rd)
		if err != nil {
			return RESP{}, err
		}
		// 整体 Raw 包含前缀
		resp.Raw = r.Buffer.Slice(start, r.Buffer.Len())
		// Data 为排除前缀和末尾 \r\n 的视图
		resp.Data = lineView.Slice(0, lineView.Len()-2)
		return resp, nil

	case Bulk:
		// 1. 读取长度行视图
		lenLineView, err := r.readUntilCRLF(rd)
		if err != nil {
			return RESP{}, err
		}

		// 解析长度 (利用 BufferView 的 Bytes() 临时转 string 转换，或直接解析 ASCII)
		n, err := strconv.Atoi(string(lenLineView.Slice(0, lenLineView.Len()-2).Bytes()))
		if err != nil {
			return RESP{}, err
		}
		resp.Count = n

		if n == -1 { // Null Bulk String "$-1\r\n"
			resp.Raw = r.Buffer.Slice(start, r.Buffer.Len())
			return resp, nil
		}

		// 2. 读取主体数据 n + 2 字节 (\r\n)
		dataStart := r.Buffer.Len()
		// 使用适配器流式灌入物理 Buffer
		if _, err := io.CopyN(r.Buffer, rd, int64(n+2)); err != nil {
			return RESP{}, err
		}

		resp.Data = r.Buffer.Slice(dataStart, dataStart+n)
		resp.Raw = r.Buffer.Slice(start, r.Buffer.Len())
		return resp, nil

	case Array:
		// 1. 读取数量行视图
		countLineView, err := r.readUntilCRLF(rd)
		if err != nil {
			return RESP{}, err
		}
		count, err := strconv.Atoi(string(countLineView.Slice(0, countLineView.Len()-2).Bytes()))
		if err != nil {
			return RESP{}, err
		}
		resp.Count = count

		if count <= 0 { // *0\r\n 或 *-1\r\n
			resp.Raw = r.Buffer.Slice(start, r.Buffer.Len())
			return resp, nil
		}

		// 2. 递归读取子元素
		resp.Array = make([]RESP, 0, count)
		for i := 0; i < count; i++ {
			subResp, err := r.decodeStream(rd)
			if err != nil {
				return RESP{}, err
			}
			resp.Array = append(resp.Array, subResp)
		}

		resp.Raw = r.Buffer.Slice(start, r.Buffer.Len())
		return resp, nil

	default:
		return RESP{}, fmt.Errorf("invalid resp type: %c", prefix)
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

// attach 处理逻辑：判断是简单结构还是 Array 成员
func (r *Respond) attach(item RESP) {
	// 1. 如果栈为空，说明当前不是在组建 Array，或者是 Array 的根节点
	if len(r.stack) == 0 {
		r.resp = item
		// 注意：如果是 Array 根节点，后续由 WriteArray 负责入栈
		return
	}

	// 2. 如果栈不为空，说明正在填充某个 Array 的成员
	parent := r.stack[len(r.stack)-1]
	parent.Array = append(parent.Array, item)

	// 3. 检查当前层级是否填满，填满则出栈
	r.checkStack()
}

func (r *Respond) checkStack() {
	for len(r.stack) > 0 {
		curr := r.stack[len(r.stack)-1]
		if len(curr.Array) < curr.Count {
			break // 当前层还没填满，停止向上回溯
		}
		// 当前层填满了，弹出
		r.stack = r.stack[:len(r.stack)-1]
	}
}

// WriteNull writes a null to the client
func (r *Respond) WriteNull() {
	res := AppendNull(r.Buffer)
	r.attach(res)
}

// WriteArray writes an array header. You must then write additional
// sub-responses to the client to complete the response.
// For example to write two strings:
//
//	c.WriteArray(2)
//	c.WriteBulkString("item 1")
//	c.WriteBulkString("item 2")
func (r *Respond) WriteArray(count int) {
	var res RESP
	if count < 0 {
		res = AppendNullArray(r.Buffer)
	} else {
		res = AppendArray(r.Buffer, count)
	}

	// 先按照普通规则挂载（如果是根则设为 root，如果是子 Array 则挂到父 Array 下）
	r.attach(res)

	// 只有当 count > 0 时才需要入栈等待后续成员
	if count > 0 {
		var target *RESP
		if len(r.stack) > 0 {
			// 如果已经在栈里，说明是嵌套 Array，取父 Array 的最后一个元素（即刚刚 attach 进去的那个）
			parent := r.stack[len(r.stack)-1]
			target = &parent.Array[len(parent.Array)-1]
		} else {
			// 否则它就是根节点
			target = &r.resp
		}
		r.stack = append(r.stack, target)
	}
}

// WriteBulk writes bulk bytes to the client.
func (r *Respond) WriteBulk(bulk []byte) *BufferView {
	res := AppendBulk(r.Buffer, bulk)
	r.attach(res)
	return res.Data
}

// WriteBulkString writes a bulk string to the client.
func (r *Respond) WriteBulkString(bulk string) {
	res := AppendBulkString(r.Buffer, bulk)
	r.attach(res)
}

// WriteError writes an error to the client.
func (r *Respond) WriteError(msg string) {
	res := AppendError(r.Buffer, msg)
	r.attach(res)
}

// WriteString writes a string to the client.
func (r *Respond) WriteString(msg string) {
	res := AppendString(r.Buffer, msg)
	r.attach(res)
}

// WriteInt writes an integer to the client.
func (r *Respond) WriteInt(num int) {
	r.WriteInt64(int64(num))
}

// WriteInt64 writes a 64-bit signed integer to the client.
func (r *Respond) WriteInt64(num int64) {
	res := AppendInt(r.Buffer, num)
	r.attach(res)
}

// WriteUint64 writes a 64-bit unsigned integer to the client.
func (r *Respond) WriteUint64(num uint64) {
	res := AppendUint(r.Buffer, num)
	r.attach(res)
}

// WriteRaw writes raw data to the client.
func (r *Respond) WriteRaw(data []byte) {
	if len(data) == 0 {
		return
	}

	// 1. 物理追加：写入内存池，记录区间
	start := r.Buffer.Len()
	_, _ = r.Buffer.Write(data)
	end := r.Buffer.Len()

	// 2. 逻辑扫描：仅针对本次写入的 data 区间生成视图
	dataView := r.Buffer.Slice(start, end)

	currentPos := 0
	for currentPos < dataView.Len() {
		// 调用你的原型函数：从当前位置切分视图进行解析
		// 如果 Type 为 0，代表数据不足或非法
		n, resp := ReadNextRESP(dataView.Slice(currentPos, dataView.Len()))

		if resp.Type == 0 || n <= 0 {
			break
		}

		// 3. 同步挂载到逻辑树（处理 Array 嵌套）
		r.attach(resp)

		currentPos += n
	}
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
