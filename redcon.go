// Package redcon implements a Redis compatible server framework
package redcon

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"strconv"
	"sync"
	"syscall"
	"time"

	"github.com/panjf2000/gnet/v2"
)

var (
	errUnbalancedQuotes       = &errProtocol{"unbalanced quotes in request"}
	errInvalidBulkLength      = &errProtocol{"invalid bulk length"}
	errInvalidMultiBulkLength = &errProtocol{"invalid multibulk length"}
	errIncompleteCommand      = errors.New("incomplete command")
	errTooMuchData            = errors.New("too much data")
)

const maxBufferCap = 262144

type errProtocol struct {
	msg string
}

func (err *errProtocol) Error() string {
	return "Protocol error: " + err.msg
}

// Conn represents a client connection
type Conn interface {
	// LocalAddr returns the local address of the client connection.
	LocalAddr() net.Addr
	// RemoteAddr returns the remote address of the client connection.
	RemoteAddr() net.Addr
	// Close closes the connection.
	Close() error
	// Context returns a user-defined context
	Context() interface{}
	// SetContext sets a user-defined context
	SetContext(v interface{})
	// NetConn returns the base net.Conn connection
	NetConn() gnet.Conn
}

// NewServer returns a new Redcon server configured on "tcp" network net.
func NewServer(addr string,
	handler func(conn Conn, cmd *Request, res *Respond),
	after func(conn Conn, cmd *Request, res *Respond),
	accept func(conn Conn) error,
	closed func(conn Conn, err error),
) *Server {
	return NewServerNetwork("tcp", addr, after, handler, accept, closed)
}

// NewServerNetwork returns a new Redcon server. The network net must be
// a stream-oriented network: "tcp", "tcp4", "tcp6", "unix" or "unixpacket"
func NewServerNetwork(
	net, laddr string,
	handler func(conn Conn, cmd *Request, res *Respond),
	after func(conn Conn, cmd *Request, res *Respond),
	accept func(conn Conn) error,
	closed func(conn Conn, err error),
) *Server {
	if handler == nil {
		panic("handler is nil")
	}
	s := newServer()
	s.net = net
	s.laddr = laddr
	s.handler = handler
	s.accept = accept
	s.closed = closed
	s.after = after
	return s
}

// Close stops listening on the TCP address.
// Already Accepted connections will be closed.
func (s *Server) Close(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.eg == nil {
		return errors.New("not serving")
	}
	s.done = true
	return s.eg.Stop(ctx)
}

// ListenAndServe serves incoming connections.
func (s *Server) ListenAndServe() error {
	return gnet.Run(s, fmt.Sprintf("%s://%s", s.net, s.laddr),
		gnet.WithMulticore(false),
		gnet.WithReusePort(false),
		gnet.WithTCPNoDelay(gnet.TCPNoDelay),
		gnet.WithNumEventLoop(8),
		gnet.WithSocketRecvBuffer(16<<10),
		gnet.WithSocketSendBuffer(16<<10),
	)
}

func newServer() *Server {
	p, err := NewSmartPool(1024, 5000)
	if err != nil {
		panic(err)
	}
	s := &Server{
		conns:   make(map[*conn]bool),
		workers: p,
	}
	return s
}

// ListenAndServe creates a new server and binds to addr configured on "tcp" network net.
func ListenAndServe(addr string,
	handler func(conn Conn, cmd *Request, res *Respond),
	after func(conn Conn, cmd *Request, res *Respond),
	accept func(conn Conn) error,
	closed func(conn Conn, err error),
) error {
	return ListenAndServeNetwork("tcp", addr, handler, after, accept, closed)
}

// ListenAndServeNetwork creates a new server and binds to addr. The network net must be
// a stream-oriented network: "tcp", "tcp4", "tcp6", "unix" or "unixpacket"
func ListenAndServeNetwork(
	net, laddr string,
	handler func(conn Conn, cmd *Request, res *Respond),
	after func(conn Conn, cmd *Request, res *Respond),
	accept func(conn Conn) error,
	closed func(conn Conn, err error),
) error {
	return NewServerNetwork(net, laddr, handler, after, accept, closed).ListenAndServe()
}

// conn represents a client connection
type conn struct {
	conn      gnet.Conn
	rd        *Reader
	addr      string
	ctx       interface{}
	needClose bool
	closed    bool
	idleClose time.Duration
}

func (c *conn) Close() error {
	c.needClose = true
	return nil
}
func (c *conn) close() error {
	c.rd.Close()
	c.closed = true
	return c.conn.Close()
}
func (c *conn) Context() interface{}     { return c.ctx }
func (c *conn) SetContext(v interface{}) { c.ctx = v }
func (c *conn) NetConn() gnet.Conn {
	return c.conn
}
func (c *conn) LocalAddr() net.Addr  { return c.conn.LocalAddr() }
func (c *conn) RemoteAddr() net.Addr { return c.conn.RemoteAddr() }

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

func NewRequest() *Request                  { return &Request{Raw: NewBuffer()} }
func (r *Request) Free()                    { r.Raw.Free() }
func (r *Request) Context() interface{}     { return r.ctx }
func (r *Request) SetContext(v interface{}) { r.ctx = v }
func (r *Request) WriteArray(count int)     { AppendArray(r.Raw, count) }
func (r *Request) WriteBulk(bulk []byte) {
	v := AppendBulk(r.Raw, bulk)
	r.Args = append(r.Args, v.Data)
}
func (r *Request) WriteRaw(data []byte) { _, _ = r.Raw.Write(data) }

// Server defines a server for clients for managing client connections.
type Server struct {
	mu        sync.Mutex
	net       string
	laddr     string
	handler   func(conn Conn, cmd *Request, res *Respond)
	after     func(conn Conn, cmd *Request, res *Respond)
	accept    func(conn Conn) error
	closed    func(conn Conn, err error)
	conns     map[*conn]bool
	eg        *gnet.Engine
	done      bool
	idleClose time.Duration
	workers   *SmartPool

	// AcceptError is an optional function used to handle Accept errors.
	AcceptError func(err error)
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
	res := AppendArray(r.Buffer, count)

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

	// 1. 物理追加：寫入內存池，記錄區間
	start := r.Buffer.Len()
	_, _ = r.Buffer.Write(data)
	end := r.Buffer.Len()

	// 2. 邏輯掃描：僅針對本次寫入的 data 區間生成視圖
	dataView := r.Buffer.Slice(start, end)

	currentPos := 0
	for currentPos < dataView.Len() {
		// 調用你的原型函數：從當前位置切分視圖進行解析
		// 如果 Type 為 0，代表數據不足或非法
		n, resp := ReadNextRESP(dataView.Slice(currentPos, dataView.Len()))

		if resp.Type == 0 || n <= 0 {
			break
		}

		// 3. 同步掛載到邏輯樹（處理 Array 嵌套）
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

// Reader represent a reader for RESP or telnet commands.
type Reader struct {
	rd   *bufio.Reader
	buf  *Buffer
	cmds []*Request
}

// NewReader returns a command reader which will read RESP or telnet commands.
func NewReader(rd io.Reader) *Reader {
	return &Reader{
		rd:  bufio.NewReader(rd),
		buf: NewBuffer(),
	}
}

func parseInt(b []byte) (int, bool) {
	if len(b) == 1 && b[0] >= '0' && b[0] <= '9' {
		return int(b[0] - '0'), true
	}
	var n int
	var sign bool
	var i int
	if len(b) > 0 && b[0] == '-' {
		sign = true
		i++
	}
	for ; i < len(b); i++ {
		if b[i] < '0' || b[i] > '9' {
			return 0, false
		}
		n = n*10 + int(b[i]-'0')
	}
	if sign {
		n *= -1
	}
	return n, true
}

func (rd *Reader) readCommands() ([]*Request, error) {
	var cmds []*Request

	b := rd.buf
	if b.Len() > 0 {
		// we have data, yay!
		// but is this enough data for a complete command? or multiple?
	next:

		switch b.At(0) {
		default:
			// just a plain text command
			for i := 0; i < b.Len(); i++ {
				if b.At(i) == '\n' {
					var line []byte
					if i > 0 && b.At(i-1) == '\r' {
						line = b.Slice(0, i-1).Bytes()
					} else {
						line = b.Slice(0, i).Bytes()
					}
					var args [][]byte
					var quote bool
					var quotech byte
					var escape bool
				outer:
					for {
						nline := make([]byte, 0, len(line))
						for i := 0; i < len(line); i++ {
							c := line[i]
							if !quote {
								if c == ' ' {
									if len(nline) > 0 {
										args = append(args, nline)
									}
									line = line[i+1:]
									continue outer
								}
								if c == '"' || c == '\'' {
									if i != 0 {
										return nil, errUnbalancedQuotes
									}
									quotech = c
									quote = true
									line = line[i+1:]
									continue outer
								}
							} else {
								if escape {
									escape = false
									switch c {
									case 'n':
										c = '\n'
									case 'r':
										c = '\r'
									case 't':
										c = '\t'
									}
								} else if c == quotech {
									quote = false
									quotech = 0
									args = append(args, nline)
									line = line[i+1:]
									if len(line) > 0 && line[0] != ' ' {
										return nil, errUnbalancedQuotes
									}
									continue outer
								} else if c == '\\' {
									escape = true
									continue
								}
							}
							nline = append(nline, c)
						}
						if quote {
							return nil, errUnbalancedQuotes
						}
						if len(line) > 0 {
							args = append(args, line)
						}
						break
					}
					if len(args) > 0 {
						var cmd = &Request{Raw: NewBuffer()}
						// convert this to resp command syntax
						var wr = NewRespond()
						wr.WriteArray(len(args))
						for i := range args {
							v := wr.WriteBulk(args[i])
							cmd.Args = append(cmd.Args, v)
						}
						cmd.Raw.Swap(wr.Buffer)
						cmds = append(cmds, cmd)
						wr.Close()
					}
					b_ := b.Split(i + 1)
					b.Swap(b_)
					b_.Free()

					if b.Len() > 0 {
						goto next
					} else {
						goto done
					}
				}
			}
		case '*':
			// resp formatted command
			marks := make([]int, 0, 16)
		outer2:
			for i := 1; i < b.Len(); i++ {
				if b.At(i) == '\n' {
					if b.At(i-1) != '\r' {
						return nil, errInvalidMultiBulkLength
					}
					count, ok := parseInt(b.Slice(1, i-1).Bytes())
					if !ok || count <= 0 {
						return nil, errInvalidMultiBulkLength
					}
					marks = marks[:0]
					for j := 0; j < count; j++ {
						// read bulk length
						i++
						if i < b.Len() {
							if b.At(i) != '$' {
								return nil, &errProtocol{"expected '$', got '" + string(b.At(i)) + "'"}
							}
							si := i
							for ; i < b.Len(); i++ {
								if b.At(i) == '\n' {
									if b.At(i-1) != '\r' {
										return nil, errInvalidBulkLength
									}
									size, ok := parseInt(b.Slice(si+1, i-1).Bytes())
									if !ok || size < 0 {
										return nil, errInvalidBulkLength
									}
									if i+size+2 >= b.Len() {
										// not ready
										break outer2
									}
									if b.At(i+size+2) != '\n' ||
										b.At(i+size+1) != '\r' {
										return nil, errInvalidBulkLength
									}
									i++
									marks = append(marks, i, i+size)
									i += size + 1
									break
								}
							}
						}
					}
					if len(marks) == count*2 {
						var cmd Request
						// just assign the slice
						b_ := b.Split(i + 1)
						b.Swap(b_)
						cmd.Raw = b_
						cmd.Args = make([]*BufferView, len(marks)/2)
						// slice up the raw command into the args based on
						// the recorded marks.
						for h := 0; h < len(marks); h += 2 {
							cmd.Args[h/2] = cmd.Raw.Slice(marks[h], marks[h+1])
						}
						cmds = append(cmds, &cmd)
						if b.Len() > 0 {
							goto next
						} else {
							goto done
						}
					}
				}
			}
		}
	done:
	}
	if len(cmds) > 0 {
		return cmds, nil
	}
	if rd.rd == nil {
		return nil, errIncompleteCommand
	}
	var newData = GetBuffer()
	n, err := rd.rd.Read(newData[:])
	if err != nil {
		return nil, err
	}
	_, _ = rd.buf.Write(newData[:n])
	PutBuffer(newData)
	return rd.readCommands()
}

// ReadCommands reads the next pipeline commands.
func (rd *Reader) ReadCommands() ([]*Request, error) {
	for {
		if len(rd.cmds) > 0 {
			cmds := rd.cmds
			rd.cmds = nil
			return cmds, nil
		}
		cmds, err := rd.readCommands()
		if err != nil {
			return []*Request{}, err
		}
		rd.cmds = cmds
	}
}

// ReadCommand reads the next command.
func (rd *Reader) ReadCommand() (*Request, error) {
	if len(rd.cmds) > 0 {
		cmd := rd.cmds[0]
		rd.cmds = rd.cmds[1:]
		return cmd, nil
	}
	cmds, err := rd.readCommands()
	if err != nil {
		return nil, err
	}
	rd.cmds = cmds
	return rd.ReadCommand()
}

func (rd *Reader) Close() {
	rd.buf.Free()
}

// Parse parses a raw RESP message and returns a command.
func Parse(raw []byte) (*Request, error) {
	buff := NewBuffer()
	_, _ = buff.Write(raw)
	rd := Reader{buf: buff}
	cmds, err := rd.readCommands()
	if err != nil {
		return nil, err
	}
	if rd.buf.Len() > 0 {
		return nil, errTooMuchData
	}
	return cmds[0], nil
}

// A Handler responds to an RESP request.
type Handler interface {
	ServeRESP(conn Conn, cmd Request)
}

// The HandlerFunc type is an adapter to allow the use of
// ordinary functions as RESP handlers. If f is a function
// with the appropriate signature, HandlerFunc(f) is a
// Handler that calls f.
type HandlerFunc func(conn Conn, cmd Request)

// ServeRESP calls f(w, r)
func (f HandlerFunc) ServeRESP(conn Conn, cmd Request) {
	f(conn, cmd)
}

// ServeMux is an RESP command multiplexer.
type ServeMux struct {
	handlers map[string]Handler
}

// SetIdleClose will automatically close idle connections after the specified
// duration. Use zero to disable this feature.
func (s *Server) SetIdleClose(dur time.Duration) {
	s.mu.Lock()
	s.idleClose = dur
	s.mu.Unlock()
}

func (s *Server) OnBoot(eng gnet.Engine) (action gnet.Action) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.eg = &eng
	return
}

func (s *Server) OnShutdown(eng gnet.Engine) {
	return
}

func (s *Server) OnOpen(c gnet.Conn) (out []byte, action gnet.Action) {
	c_ := &conn{
		conn: c,
		addr: c.RemoteAddr().String(),
		rd:   NewReader(c),
	}
	s.mu.Lock()
	c_.idleClose = s.idleClose
	s.conns[c_] = true
	s.mu.Unlock()

	if s.accept != nil {
		err := s.accept(c_)
		if err != nil {
			s.mu.Lock()
			delete(s.conns, c_)
			s.mu.Unlock()
			return []byte(err.Error()), gnet.Close
		}
	}
	c.SetContext(c_)
	return
}

func (s *Server) OnClose(c gnet.Conn, err error) (action gnet.Action) {
	c_ := c.Context().(*conn)
	if s.closed != nil {
		s.closed(c_, err)
	}
	return
}

var (
	ErrQueueOverflow = []byte("-ERR queue overflow")
	ErrNoHandler     = []byte("-ERR no handler")
)

func (s *Server) OnTraffic(c gnet.Conn) (action gnet.Action) {
	c_ := c.Context().(*conn)

	rec := time.Now()
	cmds, err := c_.rd.readCommands()
	if errors.Is(err, syscall.EAGAIN) || errors.Is(err, syscall.EWOULDBLOCK) {
		// 这不是真正的错误，仅仅表示“当前缓冲区已空，请等待下次触发”
		return
	}

	_ = s.workers.Submit(c.RemoteAddr().String(), func(drop int) {
		for i := 0; i < drop; i++ {
			err := c.AsyncWrite(ErrQueueOverflow, nil)
			if err != nil {
				_ = c_.close()
				return
			}
		}

		if err != nil {
			if err, ok := err.(*errProtocol); ok {
				// All protocol errors should attempt a response to
				// the client. Ignore write errors.
				_ = c.AsyncWrite([]byte("-ERR "+err.Error()), nil)
			}
			_ = c_.close()
			return
		}

		var callback gnet.AsyncCallback = func(c gnet.Conn, err error) error {
			if err != nil {
				_ = c_.close()
			}
			return nil
		}

		for _, cmd := range cmds {
			var res = NewRespond()
			cmd.ReceiveTime = rec
			cmd.ProcessTime = time.Now()
			if s.handler != nil {
				s.handler(c_, cmd, res)
			}
			cmd.ProcessDoneTime = time.Now()
			if s.handler != nil {
				err = c.AsyncWritev(res.Data(), callback)
			} else {
				err = c.AsyncWrite(ErrNoHandler, callback)
			}
			cmd.FlushTime = time.Now()
			if s.after != nil {
				s.after(c_, cmd, res)
			}
			cmd.Free()
			res.Close()
			if err != nil {
				_ = c_.close()
				return
			}
		}
		if c_.needClose {
			_ = c_.close()
		}
	})
	return
}

func (s *Server) OnTick() (delay time.Duration, action gnet.Action) {
	return
}
