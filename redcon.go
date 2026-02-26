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
	"sync/atomic"
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

const defaultDrainTimeout = 30 * time.Second

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
	return NewServerNetwork("tcp", addr, handler, after, accept, closed)
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
	fmt.Println("Close")

	s.mu.Lock()
	eg := s.eg
	if eg == nil {
		s.mu.Unlock()
		return errors.New("not serving")
	}
	s.done = true
	s.mu.Unlock()
	return eg.Stop(ctx)
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
	p, err := NewGPerC(128)
	if err != nil {
		panic(err)
	}
	s := &Server{
		conns:   make(map[*conn]bool),
		workers: p,
	}
	// Initialize atomic.Value before any Load() to avoid panics.
	s.drainHandler.Store(drainHandlerHolder{})
	return s
}

// Draining reports whether the server is in graceful-drain mode.
// When draining, the server rejects new connections and existing connections are expected
// to finish (or redirect) in-flight requests and then close.
func (s *Server) Draining() bool {
	return atomic.LoadUint32(&s.draining) == 1
}

func (s *Server) setDrainHandler(fn func(conn Conn, cmd *Request, res *Respond)) {
	s.drainHandler.Store(drainHandlerHolder{fn: fn})
}

func (s *Server) getDrainHandler() func(conn Conn, cmd *Request, res *Respond) {
	return s.drainHandler.Load().(drainHandlerHolder).fn
}

// ShutdownGracefully puts the server into graceful-drain mode:
// - sets a draining flag so OnTraffic can reply MOVED/redirect via the provided handler
// - periodically checks whether there are remaining connections, or the deadline has passed
// - calls eg.Stop() when drained or timed out
func (s *Server) ShutdownGracefully(ctx context.Context, handler func(conn Conn, cmd *Request, res *Respond)) error {
	fmt.Println("ShutdownGracefully")

	// handler can be nil: when nil, OnTraffic will keep using the normal server handler.
	s.setDrainHandler(handler)

	atomic.StoreUint32(&s.draining, 1)

	// Must have a deadline (avoid draining forever).
	until := time.Now().Add(defaultDrainTimeout)
	if dl, ok := ctx.Deadline(); ok {
		until = dl
	}
	atomic.StoreInt64(&s.drainUntil, until.UnixNano())

	s.mu.Lock()
	eg := s.eg
	s.mu.Unlock()

	// If we're not serving yet, just set the flags; OnTraffic/OnTick logic can still be unit-tested.
	if eg == nil {
		return nil
	}

	// Start a single background monitor that stops the engine after drained/timeout.
	s.shutdownOnce.Do(func() {
		go func() {
			tk := time.NewTicker(200 * time.Millisecond)
			defer tk.Stop()
			for {
				if time.Now().After(until) {
					goto STOP
				}

				s.mu.Lock()
				n := len(s.conns)
				s.mu.Unlock()
				if n == 0 {
					goto STOP
				}

				<-tk.C
			}

		STOP:
			stopCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			_ = s.Close(stopCtx)
		}()
	})

	return nil
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
	id        string
	ctx       interface{}
	needClose bool
	closed    bool
	pending   int32 // atomic: number of in-flight tasks that will write a response
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

// Server defines a server for clients for managing client connections.
type Server struct {
	mu           sync.Mutex
	net          string
	laddr        string
	handler      func(conn Conn, cmd *Request, res *Respond)
	after        func(conn Conn, cmd *Request, res *Respond)
	accept       func(conn Conn) error
	closed       func(conn Conn, err error)
	conns        map[*conn]bool
	eg           *gnet.Engine
	done         bool
	draining     uint32       // atomic: 1 means server is draining (no new conns, existing conns will close-after-flush)
	drainHandler atomic.Value // stores drainHandlerHolder
	drainUntil   int64        // atomic unix nano timestamp. 0 means no deadline.
	workers      *GPerC
	shutdownOnce sync.Once

	// AcceptError is an optional function used to handle Accept errors.
	AcceptError func(err error)
}

type drainHandlerHolder struct {
	fn func(conn Conn, cmd *Request, res *Respond)
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

// Reader represent a reader for RESP or telnet commands.
type Reader struct {
	rd  *bufio.Reader
	buf *Buffer
	// argsbuf 为 ReadNextCommand 复用的临时参数缓冲区。
	argsbuf [][]byte
	cmds    []*Request

	// pendingErr holds a delayed error. If we encounter an error after already
	// having parsed one or more commands, we return the commands first and
	// surface the error on the next read when no commands are produced.
	pendingErr error

	// 以下字段用于在 buf 中增量解析（半包/粘包）时保存中间状态。
	mode      byte // 0=unknown, '*'=RESP, 't'=telnet/plain
	scan      int  // scan cursor for finding '\n'
	respCount int
	respArg   int
	respPos   int // current parsing cursor within buf
	respStage int // 0: reading count line, 1: reading bulk len line, 2: waiting bulk body
	bulkSize  int
	bulkStart int
	marks     []int // bulk data marks (start,end pairs) within current command
}

func (rd *Reader) resetReadState() {
	rd.mode = 0
	rd.scan = 0
	rd.respCount = 0
	rd.respArg = 0
	rd.respPos = 0
	rd.respStage = 0
	rd.bulkSize = 0
	rd.bulkStart = 0
	rd.marks = rd.marks[:0]
}

func (rd *Reader) findLF(start int) (idx int, ok bool) {
	b := rd.buf
	for i := start; i < b.Len(); i++ {
		if b.At(i) == '\n' {
			return i, true
		}
	}
	return 0, false
}

// readOneCommand attempts to parse exactly one command from rd.buf.
//
// It returns:
// - cmd: a parsed command (non-nil) when successful
// - needMore: true when more bytes are required to continue parsing (half-packet)
// - err: protocol error when encountered
func (rd *Reader) readOneCommand() (cmd *Request, needMore bool, err error) {
	b := rd.buf
	if b.Len() == 0 {
		rd.resetReadState()
		return nil, true, nil
	}
	if rd.mode == 0 {
		if b.At(0) == '*' {
			rd.mode = '*'
			rd.scan = 1
			rd.respPos = 0
			rd.respCount = 0
			rd.respArg = 0
			rd.marks = rd.marks[:0]
		} else {
			rd.mode = 't'
			rd.scan = 0
		}
	}

	switch rd.mode {
	case 't':
		// plain text command: wait for one full line.
		i, ok := rd.findLF(rd.scan)
		if !ok {
			rd.scan = b.Len()
			return nil, true, nil
		}
		rd.scan = i + 1

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
			for j := 0; j < len(line); j++ {
				c := line[j]
				if !quote {
					if c == ' ' {
						if len(nline) > 0 {
							args = append(args, nline)
						}
						line = line[j+1:]
						continue outer
					}
					if c == '"' || c == '\'' {
						if j != 0 {
							return nil, false, errUnbalancedQuotes
						}
						quotech = c
						quote = true
						line = line[j+1:]
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
						line = line[j+1:]
						if len(line) > 0 && line[0] != ' ' {
							return nil, false, errUnbalancedQuotes
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
				return nil, false, errUnbalancedQuotes
			}
			if len(line) > 0 {
				args = append(args, line)
			}
			break
		}

		if len(args) == 0 {
			// discard empty line and continue
			b_ := b.Split(i + 1)
			b.Swap(b_)
			b_.Free()
			rd.resetReadState()
			return nil, false, nil
		}

		cmd = &Request{Raw: NewBuffer()}
		wr := NewRespond()
		wr.WriteArray(len(args))
		for k := range args {
			v := wr.WriteBulk(args[k])
			cmd.Args = append(cmd.Args, v)
		}
		cmd.Raw.Swap(wr.Buffer)
		wr.Close()

		// consume the line bytes from buffer
		b_ := b.Split(i + 1)
		b.Swap(b_)
		b_.Free()
		rd.resetReadState()
		return cmd, false, nil

	case '*':
		// RESP formatted command: incremental state machine.
		if rd.respStage == 0 {
			// parse multibulk length line: *<count>\r\n
			i, ok := rd.findLF(rd.scan)
			if !ok {
				rd.scan = b.Len()
				return nil, true, nil
			}
			if i == 0 || b.At(i-1) != '\r' {
				return nil, false, errInvalidMultiBulkLength
			}
			count, ok := parseInt(b.Slice(1, i-1).Bytes())
			if !ok || count <= 0 {
				return nil, false, errInvalidMultiBulkLength
			}
			rd.respCount = count
			rd.respArg = 0
			rd.marks = rd.marks[:0]
			rd.respPos = i + 1
			rd.scan = rd.respPos + 1
			rd.respStage = 1
		}

		for rd.respArg < rd.respCount {
			switch rd.respStage {
			case 1:
				// parse bulk len line: $<size>\r\n
				if rd.respPos >= b.Len() {
					rd.scan = b.Len()
					return nil, true, nil
				}
				if b.At(rd.respPos) != '$' {
					return nil, false, &errProtocol{"expected '$', got '" + string(b.At(rd.respPos)) + "'"}
				}
				lenStart := rd.respPos + 1
				if rd.scan < lenStart {
					rd.scan = lenStart
				}
				i, ok := rd.findLF(rd.scan)
				if !ok {
					rd.scan = b.Len()
					return nil, true, nil
				}
				if i == 0 || b.At(i-1) != '\r' {
					return nil, false, errInvalidBulkLength
				}
				size, ok := parseInt(b.Slice(lenStart, i-1).Bytes())
				if !ok || size < 0 {
					return nil, false, errInvalidBulkLength
				}
				rd.bulkSize = size
				rd.bulkStart = i + 1
				rd.respStage = 2
			case 2:
				// waiting bulk body: <data>\r\n
				if b.Len() < rd.bulkStart+rd.bulkSize+2 {
					return nil, true, nil
				}
				if b.At(rd.bulkStart+rd.bulkSize) != '\r' || b.At(rd.bulkStart+rd.bulkSize+1) != '\n' {
					return nil, false, errInvalidBulkLength
				}
				rd.marks = append(rd.marks, rd.bulkStart, rd.bulkStart+rd.bulkSize)
				rd.respPos = rd.bulkStart + rd.bulkSize + 2
				rd.respArg++
				rd.scan = rd.respPos + 1
				rd.bulkSize = 0
				rd.bulkStart = 0
				rd.respStage = 1
			default:
				rd.respStage = 1
			}
		}

		// complete command ends at respPos
		end := rd.respPos
		cmd = &Request{}
		b_ := b.Split(end)
		b.Swap(b_)
		cmd.Raw = b_
		cmd.Args = make([]*BufferView, len(rd.marks)/2)
		for h := 0; h < len(rd.marks); h += 2 {
			cmd.Args[h/2] = cmd.Raw.Slice(rd.marks[h], rd.marks[h+1])
		}
		rd.resetReadState()
		return cmd, false, nil
	default:
		rd.resetReadState()
		return nil, false, nil
	}
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

func (rd *Reader) readCommands() (cmds []*Request, err error) {
	// If we end up with both commands and an error, return the commands first
	// and delay the error to the next call.
	defer func() {
		if len(cmds) > 0 && err != nil {
			rd.pendingErr = err
			err = nil
		}
	}()

	b := rd.buf

	// If we have a pending error from the last call, only surface it when we
	// can't produce any commands this time.
	if rd.pendingErr != nil && b.Len() == 0 {
		err = rd.pendingErr
		rd.pendingErr = nil
		return nil, err
	}

	for {
		cmd, needMore, err := rd.readOneCommand()
		if err != nil {
			return cmds, err
		}
		if cmd != nil {
			cmds = append(cmds, cmd)
			// continue to parse more from current buffer
			continue
		}
		if len(cmds) > 0 {
			return cmds, nil
		}
		if !needMore {
			// nothing parsed, but not needMore: loop again
			continue
		}

		// need more data from reader
		if rd.rd == nil {
			return nil, errIncompleteCommand
		}
		newData := GetBuffer()
		n, rerr := rd.rd.Read(newData[:])
		if n > 0 {
			_, _ = b.Write(newData[:n])
		}
		PutBuffer(newData)

		if rerr != nil {
			if rerr == io.EOF && n > 0 {
				// got bytes, try parse again
				continue
			}
			return cmds, rerr
		}
		if n == 0 {
			return cmds, errIncompleteCommand
		}
	}
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
	if rd.buf != nil {
		rd.buf.Free()
		rd.buf = nil
	}
	rd.argsbuf = nil
	rd.marks = nil
	rd.cmds = nil
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
	wr.Close()
	return cmd, nil
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

func (s *Server) OnBoot(eng gnet.Engine) (action gnet.Action) {
	fmt.Println("OnBoot")
	s.mu.Lock()
	defer s.mu.Unlock()
	s.eg = &eng
	return
}

func (s *Server) OnShutdown(eng gnet.Engine) {
	fmt.Println("OnShutdown")
}

func (s *Server) OnOpen(c gnet.Conn) (out []byte, action gnet.Action) {
	fmt.Println("OnOpen")
	// If draining, reject new connections. Existing connections will be drained/closed.
	if s.Draining() {
		fmt.Println("OnOpen rejected because draining")
		return []byte("-ERR server is shutting down\r\n"), gnet.Close
	}
	c_ := &conn{
		conn: c,
		id:   c.RemoteAddr().String(),
		rd:   NewReader(c),
	}
	s.mu.Lock()
	s.conns[c_] = true
	s.mu.Unlock()

	if s.accept != nil {
		err := s.accept(c_)
		if err != nil {
			s.mu.Lock()
			delete(s.conns, c_)
			s.mu.Unlock()
			return []byte("-" + err.Error() + "\r\n"), gnet.Close
		}
	}
	c.SetContext(c_)
	s.workers.Register(c_.id, c_)
	return
}

func (s *Server) OnClose(c gnet.Conn, err error) (action gnet.Action) {
	fmt.Println("OnClose")
	// Remove from conn set.
	c_, ok := c.Context().(*conn)
	if !ok {
		return gnet.Close
	}
	s.mu.Lock()
	if s.conns != nil {
		delete(s.conns, c_)
	}
	s.mu.Unlock()
	if s.closed != nil {
		s.closed(c_, err)
	}
	s.workers.Unregister(c_.id)
	return gnet.Close
}

var (
	ErrQueueOverflow = []byte("-ERR queue overflow")
	ErrNoHandler     = []byte("-ERR no handler")
)

func (s *Server) OnTraffic(c gnet.Conn) (action gnet.Action) {
	fmt.Println("OnTraffic")

	c_, ok := c.Context().(*conn)
	if !ok {
		return gnet.Close
	}

	// If we've exceeded the drain deadline, stop processing any new incoming requests.
	// This prevents the server from waiting forever due to clients continuously sending commands.
	if s.Draining() {
		until := atomic.LoadInt64(&s.drainUntil)
		if until > 0 && time.Now().UnixNano() > until {
			return
		}
	}

	rec := time.Now()
	cmds, err := c_.rd.readCommands()

	// 如果没有解析出任何请求：
	// - io.ErrShortBuffer / EAGAIN / EWOULDBLOCK / incomplete 都表示“本次没有更多数据”，等下次触发即可。
	if len(cmds) == 0 {
		if err == nil ||
			errors.Is(err, io.ErrShortBuffer) ||
			errors.Is(err, syscall.EAGAIN) || errors.Is(err, syscall.EWOULDBLOCK) ||
			errors.Is(err, errIncompleteCommand) {
			return
		}
		// 其他错误（协议错误/IO 错误）继续走后续错误处理逻辑
	}

	// 如果已经解析出部分请求，但最后一个请求不完整，本次仍然先处理已解析的请求；
	// 不完整部分的中间状态已经保存在 Reader 中，等待下次 OnTraffic 补齐即可。
	if errors.Is(err, errIncompleteCommand) ||
		errors.Is(err, io.ErrShortBuffer) ||
		errors.Is(err, syscall.EAGAIN) || errors.Is(err, syscall.EWOULDBLOCK) {
		err = nil
	}

	id := c.RemoteAddr().String()
	if err != nil {
		s.workers.SubmitError(context.Background(), id, err)
		return
	}

	atomic.AddInt32(&c_.pending, int32(len(cmds)))

	for _, cmd_ := range cmds {
		cmd := cmd_
		err := s.workers.Submit(context.Background(), id, func(ctx context.Context) {
			var res = NewRespond()
			cmd.ReceiveTime = rec
			cmd.ProcessTime = time.Now()
			h := s.handler
			if s.Draining() {
				if h_ := s.getDrainHandler(); h_ != nil {
					h = h_
				}
			}
			if h != nil {
				h(c_, cmd, res)
			}
			cmd.ProcessDoneTime = time.Now()

			var callback gnet.AsyncCallback = func(c gnet.Conn, err error) error {
				if err == nil {
					cmd.FlushTime = time.Now()
					if s.after != nil {
						s.after(c_, cmd, res)
					}
				}
				cmd.Free()
				res.Close()
				pending := atomic.AddInt32(&c_.pending, -1)
				if err != nil || c_.needClose || (s.Draining() && pending == 0) {
					_ = c_.close()
				}
				return err
			}
			if h != nil {
				err = c.AsyncWritev(res.Data(), callback)
			} else {
				err = c.AsyncWrite(ErrNoHandler, callback)
			}
			if err != nil {
				cmd.Free()
				res.Close()
				atomic.AddInt32(&c_.pending, -1)
				_ = c_.close()
				return
			}
		})
		if err != nil {
			cmd.Free()
			atomic.AddInt32(&c_.pending, -1)
		}
	}
	return
}

func (s *Server) OnTick() (delay time.Duration, action gnet.Action) {
	fmt.Println("OnTick")

	// Default tick interval is low-frequency to minimize overhead.
	if !s.Draining() {
		delay = time.Second
		return
	}

	var closers []*conn
	s.mu.Lock()
	for c := range s.conns {
		pending := atomic.LoadInt32(&c.pending)
		fmt.Printf("pending:%s=%d\n", c.RemoteAddr().String(), pending)
		if pending == 0 {
			closers = append(closers, c)
		}
	}
	s.mu.Unlock()
	for _, c := range closers {
		_ = c.close()
	}

	delay = 200 * time.Millisecond
	return
}
