// Package redcon implements a Redis compatible server framework
package redcon

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
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
	r.Args = append(r.Args, v)
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
}

// NewRespond creates a new RESP writer.
func NewRespond() *Respond {
	return &Respond{Buffer: NewBuffer()}
}

func (r *Respond) RespType() Type {
	return GetType(r.Buffer)
}

func (r *Respond) GetArrayLength() (int, error) {
	return GetArrayLength(r.Buffer)
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
	AppendArray(r.Buffer, count)
}

// WriteBulk writes bulk bytes to the client.
func (r *Respond) WriteBulk(bulk []byte) *BufferView {
	return AppendBulk(r.Buffer, bulk)
}

// WriteBulkString writes a bulk string to the client.
func (r *Respond) WriteBulkString(bulk string) {
	AppendBulkString(r.Buffer, bulk)
}

// Data returns the unflushed buffer. This is a copy so changes
// to the resulting []byte will not affect the writer.
func (r *Respond) Data() [][]byte {
	return r.Buffer.Data()
}

func (r *Respond) Bytes() []byte {
	return r.Buffer.Bytes()
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
	_, _ = r.Buffer.Write(data)
}

func (r *Respond) Swap(r_ *Respond) { r.Buffer.Swap(r_.Buffer) }

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
