// Package redcon implements a Redis compatible server framework
package redcon

import (
	"bufio"
	"crypto/tls"
	"errors"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"time"
)

var (
	errUnbalancedQuotes       = &errProtocol{"unbalanced quotes in request"}
	errInvalidBulkLength      = &errProtocol{"invalid bulk length"}
	errInvalidMultiBulkLength = &errProtocol{"invalid multibulk length"}
	errDetached               = errors.New("detached")
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
	LocalAddr() string
	// RemoteAddr returns the remote address of the client connection.
	RemoteAddr() string
	// Close closes the connection.
	Close() error
	// Context returns a user-defined context
	Context() interface{}
	// SetContext sets a user-defined context
	SetContext(v interface{})
	// ReadPipeline returns all commands in current pipeline, if any
	// The commands are removed from the pipeline.
	ReadPipeline() []*Request
	// PeekPipeline returns all commands in current pipeline, if any.
	// The commands remain in the pipeline.
	PeekPipeline() []*Request
	// NetConn returns the base net.Conn connection
	NetConn() net.Conn
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
func (s *Server) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.ln == nil {
		return errors.New("not serving")
	}
	s.done = true
	err := s.ln.Close()
	if err != nil {
		return err
	}
	s.SetDraining(time.Second * 10)
	return nil
}

// ListenAndServe serves incoming connections.
func (s *Server) ListenAndServe() error {
	return s.ListenServeAndSignal(nil)
}

// Addr returns server's listen address
func (s *Server) Addr() net.Addr {
	return s.ln.Addr()
}

// Close stops listening on the TCP address.
// Already Accepted connections will be closed.
func (s *TLSServer) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.ln == nil {
		return errors.New("not serving")
	}
	s.done = true
	return s.ln.Close()
}

// ListenAndServe serves incoming connections.
func (s *TLSServer) ListenAndServe() error {
	return s.ListenServeAndSignal(nil)
}

func newServer() *Server {
	s := &Server{
		conns: make(map[*conn]bool),
	}
	return s
}

// Serve creates a new server and serves with the given net.Listener.
func Serve(ln net.Listener,
	handler func(conn Conn, cmd *Request, res *Respond),
	accept func(conn Conn) error,
	closed func(conn Conn, err error),
) error {
	s := newServer()
	s.mu.Lock()
	s.net = ln.Addr().Network()
	s.laddr = ln.Addr().String()
	s.ln = ln
	s.handler = handler
	s.accept = accept
	s.closed = closed
	s.mu.Unlock()
	return serve(s)
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

// ListenServeAndSignal serves incoming connections and passes nil or error
// when listening. signal can be nil.
func (s *Server) ListenServeAndSignal(signal chan error) error {
	ln, err := net.Listen(s.net, s.laddr)
	if err != nil {
		if signal != nil {
			signal <- err
		}
		return err
	}
	s.mu.Lock()
	s.ln = ln
	s.mu.Unlock()
	if signal != nil {
		signal <- nil
	}
	return serve(s)
}

// Serve serves incoming connections with the given net.Listener.
func (s *Server) Serve(ln net.Listener) error {
	s.mu.Lock()
	s.ln = ln
	s.net = ln.Addr().Network()
	s.laddr = ln.Addr().String()
	s.mu.Unlock()
	return serve(s)
}

func serve(s *Server) error {
	defer func() {
		s.ln.Close()
		draining := s.GetDrainDeadline()
		dur := draining - time.Now().Unix()
		if dur > 0 {
			wait := make(chan struct{})
			go func() {
				defer close(wait)
				t := time.NewTicker(time.Millisecond * 50)
				defer t.Stop()
				for range t.C {
					empty := false
					s.mu.Lock()
					empty = len(s.conns) == 0
					s.mu.Unlock()
					if empty {
						break
					}
				}
			}()
			select {
			case <-wait:
			case <-time.After(time.Duration(dur) * time.Second):
			}
		}
		func() {
			s.mu.Lock()
			defer s.mu.Unlock()
			for c := range s.conns {
				c.conn.Close()
			}
			s.conns = nil
		}()
	}()
	for {
		lnconn, err := s.ln.Accept()
		if err != nil {
			s.mu.Lock()
			done := s.done
			s.mu.Unlock()
			if done {
				return nil
			}
			if errors.Is(err, net.ErrClosed) {
				// see https://github.com/tidwall/redcon/issues/46
				return nil
			}
			if s.AcceptError != nil {
				s.AcceptError(err)
			}
			continue
		}
		c := &conn{
			conn: lnconn,
			addr: lnconn.RemoteAddr().String(),
			wr:   bufio.NewWriter(lnconn),
			rd:   NewReader(lnconn),
		}
		s.mu.Lock()
		c.idleClose = s.idleClose
		s.conns[c] = true
		s.mu.Unlock()
		if s.accept != nil {
			err = s.accept(c)
			if err != nil {
				s.mu.Lock()
				delete(s.conns, c)
				s.mu.Unlock()
				res := NewRespond()
				res.WriteError(err.Error())
				_, err = res.WriteTo(c.wr)
				res.Free()
				_ = c.Close()
				continue
			}
		}
		go handle(s, c)
	}
}

// handle manages the server connection.
func handle(s *Server, c *conn) {
	var err error
	defer func() {
		if err != errDetached {
			// do not close the connection when a detach is detected.
			c.conn.Close()
		}
		func() {
			// remove the conn from the server
			s.mu.Lock()
			defer s.mu.Unlock()
			delete(s.conns, c)
			if s.closed != nil {
				if err == io.EOF {
					err = nil
				}
				s.closed(c, err)
			}
		}()
	}()

	err = func() error {
		draining := int64(0)
		// read commands and feed back to the client
		for {
			if draining == 0 {
				draining = s.GetDrainDeadline()
			}
			// read pipeline commands
			if c.idleClose != 0 || draining > 0 {
				d := time.Now().Add(c.idleClose)
				if d.Unix() > draining {
					d = time.Unix(draining, 0)
				}
				_ = c.conn.SetReadDeadline(d)
			}
			cmds, err := c.rd.readCommands(nil)
			if err != nil {
				if err, ok := err.(*errProtocol); ok {
					// All protocol errors should attempt a response to
					// the client. Ignore write errors.
					res := NewRespond()
					res.WriteError("ERR " + err.Error())
					_, _ = res.WriteTo(c.wr)
					res.Free()
				}
				return err
			}
			c.cmds = cmds
			beg := time.Now()
			for len(c.cmds) > 0 {
				cmd := c.cmds[0]
				if len(c.cmds) == 1 {
					c.cmds = nil
				} else {
					c.cmds = c.cmds[1:]
				}
				if draining > 0 && time.Now().Unix() > draining {
					cmd.Free()
					_, _ = c.conn.Write([]byte("-ERR Server is shutting down\r\n"))
					continue
				}
				cmd.ReceiveTime = beg
				cmd.ProcessTime = time.Now()
				res := NewRespond()
				s.handler(c, cmd, res)
				cmd.ProcessDoneTime = time.Now()
				_, err = res.WriteTo(c.wr)
				if err != nil {
					cmd.Free()
					res.Free()
					return err
				}
				cmd.FlushTime = time.Now()
				if s.after != nil {
					s.after(c, cmd, res)
				}
				cmd.Free()
				res.Free()
			}
			if c.detached {
				// client has been detached
				return errDetached
			}
			if c.needClose {
				_ = c.close()
				return nil
			}
		}
	}()
}

// conn represents a client connection
type conn struct {
	conn      net.Conn
	wr        *bufio.Writer
	rd        *Reader
	addr      string
	ctx       interface{}
	detached  bool
	closed    bool
	cmds      []*Request
	idleClose time.Duration
	needClose bool
}

func (c *conn) LocalAddr() string {
	return c.conn.LocalAddr().String()
}

func (c *conn) Close() error {
	c.needClose = true
	return nil
}
func (c *conn) close() error {
	c.closed = true
	return c.conn.Close()
}
func (c *conn) Context() interface{}     { return c.ctx }
func (c *conn) SetContext(v interface{}) { c.ctx = v }
func (c *conn) RemoteAddr() string       { return c.addr }
func (c *conn) ReadPipeline() []*Request {
	cmds := c.cmds
	c.cmds = nil
	return cmds
}
func (c *conn) PeekPipeline() []*Request {
	return c.cmds
}
func (c *conn) NetConn() net.Conn {
	return c.conn
}

// Server defines a server for clients for managing client connections.
type Server struct {
	mu            sync.Mutex
	net           string
	laddr         string
	handler       func(conn Conn, cmd *Request, res *Respond)
	after         func(conn Conn, cmd *Request, res *Respond)
	accept        func(conn Conn) error
	closed        func(conn Conn, err error)
	conns         map[*conn]bool
	ln            net.Listener
	done          bool
	idleClose     time.Duration
	drainDeadline int64 //unix timestamp

	// AcceptError is an optional function used to handle Accept errors.
	AcceptError func(err error)
}

// TLSServer defines a server for clients for managing client connections.
type TLSServer struct {
	*Server
	config *tls.Config
}

// SetIdleClose will automatically close idle connections after the specified
// duration. Use zero to disable this feature.
func (s *Server) SetIdleClose(dur time.Duration) {
	s.mu.Lock()
	s.idleClose = dur
	s.mu.Unlock()
}

func (s *Server) SetDraining(dur time.Duration) {
	old := atomic.LoadInt64(&s.drainDeadline)
	if old > 0 {
		return
	}
	atomic.CompareAndSwapInt64(&s.drainDeadline, old, time.Now().Add(dur).Unix())
}

func (s *Server) IsDraining() bool {
	old := atomic.LoadInt64(&s.drainDeadline)
	return old > 0
}

func (s *Server) GetDrainDeadline() int64 {
	return atomic.LoadInt64(&s.drainDeadline)
}
