package redcon

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"io"
	"log"
	"math/rand"
	"net"
	"runtime"
	"runtime/debug"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/panjf2000/gnet/v2"
)

// TestRandomCommands fills a bunch of random commands and test various
// ways that the reader may receive data.
func TestRandomCommands(t *testing.T) {
	rand.Seed(time.Now().UnixNano())

	// build random commands.
	gcmds := make([][]string, 10000)
	for i := 0; i < len(gcmds); i++ {
		args := make([]string, (rand.Int()%50)+1) // 1-50 args
		for j := 0; j < len(args); j++ {
			n := rand.Int() % 10
			if j == 0 {
				n++
			}
			arg := make([]byte, n)
			for k := 0; k < len(arg); k++ {
				arg[k] = byte(rand.Int() % 0xFF)
			}
			args[j] = string(arg)
		}
		gcmds[i] = args
	}
	// create a list of a buffers
	var bufs []string

	// pipe valid RESP commands
	for i := 0; i < len(gcmds); i++ {
		args := gcmds[i]
		msg := fmt.Sprintf("*%d\r\n", len(args))
		for j := 0; j < len(args); j++ {
			msg += fmt.Sprintf("$%d\r\n%s\r\n", len(args[j]), args[j])
		}
		bufs = append(bufs, msg)
	}
	bufs = append(bufs, "RESET THE INDEX\r\n")

	// pipe valid plain commands
	for i := 0; i < len(gcmds); i++ {
		args := gcmds[i]
		var msg string
		for j := 0; j < len(args); j++ {
			quotes := false
			var narg []byte
			arg := args[j]
			if len(arg) == 0 {
				quotes = true
			}
			for k := 0; k < len(arg); k++ {
				switch arg[k] {
				default:
					narg = append(narg, arg[k])
				case ' ':
					quotes = true
					narg = append(narg, arg[k])
				case '\\', '"', '*':
					quotes = true
					narg = append(narg, '\\', arg[k])
				case '\r':
					quotes = true
					narg = append(narg, '\\', 'r')
				case '\n':
					quotes = true
					narg = append(narg, '\\', 'n')
				}
			}
			msg += " "
			if quotes {
				msg += "\""
			}
			msg += string(narg)
			if quotes {
				msg += "\""
			}
		}
		if msg != "" {
			msg = msg[1:]
		}
		msg += "\r\n"
		bufs = append(bufs, msg)
	}
	bufs = append(bufs, "RESET THE INDEX\r\n")

	// pipe valid RESP commands in broken chunks
	lmsg := ""
	for i := 0; i < len(gcmds); i++ {
		args := gcmds[i]
		msg := fmt.Sprintf("*%d\r\n", len(args))
		for j := 0; j < len(args); j++ {
			msg += fmt.Sprintf("$%d\r\n%s\r\n", len(args[j]), args[j])
		}
		msg = lmsg + msg
		if len(msg) > 0 {
			lmsg = msg[len(msg)/2:]
			msg = msg[:len(msg)/2]
		}
		bufs = append(bufs, msg)
	}
	bufs = append(bufs, lmsg)
	bufs = append(bufs, "RESET THE INDEX\r\n")

	// pipe valid RESP commands in large broken chunks
	lmsg = ""
	for i := 0; i < len(gcmds); i++ {
		args := gcmds[i]
		msg := fmt.Sprintf("*%d\r\n", len(args))
		for j := 0; j < len(args); j++ {
			msg += fmt.Sprintf("$%d\r\n%s\r\n", len(args[j]), args[j])
		}
		if len(lmsg) < 1500 {
			lmsg += msg
			continue
		}
		msg = lmsg + msg
		if len(msg) > 0 {
			lmsg = msg[len(msg)/2:]
			msg = msg[:len(msg)/2]
		}
		bufs = append(bufs, msg)
	}
	bufs = append(bufs, lmsg)
	bufs = append(bufs, "RESET THE INDEX\r\n")

	// Pipe the buffers in a background routine
	rd, wr := io.Pipe()
	go func() {
		defer wr.Close()
		for _, msg := range bufs {
			io.WriteString(wr, msg)
		}
	}()
	defer rd.Close()
	cnt := 0
	idx := 0
	start := time.Now()
	r := NewReader(rd)
	for {
		cmd, err := r.ReadCommand()
		if err != nil {
			if err == io.EOF {
				break
			}
			log.Fatal(err)
		}
		if len(cmd.Args) == 3 && string(cmd.Args[0].Bytes()) == "RESET" &&
			string(cmd.Args[1].Bytes()) == "THE" && string(cmd.Args[2].Bytes()) == "INDEX" {
			if idx != len(gcmds) {
				t.Fatalf("did not process all commands")
			}
			idx = 0
			break
		}
		if len(cmd.Args) != len(gcmds[idx]) {
			t.Fatalf("len not equal for index %d -- %d != %d", idx, len(cmd.Args), len(gcmds[idx]))
		}
		for i := 0; i < len(cmd.Args); i++ {
			if i == 0 {
				if cmd.Args[i].Len() == len(gcmds[idx][i]) {
					ok := true
					for j := 0; j < cmd.Args[i].Len(); j++ {
						c1, c2 := cmd.Args[i].At(j), gcmds[idx][i][j]
						if c1 >= 'A' && c1 <= 'Z' {
							c1 += 32
						}
						if c2 >= 'A' && c2 <= 'Z' {
							c2 += 32
						}
						if c1 != c2 {
							ok = false
							break
						}
					}
					if ok {
						continue
					}
				}
			} else if string(cmd.Args[i].Bytes()) == string(gcmds[idx][i]) {
				continue
			}
			t.Fatalf("not equal for index %d/%d", idx, i)
		}
		idx++
		cnt++
	}
	if false {
		dur := time.Since(start)
		fmt.Printf("%d commands in %s - %.0f ops/sec\n", cnt, dur, float64(cnt)/(float64(dur)/float64(time.Second)))
	}
}

func TestServerTCP(t *testing.T) {
	testServerNetwork(t, "tcp", ":12345")
}

func TestNewServerTCP_RoundTrip(t *testing.T) {
	// Use a free local port (best-effort) to avoid collisions.
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	_, port, err := net.SplitHostPort(ln.Addr().String())
	if err != nil {
		_ = ln.Close()
		t.Fatalf("split host port: %v", err)
	}
	_ = ln.Close()
	laddr := "127.0.0.1:" + port

	s := NewServer(laddr,
		func(conn Conn, cmd *Request, res *Respond) {
			switch strings.ToLower(string(cmd.Args[0].Bytes())) {
			case "ping":
				res.WriteString("PONG")
			default:
				res.WriteError("ERR unknown command")
			}
		},
		nil, // after
		nil, // accept
		nil, // closed
	)

	done := make(chan error, 1)
	go func() {
		done <- s.ListenAndServe()
	}()
	defer func() {
		_ = s.Close(context.Background())
		<-done
	}()

	// Dial with retry to avoid flaky startup timing.
	var c net.Conn
	for i := 0; i < 50; i++ {
		c, err = net.Dial("tcp", laddr)
		if err == nil {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer c.Close()

	_ = c.SetDeadline(time.Now().Add(2 * time.Second))
	if _, err := io.WriteString(c, "PING\r\n"); err != nil {
		t.Fatalf("write: %v", err)
	}
	rd := bufio.NewReader(c)
	line, err := rd.ReadString('\n')
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if line != "+PONG\r\n" {
		t.Fatalf("expected %q, got %q", "+PONG\r\n", line)
	}
}

func TestServerTCP_PartialPacket(t *testing.T) {
	// Use a free local port (best-effort) to avoid collisions.
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	_, port, err := net.SplitHostPort(ln.Addr().String())
	if err != nil {
		_ = ln.Close()
		t.Fatalf("split host port: %v", err)
	}
	_ = ln.Close()
	laddr := "127.0.0.1:" + port

	closedCh := make(chan error, 1)
	s := NewServerNetwork("tcp", laddr,
		func(conn Conn, cmd *Request, res *Respond) {
			switch strings.ToLower(string(cmd.Args[0].Bytes())) {
			default:
				res.WriteError("ERR unknown command '" + string(cmd.Args[0].Bytes()) + "'")
			case "ping":
				res.WriteString("PONG")
			case "echo":
				if len(cmd.Args) != 2 {
					res.WriteError("ERR wrong number of arguments for 'echo' command")
					return
				}
				res.WriteBulk(cmd.Args[1].Bytes())
			}
		},
		nil,
		nil,
		func(conn Conn, err error) {
			select {
			case closedCh <- err:
			default:
			}
		},
	)

	done := make(chan error, 1)
	go func() {
		done <- s.ListenAndServe()
	}()
	defer func() {
		_ = s.Close(context.Background())
		<-done
	}()

	// Dial with retry to avoid flaky startup timing.
	var c net.Conn
	for i := 0; i < 50; i++ {
		c, err = net.Dial("tcp", laddr)
		if err == nil {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer c.Close()

	rd := bufio.NewReader(c)

	readOneRESP := func() (string, error) {
		prefix, err := rd.ReadByte()
		if err != nil {
			return "", err
		}
		switch prefix {
		case '+', '-', ':':
			line, err := rd.ReadBytes('\n')
			if err != nil {
				return "", err
			}
			return string(append([]byte{prefix}, line...)), nil
		case '$':
			line, err := rd.ReadBytes('\n')
			if err != nil {
				return "", err
			}
			n, err := strconv.Atoi(strings.TrimSuffix(strings.TrimSuffix(string(line), "\n"), "\r"))
			if err != nil {
				return "", err
			}
			if n < 0 {
				return string(append([]byte{'$'}, line...)), nil
			}
			body := make([]byte, n+2)
			if _, err := io.ReadFull(rd, body); err != nil {
				return "", err
			}
			out := append([]byte{'$'}, line...)
			out = append(out, body...)
			return string(out), nil
		default:
			return "", fmt.Errorf("unexpected resp prefix: %q", prefix)
		}
	}

	assertConnStillOpen := func() {
		// Give gnet a chance to process the first half-packet; it should NOT close the connection.
		time.Sleep(80 * time.Millisecond)
		select {
		case cerr := <-closedCh:
			t.Fatalf("connection closed unexpectedly while waiting for remaining bytes, err=%v", cerr)
		default:
		}
	}

	// 1) Send a RESP command in two halves, ensure server doesn't error/close on partial data.
	// PING => +PONG\r\n
	part1 := []byte("*1\r\n$4\r\nPI")
	part2 := []byte("NG\r\n")
	if _, err := c.Write(part1); err != nil {
		t.Fatalf("write part1: %v", err)
	}
	assertConnStillOpen()
	if _, err := c.Write(part2); err != nil {
		t.Fatalf("write part2: %v", err)
	}
	got, err := readOneRESP()
	if err != nil {
		t.Fatalf("read resp: %v", err)
	}
	if got != "+PONG\r\n" {
		t.Fatalf("expected %q, got %q", "+PONG\r\n", got)
	}

	// 2) Split in the middle of a bulk body to simulate "only received half".
	// ECHO HELLO => $5\r\nHELLO\r\n
	part3 := []byte("*2\r\n$4\r\nECHO\r\n$5\r\nHE")
	part4 := []byte("LLO\r\n")
	if _, err := c.Write(part3); err != nil {
		t.Fatalf("write part3: %v", err)
	}
	assertConnStillOpen()
	if _, err := c.Write(part4); err != nil {
		t.Fatalf("write part4: %v", err)
	}
	got, err = readOneRESP()
	if err != nil {
		t.Fatalf("read resp2: %v", err)
	}
	if got != "$5\r\nHELLO\r\n" {
		t.Fatalf("expected %q, got %q", "$5\r\nHELLO\r\n", got)
	}
}

func testServerNetwork(t *testing.T, network, laddr string) {
	s := NewServerNetwork(network, laddr,
		func(conn Conn, cmd *Request, res *Respond) {
			switch strings.ToLower(string(cmd.Args[0].Bytes())) {
			default:
				res.WriteError("ERR unknown command '" + string(cmd.Args[0].Bytes()) + "'")
			case "ping":
				res.WriteString("PONG")
			case "quit":
				res.WriteString("OK")
				res.Close()
			case "int":
				res.WriteInt(100)
			case "bulk":
				res.WriteBulkString("bulk")
			case "bulkbytes":
				res.WriteBulk([]byte("bulkbytes"))
			case "null":
				res.WriteNull()
			case "err":
				res.WriteError("ERR error")
			case "array":
				res.WriteArray(2)
				res.WriteInt(99)
				res.WriteString("Hi!")
			}
		},
		func(conn Conn, cmd *Request, res *Respond) {},
		func(conn Conn) error {
			t.Logf("accept: %s", conn.RemoteAddr())
			return nil
		},
		func(conn Conn, err error) {
			t.Logf("closed: %s [%v]", conn.RemoteAddr(), err)
		},
	)
	if err := s.Close(context.Background()); err == nil {
		t.Fatalf("expected an error, should not be able to close before serving")
	}
	go func() {
		time.Sleep(time.Second)
		if err := ListenAndServeNetwork(network, laddr, func(conn Conn, cmd *Request, res *Respond) {}, nil, nil, nil); err == nil {
			panic("expected an error, should not be able to listen on the same port")
		}
		time.Sleep(time.Second)

		err := s.Close(context.Background())
		if err != nil {
			panic(err)
		}
		t.Logf("closed %s", s.laddr)
		err = s.Close(context.Background())
		if err == nil {
			panic("expected an error")
		}
		t.Logf("closed err: %v", err)
	}()
	done := make(chan bool)
	go func() {
		defer func() {
			done <- true
		}()
		time.Sleep(time.Second)
		c, err := net.Dial(network, laddr)
		if err != nil {
			panic(err)
		}
		defer c.Close()
		do := func(cmd string) (string, error) {
			io.WriteString(c, cmd)
			buf := make([]byte, 1024)
			n, err := c.Read(buf)
			if err != nil {
				return "", err
			}
			return string(buf[:n]), nil
		}
		res, err := do("PING\r\n")
		if err != nil {
			panic(err)
		}
		if res != "+PONG\r\n" {
			panic(fmt.Sprintf("expecting '+PONG\r\n', got '%v'", res))
		}
		res, err = do("BULK\r\n")
		if err != nil {
			panic(err)
		}
		if res != "$4\r\nbulk\r\n" {
			panic(fmt.Sprintf("expecting bulk, got '%v'", res))
		}
		res, err = do("BULKBYTES\r\n")
		if err != nil {
			panic(err)
		}
		if res != "$9\r\nbulkbytes\r\n" {
			panic(fmt.Sprintf("expecting bulkbytes, got '%v'", res))
		}
		res, err = do("INT\r\n")
		if err != nil {
			panic(err)
		}
		if res != ":100\r\n" {
			panic(fmt.Sprintf("expecting int, got '%v'", res))
		}
		res, err = do("NULL\r\n")
		if err != nil {
			panic(err)
		}
		if res != "$-1\r\n" {
			panic(fmt.Sprintf("expecting nul, got '%v'", res))
		}
		res, err = do("ARRAY\r\n")
		if err != nil {
			panic(err)
		}
		if res != "*2\r\n:99\r\n+Hi!\r\n" {
			panic(fmt.Sprintf("expecting array, got '%v'", res))
		}
		res, err = do("ERR\r\n")
		if err != nil {
			panic(err)
		}
		if res != "-ERR error\r\n" {
			panic(fmt.Sprintf("expecting array, got '%v'", res))
		}
	}()
	go func() {
		err := s.ListenAndServe()
		if err != nil {
			panic(err)
		}
	}()
	<-done
}

func TestWriter(t *testing.T) {
	wr := NewRespond()
	wr.WriteError("ERR bad stuff")
	if string(wr.Bytes()) != "-ERR bad stuff\r\n" {
		t.Fatal("failed")
	}
	wr.Reset()

	wr.WriteString("HELLO")
	if string(wr.Bytes()) != "+HELLO\r\n" {
		t.Fatal("failed")
	}
	wr.Reset()

	wr.WriteInt(-1234)
	if string(wr.Bytes()) != ":-1234\r\n" {
		t.Fatal("failed")
	}
	wr.Reset()

	wr.WriteNull()
	if string(wr.Bytes()) != "$-1\r\n" {
		t.Fatal("failed")
	}
	wr.Reset()

	// Null Array "*-1\r\n"
	wr.WriteArray(-1)
	if string(wr.Bytes()) != "*-1\r\n" {
		t.Fatal("failed")
	}
	wr.Reset()

	wr.WriteBulk([]byte("HELLO\r\nPLANET"))
	if string(wr.Bytes()) != "$13\r\nHELLO\r\nPLANET\r\n" {
		t.Fatal("failed")
	}
	wr.Reset()

	wr.WriteBulkString("HELLO\r\nPLANET")
	if string(wr.Bytes()) != "$13\r\nHELLO\r\nPLANET\r\n" {
		t.Fatal("failed")
	}
	wr.Reset()

	wr.WriteArray(3)
	wr.WriteBulkString("THIS")
	wr.WriteBulkString("THAT")
	wr.WriteString("THE OTHER THING")
	if string(wr.Bytes()) != "*3\r\n$4\r\nTHIS\r\n$4\r\nTHAT\r\n+THE OTHER THING\r\n" {
		t.Fatal("failed")
	}
	wr.Reset()
}

func TestRespond_WriteArray_GetRESP_Structure(t *testing.T) {
	wr := NewRespond()

	// Outer: [ Inner: [ :1, :2 ], Bulk("bar") ]
	wr.WriteArray(2)
	wr.WriteArray(2)
	wr.WriteInt(1)
	wr.WriteInt(2)
	wr.WriteBulkString("bar")

	resp := wr.GetRESP()
	if resp.Type != Array || resp.Count != 2 || len(resp.Array) != 2 {
		t.Fatalf("expected root Array count=2, got Type=%v Count=%d len=%d", resp.Type, resp.Count, len(resp.Array))
	}
	if resp.Array[0].Type != Array || resp.Array[0].Count != 2 || len(resp.Array[0].Array) != 2 {
		t.Fatalf("expected root[0] Array count=2, got Type=%v Count=%d len=%d", resp.Array[0].Type, resp.Array[0].Count, len(resp.Array[0].Array))
	}
	if resp.Array[0].Array[0].Type != Integer || resp.Array[0].Array[0].Int() != 1 {
		t.Fatalf("expected root[0][0]=:1, got Type=%v Int=%d", resp.Array[0].Array[0].Type, resp.Array[0].Array[0].Int())
	}
	if resp.Array[0].Array[1].Type != Integer || resp.Array[0].Array[1].Int() != 2 {
		t.Fatalf("expected root[0][1]=:2, got Type=%v Int=%d", resp.Array[0].Array[1].Type, resp.Array[0].Array[1].Int())
	}
	if resp.Array[1].Type != Bulk || resp.Array[1].String() != "bar" {
		t.Fatalf("expected root[1]=$bar, got Type=%v String=%q", resp.Array[1].Type, resp.Array[1].String())
	}

	expBytes := "*2\r\n*2\r\n:1\r\n:2\r\n$3\r\nbar\r\n"
	if string(wr.Bytes()) != expBytes {
		t.Fatalf("expected bytes=%q, got %q", expBytes, string(wr.Bytes()))
	}
}

func TestRespond_ReadRESP_Structure(t *testing.T) {
	raw := "*3\r\n+OK\r\n:1\r\n$3\r\nbar\r\n"
	wr := NewRespond()
	if err := wr.ReadRESP(bytes.NewReader([]byte(raw))); err != nil {
		t.Fatalf("ReadRESP error: %v", err)
	}

	resp := wr.GetRESP()
	if resp.Type != Array || resp.Count != 3 || len(resp.Array) != 3 {
		t.Fatalf("expected Array count=3, got Type=%v Count=%d len=%d", resp.Type, resp.Count, len(resp.Array))
	}
	if resp.Array[0].Type != String || resp.Array[0].String() != "OK" {
		t.Fatalf("expected [0]=+OK, got Type=%v String=%q", resp.Array[0].Type, resp.Array[0].String())
	}
	if resp.Array[1].Type != Integer || resp.Array[1].Int() != 1 {
		t.Fatalf("expected [1]=:1, got Type=%v Int=%d", resp.Array[1].Type, resp.Array[1].Int())
	}
	if resp.Array[2].Type != Bulk || resp.Array[2].String() != "bar" {
		t.Fatalf("expected [2]=$bar, got Type=%v String=%q", resp.Array[2].Type, resp.Array[2].String())
	}
	if string(wr.Bytes()) != raw {
		t.Fatalf("expected buffer bytes=%q, got %q", raw, string(wr.Bytes()))
	}
}

func TestRequest_WriteArray_WriteBulk_ArgsAndRaw(t *testing.T) {
	req := NewRequest()
	req.WriteArray(2)
	req.WriteBulk([]byte("SET"))
	req.WriteBulk([]byte("a"))

	if len(req.Args) != 2 {
		t.Fatalf("expected Args len=2, got %d", len(req.Args))
	}
	if string(req.Args[0].Bytes()) != "SET" {
		t.Fatalf("expected Args[0]=%q, got %q", "SET", string(req.Args[0].Bytes()))
	}
	if string(req.Args[1].Bytes()) != "a" {
		t.Fatalf("expected Args[1]=%q, got %q", "a", string(req.Args[1].Bytes()))
	}

	expRaw := "*2\r\n$3\r\nSET\r\n$1\r\na\r\n"
	if string(req.Raw.Bytes()) != expRaw {
		t.Fatalf("expected Raw=%q, got %q", expRaw, string(req.Raw.Bytes()))
	}
}

func testMakeRawCommands(rawargs [][]string) []string {
	var rawcmds []string
	for i := 0; i < len(rawargs); i++ {
		rawcmd := "*" + strconv.FormatUint(uint64(len(rawargs[i])), 10) + "\r\n"
		for j := 0; j < len(rawargs[i]); j++ {
			rawcmd += "$" + strconv.FormatUint(uint64(len(rawargs[i][j])), 10) + "\r\n"
			rawcmd += rawargs[i][j] + "\r\n"
		}
		rawcmds = append(rawcmds, rawcmd)
	}
	return rawcmds
}

func TestReaderRespRandom(t *testing.T) {
	// This test previously had placeholder code that produced nil-slice index
	// linter warnings. Keep it small but real to validate RESP reader stability.
	rand.Seed(time.Now().UnixNano())

	commands := 200
	maxArgs := 8
	maxArgBytes := 64

	rawargs := make([][]string, 0, commands)
	for i := 0; i < commands; i++ {
		n := int(rand.Int()%maxArgs) + 1 // at least 1 arg
		args := make([]string, 0, n)
		for j := 0; j < n; j++ {
			ln := int(rand.Int() % maxArgBytes)
			b := make([]byte, ln)
			_, _ = rand.Read(b)
			args = append(args, string(b))
		}
		rawargs = append(rawargs, args)
	}

	rawcmds := testMakeRawCommands(rawargs)
	data := strings.Join(rawcmds, "")
	rd := NewReader(bytes.NewBufferString(data))
	for i := 0; i < len(rawcmds); i++ {
		cmd, err := rd.ReadCommand()
		if err != nil {
			t.Fatal(err)
		}
		if string(cmd.Raw.Bytes()) != rawcmds[i] {
			t.Fatalf("expected '%v', got '%v'", rawcmds[i], string(cmd.Raw.Bytes()))
		}
		if len(cmd.Args) != len(rawargs[i]) {
			t.Fatalf("expected '%v', got '%v'", len(rawargs[i]), len(cmd.Args))
		}
		for j := 0; j < len(rawargs[i]); j++ {
			if string(cmd.Args[j].Bytes()) != rawargs[i][j] {
				t.Fatalf("expected '%v', got '%v'", rawargs[i][j], string(cmd.Args[j].Bytes()))
			}
		}
	}
}

func TestPlainReader(t *testing.T) {
	rawargs := [][]string{
		{"HELLO", "WORLD"},
		{"HELLO", "WORLD"},
		{"HELLO", "PLANET"},
		{"HELLO", "JELLO"},
		{"HELLO ", "JELLO"},
	}
	rawcmds := []string{
		"HELLO WORLD\n",
		"HELLO WORLD\r\n",
		"  HELLO  PLANET \r\n",
		" \"HELLO\" \"JELLO\" \r\n",
		" \"HELLO \" JELLO \n",
	}
	rawres := []string{
		"*2\r\n$5\r\nHELLO\r\n$5\r\nWORLD\r\n",
		"*2\r\n$5\r\nHELLO\r\n$5\r\nWORLD\r\n",
		"*2\r\n$5\r\nHELLO\r\n$6\r\nPLANET\r\n",
		"*2\r\n$5\r\nHELLO\r\n$5\r\nJELLO\r\n",
		"*2\r\n$6\r\nHELLO \r\n$5\r\nJELLO\r\n",
	}
	data := strings.Join(rawcmds, "")
	rd := NewReader(bytes.NewBufferString(data))
	for i := 0; i < len(rawcmds); i++ {
		if len(rawargs[i]) == 0 {
			continue
		}
		cmd, err := rd.ReadCommand()
		if err != nil {
			t.Fatal(err)
		}
		if string(cmd.Raw.Bytes()) != rawres[i] {
			t.Fatalf("expected '%v', got '%v'", rawres[i], string(cmd.Raw.Bytes()))
		}
		if len(cmd.Args) != len(rawargs[i]) {
			t.Fatalf("expected '%v', got '%v'", len(rawargs[i]), len(cmd.Args))
		}
		for j := 0; j < len(rawargs[i]); j++ {
			if string(cmd.Args[j].Bytes()) != rawargs[i][j] {
				t.Fatalf("expected '%v', got '%v'", rawargs[i][j], string(cmd.Args[j].Bytes()))
			}
		}
	}
}

func TestParse(t *testing.T) {
	_, err := Parse(nil)
	if err != errIncompleteCommand {
		t.Fatalf("expected '%v', got '%v'", errIncompleteCommand, err)
	}
	_, err = Parse([]byte("*1\r\n"))
	if err != errIncompleteCommand {
		t.Fatalf("expected '%v', got '%v'", errIncompleteCommand, err)
	}
	_, err = Parse([]byte("*-1\r\n"))
	if err != errInvalidMultiBulkLength {
		t.Fatalf("expected '%v', got '%v'", errInvalidMultiBulkLength, err)
	}
	_, err = Parse([]byte("*0\r\n"))
	if err != errInvalidMultiBulkLength {
		t.Fatalf("expected '%v', got '%v'", errInvalidMultiBulkLength, err)
	}
	cmd, err := Parse([]byte("*1\r\n$1\r\nA\r\n"))
	if err != nil {
		t.Fatal(err)
	}
	if string(cmd.Raw.Bytes()) != "*1\r\n$1\r\nA\r\n" {
		t.Fatalf("expected '%v', got '%v'", "*1\r\n$1\r\nA\r\n", string(cmd.Raw.Bytes()))
	}
	if len(cmd.Args) != 1 {
		t.Fatalf("expected '%v', got '%v'", 1, len(cmd.Args))
	}
	if string(cmd.Args[0].Bytes()) != "A" {
		t.Fatalf("expected '%v', got '%v'", "A", string(cmd.Args[0].Bytes()))
	}
	cmd, err = Parse([]byte("A\r\n"))
	if err != nil {
		t.Fatal(err)
	}
	if string(cmd.Raw.Bytes()) != "*1\r\n$1\r\nA\r\n" {
		t.Fatalf("expected '%v', got '%v'", "*1\r\n$1\r\nA\r\n", string(cmd.Raw.Bytes()))
	}
	if len(cmd.Args) != 1 {
		t.Fatalf("expected '%v', got '%v'", 1, len(cmd.Args))
	}
	if string(cmd.Args[0].Bytes()) != "A" {
		t.Fatalf("expected '%v', got '%v'", "A", string(cmd.Args[0].Bytes()))
	}
}

// minimalGnetConn is a minimal in-memory implementation of gnet.Conn for unit tests.
// It only supports what Reader/OnTraffic need in this test (Read + Context + RemoteAddr).
type minimalGnetConn struct {
	ctx any

	in  []byte
	pos int

	remote net.Addr

	mu     sync.Mutex
	frames [][]byte
}

func newMinimalGnetConn() *minimalGnetConn {
	return &minimalGnetConn{
		remote: &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 20000},
	}
}

func (c *minimalGnetConn) appendInbound(p []byte) { c.in = append(c.in, p...) }

func (c *minimalGnetConn) resetFrames() {
	c.mu.Lock()
	c.frames = nil
	c.mu.Unlock()
}

func (c *minimalGnetConn) getFrames() [][]byte {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([][]byte, len(c.frames))
	for i := range c.frames {
		out[i] = append([]byte(nil), c.frames[i]...)
	}
	return out
}

// ---- gnet.Conn ----
func (c *minimalGnetConn) Context() any              { return c.ctx }
func (c *minimalGnetConn) SetContext(ctx any)        { c.ctx = ctx }
func (c *minimalGnetConn) RemoteAddr() net.Addr      { return c.remote }
func (c *minimalGnetConn) LocalAddr() net.Addr       { return c.remote }
func (c *minimalGnetConn) EventLoop() gnet.EventLoop { return nil }
func (c *minimalGnetConn) Wake(cb gnet.AsyncCallback) error {
	if cb != nil {
		return cb(c, nil)
	}
	return nil
}
func (c *minimalGnetConn) CloseWithCallback(cb gnet.AsyncCallback) error {
	if cb != nil {
		return cb(c, nil)
	}
	return nil
}
func (c *minimalGnetConn) Close() error                     { return nil }
func (c *minimalGnetConn) SetDeadline(time.Time) error      { return nil }
func (c *minimalGnetConn) SetReadDeadline(time.Time) error  { return nil }
func (c *minimalGnetConn) SetWriteDeadline(time.Time) error { return nil }
func (c *minimalGnetConn) Fd() int                          { return 0 }
func (c *minimalGnetConn) Dup() (int, error)                { return 0, nil }
func (c *minimalGnetConn) SetReadBuffer(int) error          { return nil }
func (c *minimalGnetConn) SetWriteBuffer(int) error         { return nil }
func (c *minimalGnetConn) SetLinger(int) error              { return nil }
func (c *minimalGnetConn) SetKeepAlivePeriod(time.Duration) error {
	return nil
}
func (c *minimalGnetConn) SetKeepAlive(bool, time.Duration, time.Duration, int) error {
	return nil
}
func (c *minimalGnetConn) SetNoDelay(bool) error { return nil }

// ---- gnet.Reader (unused methods can be stubs) ----
func (c *minimalGnetConn) Read(p []byte) (int, error) {
	if c.pos >= len(c.in) {
		return 0, io.EOF
	}
	n := copy(p, c.in[c.pos:])
	c.pos += n
	return n, nil
}
func (c *minimalGnetConn) WriteTo(w io.Writer) (int64, error) { return 0, io.EOF }
func (c *minimalGnetConn) Next(int) ([]byte, error)           { return nil, io.ErrShortBuffer }
func (c *minimalGnetConn) Peek(int) ([]byte, error)           { return nil, io.ErrShortBuffer }
func (c *minimalGnetConn) Discard(int) (int, error)           { return 0, io.ErrShortBuffer }
func (c *minimalGnetConn) InboundBuffered() int               { return len(c.in) - c.pos }

// ---- gnet.Writer (unused in this test) ----
func (c *minimalGnetConn) Write(p []byte) (int, error)                   { return len(p), nil }
func (c *minimalGnetConn) ReadFrom(r io.Reader) (int64, error)           { return 0, nil }
func (c *minimalGnetConn) SendTo(buf []byte, addr net.Addr) (int, error) { return len(buf), nil }
func (c *minimalGnetConn) Writev(bs [][]byte) (int, error)               { return 0, nil }
func (c *minimalGnetConn) Flush() error                                  { return nil }
func (c *minimalGnetConn) OutboundBuffered() int                         { return 0 }
func (c *minimalGnetConn) AsyncWrite(buf []byte, cb gnet.AsyncCallback) error {
	c.mu.Lock()
	c.frames = append(c.frames, append([]byte(nil), buf...))
	c.mu.Unlock()
	if cb != nil {
		return cb(c, nil)
	}
	return nil
}
func (c *minimalGnetConn) AsyncWritev(bs [][]byte, cb gnet.AsyncCallback) error {
	n := 0
	for _, b := range bs {
		n += len(b)
	}
	out := make([]byte, 0, n)
	for _, b := range bs {
		out = append(out, b...)
	}
	c.mu.Lock()
	c.frames = append(c.frames, out)
	c.mu.Unlock()
	if cb != nil {
		return cb(c, nil)
	}
	return nil
}

func TestOnTraffic_Drop_ReleasesBufferToPool(t *testing.T) {
	// Disable GC so sync.Pool keeps objects and our allocation counters stay deterministic.
	prevGC := debug.SetGCPercent(-1)
	defer debug.SetGCPercent(prevGC)
	prevP := runtime.GOMAXPROCS(1)
	defer runtime.GOMAXPROCS(prevP)

	// Count how many times chunkPool/smallChunkPool allocate new pages.
	var bigAllocs atomic.Int32
	var smallAllocs atomic.Int32
	prevBigNew := chunkPool.New
	prevSmallNew := smallChunkPool.New
	chunkPool.New = func() interface{} {
		bigAllocs.Add(1)
		return new(Chunk)
	}
	smallChunkPool.New = func() interface{} {
		smallAllocs.Add(1)
		return new(SmallChunk)
	}
	defer func() {
		chunkPool.New = prevBigNew
		smallChunkPool.New = prevSmallNew
	}()

	pool, err := NewSmartPool(1, 1)
	if err != nil {
		t.Fatalf("NewSmartPool: %v", err)
	}
	s := newServer()
	s.workers = pool

	c := newMinimalGnetConn()
	c_ := &conn{conn: c, rd: NewReader(c)}
	c.SetContext(c_)
	id := c.RemoteAddr().String()

	// Block the only worker so the per-conn queue can be filled to capacity.
	started := make(chan struct{})
	block := make(chan struct{})
	if err := s.workers.Submit(id, nil, func(ctx context.Context) {
		close(started)
		<-block
	}); err != nil {
		t.Fatalf("submit blocker: %v", err)
	}
	<-started

	// Fill the per-conn queue (128).
	for i := 0; i < 128; i++ {
		if err := s.workers.Submit(id, nil, func(context.Context) {}); err != nil {
			t.Fatalf("fill submit %d: %v", i, err)
		}
	}

	// Now every OnTraffic will parse a command but fail to enqueue -> must Free() buffers.
	// Use a big argument so cmd.Raw allocates big pages.
	bigArg := strings.Repeat("a", 1024)
	iters := 200
	beforeBig := bigAllocs.Load()
	beforeSmall := smallAllocs.Load()
	for i := 0; i < iters; i++ {
		c.appendInbound([]byte("ECHO " + bigArg + "\r\n"))
		s.OnTraffic(c)
	}
	afterBig := bigAllocs.Load()
	afterSmall := smallAllocs.Load()

	// If dropped requests are not freed back to the pool, we'd allocate on almost every iteration.
	// With Free(), we should see only a small constant number of allocations.
	if got := int(afterBig - beforeBig); got > 10 {
		t.Fatalf("too many big-page allocations while dropping requests: got %d, want <= 10", got)
	}
	if got := int(afterSmall - beforeSmall); got > 10 {
		t.Fatalf("too many small-page allocations while dropping requests: got %d, want <= 10", got)
	}

	close(block)
}

func TestOnTraffic_DropMultiple_ReturnsOrderedErrors(t *testing.T) {
	const N = 7

	pool, err := NewSmartPool(1, 1)
	if err != nil {
		t.Fatalf("NewSmartPool: %v", err)
	}
	s := newServer()
	s.workers = pool
	s.handler = func(conn Conn, cmd *Request, res *Respond) {
		res.WriteString("PONG")
	}

	c := newMinimalGnetConn()
	c_ := &conn{conn: c, rd: NewReader(c)}
	c.SetContext(c_)
	id := c.RemoteAddr().String()

	// Block worker so we can fill the queue to capacity.
	started := make(chan struct{})
	block := make(chan struct{})
	if err := s.workers.Submit(id, nil, func(ctx context.Context) {
		close(started)
		<-block
	}); err != nil {
		t.Fatalf("submit blocker: %v", err)
	}
	<-started

	// Fill queue to capacity (128) with tasks that will complete once unblocked.
	var fillWG sync.WaitGroup
	fillWG.Add(128)
	for i := 0; i < 128; i++ {
		if err := s.workers.Submit(id, nil, func(context.Context) { fillWG.Done() }); err != nil {
			t.Fatalf("fill submit %d: %v", i, err)
		}
	}

	// Make N requests be dropped (Submit queue full).
	for i := 0; i < N; i++ {
		c.appendInbound([]byte("PING\r\n"))
		s.OnTraffic(c)
	}

	// Unblock and wait for the 128 fill tasks to complete.
	close(block)
	drainDone := make(chan struct{})
	go func() {
		fillWG.Wait()
		close(drainDone)
	}()
	select {
	case <-drainDone:
	case <-time.After(2 * time.Second):
		t.Fatalf("timeout waiting for queue drain")
	}

	// With SmartPool flushDrops callback, drops should be flushed as soon as the queue drains,
	// without waiting for a follow-up request.
	deadline := time.Now().Add(2 * time.Second)
	for {
		frames := c.getFrames()
		if len(frames) > 0 {
			got := bytes.Join(frames, nil)
			want := bytes.Repeat(ErrQueueOverflow, N)
			if len(got) >= len(want) {
				if !bytes.Equal(got, want) {
					t.Fatalf("expected overflow bytes=%q, got %q", string(want), string(got))
				}
				break
			}
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("timeout waiting for %d overflow frames, got %d", N, len(frames))
		}
		time.Sleep(5 * time.Millisecond)
	}

	// Next accepted request should get its normal response only.
	c.resetFrames()
	done := make(chan struct{})
	s.after = func(conn Conn, cmd *Request, res *Respond) {
		select {
		case <-done:
		default:
			close(done)
		}
	}
	c.appendInbound([]byte("PING\r\n"))
	s.OnTraffic(c)
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatalf("timeout waiting for response")
	}
	frames := c.getFrames()
	if len(frames) != 1 {
		t.Fatalf("expected 1 frame, got %d", len(frames))
	}
	if string(frames[0]) != "+PONG\r\n" {
		t.Fatalf("expected %q, got %q", "+PONG\r\n", string(frames[0]))
	}
}
