package redcon

import (
	"bufio"
	"io"
)

// Reader represent a reader for RESP or telnet commands.
type Reader struct {
	rd   *bufio.Reader
	buf  *Buffer
	cmds []*Request
}

// NewReader returns a command reader which will read RESP or telnet commands.
func NewReader(rd io.Reader) *Reader {
	return &Reader{
		rd:  bufio.NewReaderSize(rd, 32<<10),
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

func (rd *Reader) readCommands(leftover *int) ([]*Request, error) {
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
						// convert this to resp command syntax
						var cmd = NewRequest()
						cmd.WriteArray(len(args))
						for i := range args {
							cmd.WriteBulk(args[i])
						}
						cmds = append(cmds, cmd)
					}
					b.Discard(i + 1)
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
						var cmd = NewRequest()
						b.ShiftTo(i+1, cmd.Raw)
						cmd.Args = make([]BufferView, len(marks)/2)
						// slice up the raw command into the args based on
						// the recorded marks.
						for h := 0; h < len(marks); h += 2 {
							cmd.Args[h/2] = cmd.Raw.Slice(marks[h], marks[h+1])
						}
						cmds = append(cmds, cmd)
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
	if leftover != nil {
		*leftover = b.Len()
	}
	if len(cmds) > 0 {
		return cmds, nil
	}
	if rd.rd == nil {
		return nil, errIncompleteCommand
	}
	_, err := rd.buf.ReadFrom(rd.rd)
	if err != nil {
		return nil, err
	}
	return rd.readCommands(leftover)
}

// ReadCommands reads the next pipeline commands.
func (rd *Reader) ReadCommands() ([]*Request, error) {
	for {
		if len(rd.cmds) > 0 {
			cmds := rd.cmds
			rd.cmds = nil
			return cmds, nil
		}
		cmds, err := rd.readCommands(nil)
		if err != nil {
			return nil, err
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
	cmds, err := rd.readCommands(nil)
	if err != nil {
		return nil, err
	}
	rd.cmds = cmds
	return rd.ReadCommand()
}
