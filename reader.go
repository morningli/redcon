package redcon

import (
	"bufio"
	"io"
)

// Reader represent a reader for RESP or telnet commands.
type Reader struct {
	rd    *bufio.Reader
	buf   *Buffer
	wr    *BufferWriter
	cmds  []*Request
	marks []int //复用坐标切片，避免解析 Array 时申请内存
}

// NewReader returns a command reader which will read RESP or telnet commands.
func NewReader(rd io.Reader) *Reader {
	b := NewBuffer()
	return &Reader{
		rd:  bufio.NewReaderSize(rd, 32<<10),
		buf: b,
		wr:  b.NewWriter(),
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
	// 1. 快速路径：如果 Buffer 里已有数据，直接尝试解析 (Pipeline 场景)
	if rd.buf.Len() > 0 {
		cmds, err := rd.parseAvailable(leftover)
		// 如果解析出了至少一个命令，或者遇到了协议错误，直接返回
		if len(cmds) > 0 || err != nil {
			return cmds, err
		}
	}

	// 2. 慢速路径：Buffer 为空或数据不足，进入物理读取循环 (Slow Path)
	// 这里的调用将替代原有的递归逻辑
	return rd.readAndParseSlow(leftover)
}

//go:noinline
func (rd *Reader) readAndParseSlow(leftover *int) ([]*Request, error) {
	for {
		if rd.rd == nil {
			return nil, errIncompleteCommand
		}

		// 物理读取：ReadBuffered 内部已优化为不返回 (0, nil)
		n, err := rd.wr.CopyBufferedTo(rd.rd)
		if err != nil {
			return nil, err
		}
		if n == 0 {
			return nil, io.EOF
		}

		// 读到新数据后，调用解析器尝试提取命令
		cmds, err := rd.parseAvailable(leftover)
		if len(cmds) > 0 || err != nil {
			return cmds, err
		}
		// 数据依然不完整（半包），继续循环读取
	}
}

//go:noinline
func (rd *Reader) parseAvailable(leftover *int) ([]*Request, error) {
	rd.cmds = rd.cmds[:0] // 重置并复用命令切片
	b := rd.buf

next:
	if b.Len() == 0 {
		goto done
	}

	switch b.At(0) {
	case '*': // RESP Array 格式
		i := b.IndexByte('\n', 1)
		if i < 0 {
			goto done
		}

		count, err := b.Slice(1, i-1).ParseInt()
		if err != nil || count <= 0 {
			return nil, errInvalidMultiBulkLength
		}

		rd.marks = rd.marks[:0] // 重置并复用坐标切片
		curr := i + 1
		for j := 0; j < count; j++ {
			if curr >= b.Len() {
				goto done
			}
			if b.At(curr) != '$' {
				return nil, errInvalidBulkLength
			}

			si := curr
			bulkEnd := b.IndexByte('\n', curr+1)
			if bulkEnd < 0 {
				goto done
			}

			size, err := b.Slice(si+1, bulkEnd-1).ParseInt()
			if err != nil || size < 0 {
				return nil, errInvalidBulkLength
			}

			bodyEnd := bulkEnd + 1 + size + 2
			if bodyEnd > b.Len() {
				goto done
			}

			// 校验结尾 \r\n
			if b.At(bodyEnd-1) != '\n' || b.At(bodyEnd-2) != '\r' {
				return nil, errInvalidBulkLength
			}

			rd.marks = append(rd.marks, bulkEnd+1, bulkEnd+1+size)
			curr = bodyEnd
		}

		// 解析成功：打包 Request
		cmd := NewRequest()
		b.ShiftTo(curr, cmd.Raw) // 物理页引用转移
		rd.wr.Sync()
		cmd.Args = make([]BufferView, count)
		for h := 0; h < len(rd.marks); h += 2 {
			cmd.Args[h/2] = cmd.Raw.Slice(rd.marks[h], rd.marks[h+1])
		}
		rd.cmds = append(rd.cmds, cmd)
		if b.Len() > 0 {
			goto next
		}

	default:
		// 1. 利用优化的 IndexByte 寻找行尾 \n
		i := b.IndexByte('\n', 0)
		if i < 0 {
			goto done
		}

		// 2. 截取当前行视图 (逻辑切片，0 拷贝)
		var lineView BufferView
		if i > 0 && b.At(i-1) == '\r' {
			lineView = b.Slice(0, i-1)
		} else {
			lineView = b.Slice(0, i)
		}

		// 3. 执行状态机解析
		args, err := rd.parsePlainText(lineView)
		if err != nil {
			return nil, err
		}

		// 4. 将解析出的文本命令转换并暂存
		if len(args) > 0 {
			cmd := NewRequest()
			cmd.WriteArray(len(args))
			for j := range args {
				cmd.WriteBulk(args[j])
			}
			rd.cmds = append(rd.cmds, cmd)
		}

		// 5. 移除已解析的行并尝试解析下一条
		b.Discard(i + 1)
		rd.wr.Sync()
		if b.Len() > 0 {
			goto next
		}
		goto done

	}

done:
	if leftover != nil {
		*leftover = b.Len()
	}
	return rd.cmds, nil
}

// parsePlainText 解析文本协议命令。
// 虽然是不常用分支，但依然利用 BufferView 特性减少不必要的转换。
func (rd *Reader) parsePlainText(line BufferView) ([][]byte, error) {
	var args [][]byte
	length := line.Len()
	start := 0

	for start < length {
		// 1. 跳过前导空格
		for start < length && line.At(start) == ' ' {
			start++
		}
		if start >= length {
			break
		}

		// 2. 检查是否是带引号的参数
		c := line.At(start)
		if c == '"' || c == '\'' {
			// 复杂参数：处理引号和可能的转义
			arg, nextIdx, err := rd.parseComplexQuote(line, start)
			if err != nil {
				return nil, err
			}
			args = append(args, arg)
			start = nextIdx + 1
		} else {
			// 普通参数：寻找到下一个空格或行尾
			end := line.IndexByte(' ', start)
			if end < 0 {
				end = length
			}
			// 利用 Slice().Bytes() 获取原始数据。
			// 在你的 Buffer 特性下，如果不跨页，这是零拷贝。
			args = append(args, line.Slice(start, end).Bytes())
			start = end + 1
		}
	}

	return args, nil
}

// parseComplexQuote 处理带引号和转义的复杂参数
// 使用 go:noinline 确保它不占用主解析路径的内联配额
//
//go:noinline
func (rd *Reader) parseComplexQuote(line BufferView, start int) ([]byte, int, error) {
	quotech := line.At(start)
	length := line.Len()
	var res []byte
	escape := false

	for i := start + 1; i < length; i++ {
		c := line.At(i)
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
		} else {
			if c == quotech {
				// 规范：引号后必须是空格或行尾
				if i+1 < length && line.At(i+1) != ' ' {
					return nil, 0, errUnbalancedQuotes
				}
				return res, i, nil
			} else if c == '\\' {
				escape = true
				continue
			}
		}
		res = append(res, c)
	}
	return nil, 0, errUnbalancedQuotes
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
