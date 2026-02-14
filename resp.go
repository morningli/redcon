package redcon

import (
	"errors"
	"fmt"
	"reflect"
	"sort"
	"strconv"
	"strings"
)

// Type of RESP
type Type byte

// Various RESP kinds
const (
	Integer Type = ':'
	String  Type = '+'
	Bulk    Type = '$'
	Array   Type = '*'
	Error   Type = '-'
)

// GetType 返回 b 首字节对应的 RESP 类型标记；b 为空时返回 0。
func GetType(b *Buffer) Type {
	if b.Len() == 0 {
		return 0
	}
	return Type(b.At(0))
}

// GetArrayLength 返回 Array RESP 的元素数量；对非 Array 类型返回 1。
func GetArrayLength(b *Buffer) (int, error) {
	if GetType(b) != Array {
		return 1, nil
	}
	for i := 1; i < b.Len(); i++ {
		if b.At(i) == '\n' && b.At(i-1) == '\r' {
			return strconv.Atoi(string(b.Slice(1, i-1).Bytes()))
		}
	}
	return 0, errors.New("unexpected EOF")
}

// RESP 表示一次解析后的 RESP 消息结构。
type RESP struct {
	Type  Type
	Raw   *BufferView
	Data  *BufferView
	Array []RESP
	Count int
}

// ForEach iterates over each Array element
func (r RESP) ForEach(iter func(resp RESP) bool) {
	data := r.Data
	for i := 0; i < r.Count; i++ {
		n, resp := ReadNextRESP(data)
		if !iter(resp) {
			return
		}
		data = data.Slice(n, data.Len())
	}
}

// Bytes 返回 RESP 数据部分（Data）的字节内容（会发生拷贝）。
func (r RESP) Bytes() []byte {
	return r.Data.Bytes()
}

// String 将 RESP 数据部分（Data）转换为 string。
func (r RESP) String() string {
	return string(r.Data.Bytes())
}

// Int 将 RESP 数据部分按十进制解析为 int64（解析失败返回 0）。
func (r RESP) Int() int64 {
	x, _ := strconv.ParseInt(r.String(), 10, 64)
	return x
}

// Float 将 RESP 数据部分解析为 float64（解析失败返回 0）。
func (r RESP) Float() float64 {
	x, _ := strconv.ParseFloat(r.String(), 10)
	return x
}

// Map returns a key/value map of an Array.
// The receiver RESP must be an Array with an equal number of values, where
// the value of the key is followed by the key.
// Example: key1,value1,key2,value2,key3,value3
func (r RESP) Map() map[string]RESP {
	if r.Type != Array {
		return nil
	}
	var n int
	var key string
	m := make(map[string]RESP)
	r.ForEach(func(resp RESP) bool {
		if n&1 == 0 {
			key = resp.String()
		} else {
			m[key] = resp
		}
		n++
		return true
	})
	return m
}

// MapGet 从 key/value 形式的 Array 中按 key 获取对应的值。
func (r RESP) MapGet(key string) RESP {
	if r.Type != Array {
		return RESP{}
	}
	var val RESP
	var n int
	var ok bool
	r.ForEach(func(resp RESP) bool {
		if n&1 == 0 {
			ok = resp.String() == key
		} else if ok {
			val = resp
			return false
		}
		n++
		return true
	})
	return val
}

// Exists 判断 r 是否为有效 RESP（Type != 0）。
func (r RESP) Exists() bool {
	return r.Type != 0
}

// ReadNextRESP returns the next resp in b and returns the number of bytes the
// took up the result.
func ReadNextRESP(b *BufferView) (n int, resp RESP) {
	if b.Len() == 0 {
		return 0, RESP{} // no data to read
	}
	resp.Type = Type(b.At(0))
	switch resp.Type {
	case Integer, String, Bulk, Array, Error:
	default:
		return 0, RESP{} // invalid kind
	}
	// read to end of line
	i := 1
	for ; ; i++ {
		if i == b.Len() {
			return 0, RESP{} // not enough data
		}
		if b.At(i) == '\n' {
			if b.At(i-1) != '\r' {
				return 0, RESP{} //, missing CR character
			}
			i++
			break
		}
	}
	resp.Raw = b.Slice(0, i)
	resp.Data = b.Slice(1, i-2)
	if resp.Type == Integer {
		// Integer
		if resp.Data.Len() == 0 {
			return 0, RESP{} //, invalid integer
		}
		var j int
		if resp.Data.At(0) == '-' {
			if resp.Data.Len() == 1 {
				return 0, RESP{} //, invalid integer
			}
			j++
		}
		for ; j < resp.Data.Len(); j++ {
			if resp.Data.At(j) < '0' || resp.Data.At(j) > '9' {
				return 0, RESP{} // invalid integer
			}
		}
		return resp.Raw.Len(), resp
	}
	if resp.Type == String || resp.Type == Error {
		// String, Error
		return resp.Raw.Len(), resp
	}
	var err error
	resp.Count, err = strconv.Atoi(string(resp.Data.Bytes()))
	if resp.Type == Bulk {
		// Bulk
		if err != nil {
			return 0, RESP{} // invalid number of bytes
		}
		if resp.Count < 0 {
			resp.Data = nil
			resp.Count = 0
			return resp.Raw.Len(), resp
		}
		if b.Len() < i+resp.Count+2 {
			return 0, RESP{} // not enough data
		}
		if b.At(i+resp.Count) != '\r' || b.At(i+resp.Count+1) != '\n' {
			return 0, RESP{} // invalid end of line
		}
		resp.Data = b.Slice(i, i+resp.Count)
		resp.Raw = b.Slice(0, i+resp.Count+2)
		resp.Count = 0
		return resp.Raw.Len(), resp
	}
	// Array
	if err != nil {
		return 0, RESP{} // invalid number of elements
	}
	var tn int
	sdata := b.Slice(i, b.Len())
	for j := 0; j < resp.Count; j++ {
		rn, rresp := ReadNextRESP(sdata)
		if rresp.Type == 0 {
			return 0, RESP{}
		}
		tn += rn
		sdata = sdata.Slice(rn, sdata.Len())
		resp.Array = append(resp.Array, rresp)
	}
	resp.Data = b.Slice(i, i+tn)
	resp.Raw = b.Slice(0, i+tn)
	return resp.Raw.Len(), resp
}

// Kind is the kind of command
type Kind int

const (
	// Redis is returned for Redis protocol commands
	Redis Kind = iota
	// Tile38 is returnd for Tile38 native protocol commands
	Tile38
	// Telnet is returnd for plain telnet commands
	Telnet
)

var errInvalidMessage = &errProtocol{"invalid message"}

// ReadNextCommand reads the next command from the provided packet. It's
// possible that the packet contains multiple commands, or zero commands
// when the packet is incomplete.
// 'argsbuf' is an optional reusable buffer and it can be nil.
// 'complete' indicates that a command was read. false means no more commands.
// 'args' are the output arguments for the command.
// 'kind' is the type of command that was read.
// 'leftover' is any remaining unused bytes which belong to the next command.
// 'err' is returned when a protocol error was encountered.
func ReadNextCommand(packet []byte, argsbuf [][]byte) (
	complete bool, args [][]byte, kind Kind, leftover []byte, err error,
) {
	args = argsbuf[:0]
	if len(packet) > 0 {
		if packet[0] != '*' {
			if packet[0] == '$' {
				return readTile38Command(packet, args)
			}
			return readTelnetCommand(packet, args)
		}
		// standard redis command
		for s, i := 1, 1; i < len(packet); i++ {
			if packet[i] == '\n' {
				if packet[i-1] != '\r' {
					return false, args[:0], Redis, packet, errInvalidMultiBulkLength
				}
				count, ok := parseInt(packet[s : i-1])
				if !ok || count < 0 {
					return false, args[:0], Redis, packet, errInvalidMultiBulkLength
				}
				i++
				if count == 0 {
					return true, args[:0], Redis, packet[i:], nil
				}
			nextArg:
				for j := 0; j < count; j++ {
					if i == len(packet) {
						break
					}
					if packet[i] != '$' {
						return false, args[:0], Redis, packet,
							&errProtocol{"expected '$', got '" +
								string(packet[i]) + "'"}
					}
					for s := i + 1; i < len(packet); i++ {
						if packet[i] == '\n' {
							if packet[i-1] != '\r' {
								return false, args[:0], Redis, packet, errInvalidBulkLength
							}
							n, ok := parseInt(packet[s : i-1])
							if !ok || count <= 0 {
								return false, args[:0], Redis, packet, errInvalidBulkLength
							}
							i++
							if len(packet)-i >= n+2 {
								if packet[i+n] != '\r' || packet[i+n+1] != '\n' {
									return false, args[:0], Redis, packet, errInvalidBulkLength
								}
								args = append(args, packet[i:i+n])
								i += n + 2
								if j == count-1 {
									// done reading
									return true, args, Redis, packet[i:], nil
								}
								continue nextArg
							}
							break
						}
					}
					break
				}
				break
			}
		}
	}
	return false, args[:0], Redis, packet, nil
}

func readTile38Command(packet []byte, argsbuf [][]byte) (
	complete bool, args [][]byte, kind Kind, leftover []byte, err error,
) {
	for i := 1; i < len(packet); i++ {
		if packet[i] == ' ' {
			n, ok := parseInt(packet[1:i])
			if !ok || n < 0 {
				return false, args[:0], Tile38, packet, errInvalidMessage
			}
			i++
			if len(packet) >= i+n+2 {
				if packet[i+n] != '\r' || packet[i+n+1] != '\n' {
					return false, args[:0], Tile38, packet, errInvalidMessage
				}
				line := packet[i : i+n]
			reading:
				for len(line) != 0 {
					if line[0] == '{' {
						// The native protocol cannot understand json boundaries so it assumes that
						// a json element must be at the end of the line.
						args = append(args, line)
						break
					}
					if line[0] == '"' && line[len(line)-1] == '"' {
						if len(args) > 0 &&
							strings.ToLower(string(args[0])) == "set" &&
							strings.ToLower(string(args[len(args)-1])) == "string" {
							// Setting a string value that is contained inside double quotes.
							// This is only because of the boundary issues of the native protocol.
							args = append(args, line[1:len(line)-1])
							break
						}
					}
					i := 0
					for ; i < len(line); i++ {
						if line[i] == ' ' {
							value := line[:i]
							if len(value) > 0 {
								args = append(args, value)
							}
							line = line[i+1:]
							continue reading
						}
					}
					args = append(args, line)
					break
				}
				return true, args, Tile38, packet[i+n+2:], nil
			}
			break
		}
	}
	return false, args[:0], Tile38, packet, nil
}
func readTelnetCommand(packet []byte, argsbuf [][]byte) (
	complete bool, args [][]byte, kind Kind, leftover []byte, err error,
) {
	// just a plain text command
	for i := 0; i < len(packet); i++ {
		if packet[i] == '\n' {
			var line []byte
			if i > 0 && packet[i-1] == '\r' {
				line = packet[:i-1]
			} else {
				line = packet[:i]
			}
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
								return false, args[:0], Telnet, packet, errUnbalancedQuotes
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
								return false, args[:0], Telnet, packet, errUnbalancedQuotes
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
					return false, args[:0], Telnet, packet, errUnbalancedQuotes
				}
				if len(line) > 0 {
					args = append(args, line)
				}
				break
			}
			return true, args, Telnet, packet[i+1:], nil
		}
	}
	return false, args[:0], Telnet, packet, nil
}

// appendPrefix will append a "$3\r\n" style redis prefix for a message.
func appendPrefix(b *Buffer, c byte, n int64) {
	if n >= 0 && n <= 9 {
		_, _ = b.Write([]byte{c, byte('0' + n), '\r', '\n'})
		return
	}
	_, _ = b.Write([]byte{c})
	_, _ = b.Write(strconv.AppendInt(nil, n, 10))
	_, _ = b.Write([]byte{'\r', '\n'})
}

// AppendUint appends a Redis protocol uint64 to the input bytes.
func AppendUint(b *Buffer, n uint64) RESP {
	start := b.Len()
	_, _ = b.Write([]byte{':'})
	_, _ = b.Write(strconv.AppendUint(nil, n, 10))
	_, _ = b.Write([]byte{'\r', '\n'})
	end := b.Len()
	return RESP{
		Type: Integer,
		Raw:  b.Slice(start, end),
		Data: b.Slice(start+1, end-2), // 排除 ':' 和 '\r\n'
	}
}

// AppendInt appends a Redis protocol int64 to the input bytes.
func AppendInt(b *Buffer, n int64) RESP {
	start := b.Len()
	appendPrefix(b, ':', n)
	end := b.Len()
	return RESP{
		Type: Integer,
		Raw:  b.Slice(start, end),
		Data: b.Slice(start+1, end-2), // 排除 ':' 和 '\r\n'
	}
}

// AppendArray appends a Redis protocol array to the input bytes.
func AppendArray(b *Buffer, n int) RESP {
	start := b.Len()
	appendPrefix(b, '*', int64(n))
	end := b.Len()
	return RESP{
		Type:  Array,
		Count: n,
		Raw:   b.Slice(start, end),
		Array: make([]RESP, 0, n), // 预分配子元素空间
	}
}

// AppendBulk appends a Redis protocol bulk byte slice to the input bytes.
func AppendBulk(b *Buffer, bulk []byte) RESP {
	start := b.Len()
	appendPrefix(b, '$', int64(len(bulk)))
	headerEnd := b.Len()

	_, _ = b.Write(bulk)
	dataEnd := b.Len()
	_, _ = b.Write([]byte{'\r', '\n'})
	end := b.Len()

	return RESP{
		Type:  Bulk,
		Count: len(bulk),
		Raw:   b.Slice(start, end),
		Data:  b.Slice(headerEnd, dataEnd), // 仅指向数据主体
	}
}

// AppendBulkString appends a Redis protocol bulk string to the input bytes.
func AppendBulkString(b *Buffer, bulk string) RESP {
	return AppendBulk(b, []byte(bulk))
}

// AppendString appends a Redis protocol string to the input bytes.
func AppendString(b *Buffer, s string) RESP {
	// 1. 记录起始物理位移
	start := b.Len()

	// 2. 执行物理写入 (利用内存池 Buffer)
	_, _ = b.Write([]byte{'+'})
	_, _ = b.Write([]byte(stripNewlines(s)))
	_, _ = b.Write([]byte{'\r', '\n'})

	// 3. 获取当前总长度
	end := b.Len()

	// 4. 返回逻辑结构
	return RESP{
		Type: String, // 即 '+'
		Raw:  b.Slice(start, end),
		Data: b.Slice(start+1, end-2), // 排除前缀 '+' 和末尾 '\r\n'
	}
}

// AppendError appends a Redis protocol error to the input bytes.
func AppendError(b *Buffer, s string) RESP {
	start := b.Len()
	_, _ = b.Write([]byte{'-'})
	_, _ = b.Write([]byte(stripNewlines(s)))
	_, _ = b.Write([]byte{'\r', '\n'})
	end := b.Len()
	return RESP{
		Type: Error,
		Raw:  b.Slice(start, end),
		Data: b.Slice(start+1, end-2),
	}
}

// AppendOK appends a Redis protocol OK to the input bytes.
func AppendOK(b *Buffer) {
	_, _ = b.Write([]byte{'+', 'O', 'K', '\r', '\n'})
}

func stripNewlines(s string) string {
	for i := 0; i < len(s); i++ {
		if s[i] == '\r' || s[i] == '\n' {
			s = strings.Replace(s, "\r", " ", -1)
			s = strings.Replace(s, "\n", " ", -1)
			break
		}
	}
	return s
}

// AppendTile38 appends a Tile38 message to the input bytes.
func AppendTile38(b Buffer, data []byte) {
	_, _ = b.Write([]byte{'$'})
	_, _ = b.Write(strconv.AppendInt(nil, int64(len(data)), 10))
	_, _ = b.Write([]byte{' '})
	_, _ = b.Write(data)
	_, _ = b.Write([]byte{'\r', '\n'})
}

// AppendNull appends a Redis protocol null to the input bytes.
func AppendNull(b *Buffer) RESP {
	start := b.Len()
	_, _ = b.Write([]byte{'$', '-', '1', '\r', '\n'})
	end := b.Len()
	return RESP{
		Type:  Bulk,
		Count: -1,
		Raw:   b.Slice(start, end),
	}
}

// AppendBulkFloat appends a float64, as bulk bytes.
func AppendBulkFloat(b *Buffer, f float64) RESP {
	return AppendBulk(b, strconv.AppendFloat(nil, f, 'f', -1, 64))
}

// AppendBulkInt appends an int64, as bulk bytes.
func AppendBulkInt(b *Buffer, x int64) RESP {
	return AppendBulk(b, strconv.AppendInt(nil, x, 10))
}

// AppendBulkUint appends an uint64, as bulk bytes.
func AppendBulkUint(b *Buffer, x uint64) RESP {
	return AppendBulk(b, strconv.AppendUint(nil, x, 10))
}

func prefixERRIfNeeded(msg string) string {
	msg = strings.TrimSpace(msg)
	firstWord := strings.Split(msg, " ")[0]
	addERR := len(firstWord) == 0
	for i := 0; i < len(firstWord); i++ {
		if firstWord[i] < 'A' || firstWord[i] > 'Z' {
			addERR = true
			break
		}
	}
	if addERR {
		msg = strings.TrimSpace("ERR " + msg)
	}
	return msg
}

// SimpleString is for representing a non-bulk representation of a string
// from an *Any call.
type SimpleString string

// SimpleInt is for representing a non-bulk representation of a int
// from an *Any call.
type SimpleInt int

// SimpleError is for representing an error without adding the "ERR" prefix
// from an *Any call.
type SimpleError error

// Marshaler is the interface implemented by types that
// can marshal themselves into a Redis response type from an *Any call.
// The return value is not check for validity.
type Marshaler interface {
	MarshalRESP() []byte
}

// AppendAny appends any type to valid Redis type.
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
//	Marshaler       -> raw bytes
//	everything-else -> bulk-string representation using fmt.Sprint()
func AppendAny(b *Buffer, v interface{}) RESP {
	switch v := v.(type) {
	case SimpleString:
		return AppendString(b, string(v))
	case SimpleInt:
		return AppendInt(b, int64(v))
	case SimpleError:
		return AppendError(b, v.Error())
	case nil:
		return AppendNull(b)
	case error:
		return AppendError(b, prefixERRIfNeeded(v.Error()))
	case string:
		return AppendBulkString(b, v)
	case []byte:
		if v == nil {
			return AppendNull(b)
		} else {
			return AppendBulk(b, v)
		}
	case bool:
		if v {
			return AppendBulkString(b, "1")
		} else {
			return AppendBulkString(b, "0")
		}
	case int:
		return AppendBulkInt(b, int64(v))
	case int8:
		return AppendBulkInt(b, int64(v))
	case int16:
		return AppendBulkInt(b, int64(v))
	case int32:
		return AppendBulkInt(b, int64(v))
	case int64:
		return AppendBulkInt(b, int64(v))
	case uint:
		return AppendBulkUint(b, uint64(v))
	case uint8:
		return AppendBulkUint(b, uint64(v))
	case uint16:
		return AppendBulkUint(b, uint64(v))
	case uint32:
		return AppendBulkUint(b, uint64(v))
	case uint64:
		return AppendBulkUint(b, uint64(v))
	case float32:
		return AppendBulkFloat(b, float64(v))
	case float64:
		return AppendBulkFloat(b, float64(v))
	case Marshaler:
		start := b.Len()
		data := v.MarshalRESP()
		_, _ = b.Write(data)
		end := b.Len()

		// 获取写入数据的视图
		view := b.Slice(start, end)
		// 调用原型：func ReadNextRESP(b *BufferView) (n int, resp RESP)
		// 这样可以准确识别 Marshaler 产生的 RESP 类型 (如 Array 或 Bulk)
		n, resp := ReadNextRESP(view)
		if resp.Type == 0 || n == 0 {
			// 解析失败降级处理：视为原始 Bulk 或错误
			return RESP{Type: Type(data[0]), Raw: view, Data: view}
		}
		return resp
	default:
		vv := reflect.ValueOf(v)
		switch vv.Kind() {
		case reflect.Slice:
			n := vv.Len()
			start := b.Len()
			headerResp := AppendArray(b, n)
			childResps := make([]RESP, 0, n)
			for i := 0; i < n; i++ {
				childResps = append(childResps, AppendAny(b, vv.Index(i).Interface()))
			}
			end := b.Len()
			return RESP{
				Type:  Array,
				Count: n,
				Raw:   b.Slice(start, end),
				Data:  b.Slice(headerResp.Raw.Len(), end), // Data 为 header 之后的部分
				Array: childResps,
			}
		case reflect.Map:
			n := vv.Len()
			start := b.Len()
			headerResp := AppendArray(b, n*2)
			childResps := make([]RESP, 0, n*2)

			var i int
			var strKey bool
			var strsKeyItems []strKeyItem

			iter := vv.MapRange()
			for iter.Next() {
				key := iter.Key().Interface()
				if i == 0 {
					if _, ok := key.(string); ok {
						strKey = true
						strsKeyItems = make([]strKeyItem, n)
					}
				}
				if strKey {
					strsKeyItems[i] = strKeyItem{
						key.(string), iter.Value().Interface(),
					}
				} else {
					childResps = append(childResps, AppendAny(b, key))
					childResps = append(childResps, AppendAny(b, iter.Value().Interface()))
				}
				i++
			}
			if strKey {
				sort.Slice(strsKeyItems, func(i, j int) bool {
					return strsKeyItems[i].key < strsKeyItems[j].key
				})
				for _, item := range strsKeyItems {
					childResps = append(childResps, AppendBulkString(b, item.key))
					childResps = append(childResps, AppendAny(b, item.value))
				}
				end := b.Len()
				return RESP{
					Type:  Array,
					Count: n * 2,
					Raw:   b.Slice(start, end),
					Data:  b.Slice(headerResp.Raw.Len(), end),
					Array: childResps,
				}
			}
		default:
			return AppendBulkString(b, fmt.Sprint(v))
		}
	}
	return RESP{}
}

type strKeyItem struct {
	key   string
	value interface{}
}
