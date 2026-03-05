package redcon

import "errors"

var (
	ErrInvalidLength   = errors.New("redis: ERR invalid length") // 预定义错误，消除 heap 逃逸
	ErrInvalidRespType = errors.New("redis: ERR invalid resp type")
	ErrUnexpectedEOF   = errors.New("unexpected EOF")
)
