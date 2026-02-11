package redcon

import (
	"testing"
)

func TestAppendBulkFloat(t *testing.T) {
	var b = NewBuffer()
	AppendString(b, "HELLO")
	AppendBulkFloat(b, 9.123192839)
	AppendString(b, "HELLO")
	exp := "+HELLO\r\n$11\r\n9.123192839\r\n+HELLO\r\n"
	if string(b.Bytes()) != exp {
		t.Fatalf("expected '%s', got '%s'", exp, b.Bytes())
	}
}

func TestAppendBulkInt(t *testing.T) {
	var b = NewBuffer()
	AppendString(b, "HELLO")
	AppendBulkInt(b, -9182739137)
	AppendString(b, "HELLO")
	exp := "+HELLO\r\n$11\r\n-9182739137\r\n+HELLO\r\n"
	if string(b.Bytes()) != exp {
		t.Fatalf("expected '%s', got '%s'", exp, b.Bytes())
	}
}

func TestAppendBulkUint(t *testing.T) {
	var b = NewBuffer()
	AppendString(b, "HELLO")
	AppendBulkInt(b, 91827391370)
	AppendString(b, "HELLO")
	exp := "+HELLO\r\n$11\r\n91827391370\r\n+HELLO\r\n"
	if string(b.Bytes()) != exp {
		t.Fatalf("expected '%s', got '%s'", exp, b.Bytes())
	}
}

func TestAppendArray(t *testing.T) {
	var b = NewBuffer()
	AppendArray(b, 1)
	AppendBulk(b, []byte("HELLO"))
	exp := "*1\r\n$5\r\nHELLO\r\n"
	if string(b.Bytes()) != exp {
		t.Fatalf("expected '%s', got '%s'", exp, b.Bytes())
	}
}
