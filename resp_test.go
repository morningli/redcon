package redcon

import (
	"bytes"
	"testing"
)

func TestAppendBulkFloat(t *testing.T) {
	var buf = NewBuffer()
	b := buf.NewWriter()

	AppendString(b, "HELLO")
	AppendBulkFloat(b, 9.123192839)
	AppendString(b, "HELLO")
	exp := "+HELLO\r\n$11\r\n9.123192839\r\n+HELLO\r\n"
	if string(buf.Bytes()) != exp {
		t.Fatalf("expected '%s', got '%s'", exp, buf.Bytes())
	}
}

func TestAppendBulkInt(t *testing.T) {
	var buf = NewBuffer()
	b := buf.NewWriter()

	AppendString(b, "HELLO")
	AppendBulkInt(b, -9182739137)
	AppendString(b, "HELLO")
	exp := "+HELLO\r\n$11\r\n-9182739137\r\n+HELLO\r\n"
	if string(buf.Bytes()) != exp {
		t.Fatalf("expected '%s', got '%s'", exp, buf.Bytes())
	}
}

func TestAppendBulkUint(t *testing.T) {
	var buf = NewBuffer()
	b := buf.NewWriter()

	AppendString(b, "HELLO")
	AppendBulkInt(b, 91827391370)
	AppendString(b, "HELLO")
	exp := "+HELLO\r\n$11\r\n91827391370\r\n+HELLO\r\n"
	if string(buf.Bytes()) != exp {
		t.Fatalf("expected '%s', got '%s'", exp, buf.Bytes())
	}
}

func TestAppendArray(t *testing.T) {
	var buf = NewBuffer()
	b := buf.NewWriter()

	AppendArray(b, 1)
	AppendBulk(b, []byte("HELLO"))
	exp := "*1\r\n$5\r\nHELLO\r\n"
	if string(buf.Bytes()) != exp {
		t.Fatalf("expected '%s', got '%s'", exp, buf.Bytes())
	}
}

func TestAppendNullArray(t *testing.T) {
	var buf = NewBuffer()
	b := buf.NewWriter()

	AppendNullArray(b)
	if string(buf.Bytes()) != "*-1\r\n" {
		t.Fatalf("expected %q, got %q", "*-1\r\n", string(buf.Bytes()))
	}
}

func TestReadNextRESP_AllTypesAndStructures(t *testing.T) {
	tests := []struct {
		name string
		raw  string
		typ  Type
		// Optional expectations
		data      string
		dataIsNil bool
		count     *int
	}{
		{name: "simple_string", raw: "+OK\r\n", typ: String, data: "OK"},
		{name: "simple_string_empty", raw: "+\r\n", typ: String, data: ""},
		{name: "error", raw: "-ERR bad stuff\r\n", typ: Error, data: "ERR bad stuff"},
		{name: "integer_pos", raw: ":123\r\n", typ: Integer, data: "123"},
		{name: "integer_neg", raw: ":-1\r\n", typ: Integer, data: "-1"},
		{name: "bulk", raw: "$3\r\nfoo\r\n", typ: Bulk, data: "foo"},
		{name: "bulk_empty", raw: "$0\r\n\r\n", typ: Bulk, data: ""},
		{name: "bulk_null", raw: "$-1\r\n", typ: Bulk, dataIsNil: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b := NewBuffer()
			wr := b.NewWriter()
			_, _ = wr.Write([]byte(tt.raw))
			view := b.Slice(0, b.Len())

			n, resp := ReadNextRESP(view)
			if n != view.Len() {
				t.Fatalf("expected n=%d, got %d", view.Len(), n)
			}
			if resp.Type != tt.typ {
				t.Fatalf("expected Type=%v, got %v", tt.typ, resp.Type)
			}
			if string(resp.Raw.Bytes()) != tt.raw {
				t.Fatalf("expected Raw=%q, got %q", tt.raw, string(resp.Raw.Bytes()))
			}
			if tt.dataIsNil {
				if !resp.Null {
					t.Fatalf("expected Data=nil, got %q", string(resp.Data.Bytes()))
				}
			} else {
				if resp.Null {
					t.Fatalf("expected Data non-nil")
				}
				if string(resp.Data.Bytes()) != tt.data {
					t.Fatalf("expected Data=%q, got %q", tt.data, string(resp.Data.Bytes()))
				}
			}
			if tt.count != nil && resp.Count != *tt.count {
				t.Fatalf("expected Count=%d, got %d", *tt.count, resp.Count)
			}
		})
	}
}

func TestReadNextRESP_ArrayVariantsAndNested(t *testing.T) {
	t.Run("array_empty", func(t *testing.T) {
		raw := "*0\r\n"
		var buf = NewBuffer()
		b := buf.NewWriter()

		_, _ = b.Write([]byte(raw))
		view := buf.Slice(0, buf.Len())

		n, resp := ReadNextRESP(view)
		if n != view.Len() {
			t.Fatalf("expected n=%d, got %d", view.Len(), n)
		}
		if resp.Type != Array {
			t.Fatalf("expected Type=Array, got %v", resp.Type)
		}
		if resp.Count != 0 {
			t.Fatalf("expected Count=0, got %d", resp.Count)
		}
		if string(resp.Raw.Bytes()) != raw {
			t.Fatalf("expected Raw=%q, got %q", raw, string(resp.Raw.Bytes()))
		}
		if len(resp.Array) != 0 {
			t.Fatalf("expected Array len=0, got %d", len(resp.Array))
		}
	})

	t.Run("array_null", func(t *testing.T) {
		raw := "*-1\r\n"
		var buf = NewBuffer()
		b := buf.NewWriter()

		_, _ = b.Write([]byte(raw))
		view := buf.Slice(0, buf.Len())

		n, resp := ReadNextRESP(view)
		if n != view.Len() {
			t.Fatalf("expected n=%d, got %d", view.Len(), n)
		}
		if resp.Type != Array {
			t.Fatalf("expected Type=Array, got %v", resp.Type)
		}
		if resp.Count != -1 {
			t.Fatalf("expected Count=-1, got %d", resp.Count)
		}
		if string(resp.Raw.Bytes()) != raw {
			t.Fatalf("expected Raw=%q, got %q", raw, string(resp.Raw.Bytes()))
		}
		if len(resp.Array) != 0 {
			t.Fatalf("expected Array len=0, got %d", len(resp.Array))
		}
	})

	t.Run("array_mixed_nested", func(t *testing.T) {
		raw := "*4\r\n+OK\r\n:1\r\n$3\r\nbar\r\n*2\r\n:2\r\n$3\r\nbaz\r\n"
		var buf = NewBuffer()
		b := buf.NewWriter()

		_, _ = b.Write([]byte(raw))
		view := buf.Slice(0, buf.Len())

		n, resp := ReadNextRESP(view)
		if n != view.Len() {
			t.Fatalf("expected n=%d, got %d", view.Len(), n)
		}
		if resp.Type != Array || resp.Count != 4 || len(resp.Array) != 4 {
			t.Fatalf("expected Array count=4, got Type=%v Count=%d len=%d", resp.Type, resp.Count, len(resp.Array))
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
		if resp.Array[3].Type != Array || resp.Array[3].Count != 2 || len(resp.Array[3].Array) != 2 {
			t.Fatalf("expected [3]=Array(2), got Type=%v Count=%d len=%d", resp.Array[3].Type, resp.Array[3].Count, len(resp.Array[3].Array))
		}
		if resp.Array[3].Array[0].Type != Integer || resp.Array[3].Array[0].Int() != 2 {
			t.Fatalf("expected [3][0]=:2, got Type=%v Int=%d", resp.Array[3].Array[0].Type, resp.Array[3].Array[0].Int())
		}
		if resp.Array[3].Array[1].Type != Bulk || resp.Array[3].Array[1].String() != "baz" {
			t.Fatalf("expected [3][1]=$baz, got Type=%v String=%q", resp.Array[3].Array[1].Type, resp.Array[3].Array[1].String())
		}
		if string(resp.Raw.Bytes()) != raw {
			t.Fatalf("expected Raw=%q, got %q", raw, string(resp.Raw.Bytes()))
		}
	})
}

func TestRESP_MapAndMapGet(t *testing.T) {
	raw := "*4\r\n$3\r\nfoo\r\n$3\r\nbar\r\n$3\r\nbaz\r\n$3\r\nqux\r\n"
	var buf = NewBuffer()
	b := buf.NewWriter()

	_, _ = b.Write([]byte(raw))
	view := buf.Slice(0, buf.Len())

	_, resp := ReadNextRESP(view)
	if resp.Type != Array || resp.Count != 4 {
		t.Fatalf("expected Array(4), got Type=%v Count=%d", resp.Type, resp.Count)
	}

	m := resp.Map()
	if len(m) != 2 {
		t.Fatalf("expected map size 2, got %d", len(m))
	}
	if m["foo"].String() != "bar" {
		t.Fatalf("expected foo=bar, got %q", m["foo"].String())
	}
	if m["baz"].String() != "qux" {
		t.Fatalf("expected baz=qux, got %q", m["baz"].String())
	}

	if resp.MapGet("foo").String() != "bar" {
		t.Fatalf("expected MapGet(foo)=bar, got %q", resp.MapGet("foo").String())
	}
	if resp.MapGet("missing").Exists() {
		t.Fatalf("expected MapGet(missing) to be non-existent")
	}
}

func TestReadNextRESP_ConsumesOnlyOneMessage(t *testing.T) {
	raw := "+OK\r\n:1\r\n"
	var buf = NewBuffer()
	b := buf.NewWriter()
	_, _ = b.Write([]byte(raw))
	view := buf.Slice(0, buf.Len())

	n, resp := ReadNextRESP(view)
	if resp.Type != String || resp.String() != "OK" {
		t.Fatalf("expected first resp +OK, got Type=%v String=%q", resp.Type, resp.String())
	}
	left := view.Slice(n, view.Len())
	n2, resp2 := ReadNextRESP(left)
	if resp2.Type != Integer || resp2.Int() != 1 {
		t.Fatalf("expected second resp :1, got Type=%v Int=%d", resp2.Type, resp2.Int())
	}
	if n+n2 != view.Len() {
		t.Fatalf("expected to consume all bytes, got %d/%d", n+n2, view.Len())
	}
}

func TestReadNextRESP_WithBytesBufferView(t *testing.T) {
	// Ensure BufferView slicing works as expected when underlying data is contiguous.
	raw := "$5\r\nHELLO\r\n"
	buf := bytes.NewBufferString(raw)
	b := NewBuffer()
	wr := b.NewWriter()
	_, _ = wr.Write(buf.Bytes())
	view := b.Slice(0, b.Len())
	_, resp := ReadNextRESP(view)
	if resp.Type != Bulk || resp.String() != "HELLO" {
		t.Fatalf("expected bulk HELLO, got Type=%v String=%q", resp.Type, resp.String())
	}
}

func TestReadNextRESP_SimpleString_Structure(t *testing.T) {
	b := NewBuffer()
	wr := b.NewWriter()
	_, _ = wr.Write([]byte("+OK\r\n"))
	view := b.Slice(0, b.Len())

	n, resp := ReadNextRESP(view)
	if n != view.Len() {
		t.Fatalf("expected n=%d, got %d", view.Len(), n)
	}
	if resp.Type != String {
		t.Fatalf("expected Type=String, got %v", resp.Type)
	}
	if string(resp.Raw.Bytes()) != "+OK\r\n" {
		t.Fatalf("expected Raw=%q, got %q", "+OK\r\n", string(resp.Raw.Bytes()))
	}
	if string(resp.Data.Bytes()) != "OK" {
		t.Fatalf("expected Data=%q, got %q", "OK", string(resp.Data.Bytes()))
	}
	if resp.Count != 0 {
		t.Fatalf("expected Count=0, got %d", resp.Count)
	}
	if len(resp.Array) != 0 {
		t.Fatalf("expected Array empty, got len=%d", len(resp.Array))
	}
}

func TestReadNextRESP_Bulk_Structure(t *testing.T) {
	b := NewBuffer()
	wr := b.NewWriter()
	_, _ = wr.Write([]byte("$3\r\nfoo\r\n"))
	view := b.Slice(0, b.Len())

	n, resp := ReadNextRESP(view)
	if n != view.Len() {
		t.Fatalf("expected n=%d, got %d", view.Len(), n)
	}
	if resp.Type != Bulk {
		t.Fatalf("expected Type=Bulk, got %v", resp.Type)
	}
	if string(resp.Raw.Bytes()) != "$3\r\nfoo\r\n" {
		t.Fatalf("expected Raw=%q, got %q", "$3\r\nfoo\r\n", string(resp.Raw.Bytes()))
	}
	if string(resp.Data.Bytes()) != "foo" {
		t.Fatalf("expected Data=%q, got %q", "foo", string(resp.Data.Bytes()))
	}
}

func TestReadNextRESP_ArrayNested_Structure(t *testing.T) {
	raw := "*2\r\n*2\r\n:1\r\n:2\r\n$3\r\nbar\r\n"
	b := NewBuffer()
	wr := b.NewWriter()
	_, _ = wr.Write([]byte(raw))
	view := b.Slice(0, b.Len())

	n, resp := ReadNextRESP(view)
	if n != view.Len() {
		t.Fatalf("expected n=%d, got %d", view.Len(), n)
	}
	if resp.Type != Array {
		t.Fatalf("expected Type=Array, got %v", resp.Type)
	}
	if resp.Count != 2 {
		t.Fatalf("expected Count=2, got %d", resp.Count)
	}
	if string(resp.Raw.Bytes()) != raw {
		t.Fatalf("expected Raw=%q, got %q", raw, string(resp.Raw.Bytes()))
	}
	if len(resp.Array) != 2 {
		t.Fatalf("expected Array len=2, got %d", len(resp.Array))
	}

	inner := resp.Array[0]
	if inner.Type != Array || inner.Count != 2 || len(inner.Array) != 2 {
		t.Fatalf("expected inner Array count=2, got Type=%v Count=%d len=%d", inner.Type, inner.Count, len(inner.Array))
	}
	if inner.Array[0].Type != Integer || inner.Array[0].Int() != 1 {
		t.Fatalf("expected inner[0]=:1, got Type=%v Int=%d", inner.Array[0].Type, inner.Array[0].Int())
	}
	if inner.Array[1].Type != Integer || inner.Array[1].Int() != 2 {
		t.Fatalf("expected inner[1]=:2, got Type=%v Int=%d", inner.Array[1].Type, inner.Array[1].Int())
	}

	last := resp.Array[1]
	if last.Type != Bulk || last.String() != "bar" {
		t.Fatalf("expected last=$bar, got Type=%v String=%q", last.Type, last.String())
	}
}
