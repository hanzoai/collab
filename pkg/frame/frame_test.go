package frame

import (
	"bytes"
	"fmt"
	"testing"
)

// Messages as y-websocket writes them, captured from yjs 13.6 and y-protocols
// 1.0: update inserts "hi" into Y.Text "t", step1 and step2 are an empty
// document's, awareness sets one client's state to {"user":"a"}.
var (
	update    = []byte{0, 2, 16, 1, 1, 192, 217, 245, 190, 7, 0, 4, 1, 1, 116, 2, 104, 105, 0}
	step1     = []byte{0, 0, 1, 0}
	step2     = []byte{0, 1, 2, 0, 0}
	awareness = []byte{1, 20, 1, 192, 217, 245, 190, 7, 1, 12, 123, 34, 117, 115, 101, 114, 34, 58, 34, 97, 34, 125}
	query     = []byte{3}
	denied    = []byte{2, 0, 2, 'n', 'o'}
)

func TestSplit(t *testing.T) {
	msgs := [][]byte{step1, awareness, update, step2, query, denied}
	got, ok := Split(bytes.Join(msgs, nil))
	if !ok || fmt.Sprint(got) != fmt.Sprint(msgs) {
		t.Fatalf("Split = %v, %v; want %v", got, ok, msgs)
	}
	if got, ok := Split(nil); !ok || len(got) != 0 {
		t.Fatalf("Split(nil) = %v, %v", got, ok)
	}
}

// A length past 127 takes a second varUint byte.
func TestSplitLongLength(t *testing.T) {
	body := bytes.Repeat([]byte{7}, 300)
	msg := append([]byte{0, 2, 0xac, 0x02}, body...) // 300 = 0b10_0101100
	got, ok := Split(append(msg, step1...))
	if !ok || len(got) != 2 || !bytes.Equal(got[0], msg) || !bytes.Equal(got[1], step1) {
		t.Fatalf("Split = %d messages, %v", len(got), ok)
	}
}

func TestSplitRefuses(t *testing.T) {
	for name, b := range map[string][]byte{
		"text":            []byte("update-from-A-hanzo:blue:relay1790152991638"),
		"truncated":       update[:len(update)-1],
		"trailing":        append(append([]byte{}, update...), 0),
		"sync type 3":     {0, 3, 0},
		"auth type 1":     {2, 1, 0},
		"message type 4":  {4},
		"endless varUint": {0, 2, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x01},
	} {
		if got, ok := Split(b); ok || got != nil {
			t.Errorf("%s: Split = %v, %v; want nil, false", name, got, ok)
		}
	}
}

func TestContent(t *testing.T) {
	for _, c := range []struct {
		msg  []byte
		want bool
	}{
		{update, true}, {step2, true}, {step1, false}, {awareness, false},
		{query, false}, {denied, false}, {[]byte{0}, false}, {nil, false},
	} {
		if got := Content(c.msg); got != c.want {
			t.Errorf("Content(%v) = %v, want %v", c.msg, got, c.want)
		}
	}
}
