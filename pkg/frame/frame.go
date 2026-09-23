// Package frame reads the y-websocket message framing (y-protocols sync,
// awareness and auth, lib0 variable-length encoding).
//
// A message is a varUint message type followed by its body:
//
//	0 sync:            varUint sync type (0 step1, 1 step2, 2 update), varUint8Array
//	1 awareness:       varUint8Array
//	2 auth:            varUint 0 (permission denied), varString reason
//	3 query awareness: no body
//
// varUint8Array and varString are a varUint length and that many bytes.
package frame

// Content reports whether msg is a sync message that carries document content:
// messageSync (0) followed by syncStep2 (1) or syncUpdate (2). Sync step 1 (a
// state vector) and awareness (presence) carry none.
func Content(msg []byte) bool {
	return len(msg) >= 2 && msg[0] == 0 && (msg[1] == 1 || msg[1] == 2)
}

// Split cuts b into the whole messages it concatenates, in order. ok is false,
// and msgs nil, when b is not such a sequence: an unknown type, or a length
// running past the end.
func Split(b []byte) (msgs [][]byte, ok bool) {
	for d := (decoder{b: b}); len(d.b) > 0; {
		start := d.b
		d.message()
		if d.bad {
			return nil, false
		}
		msgs = append(msgs, start[:len(start)-len(d.b)])
	}
	return msgs, true
}

type decoder struct {
	b   []byte
	bad bool
}

// message consumes one message.
func (d *decoder) message() {
	switch d.uint() {
	case 0:
		if d.uint() > 2 {
			d.bad = true
		}
		d.bytes()
	case 1:
		d.bytes()
	case 2:
		if d.uint() != 0 {
			d.bad = true
		}
		d.bytes()
	case 3:
	default:
		d.bad = true
	}
}

// uint consumes a lib0 varUint: 7 bits a byte, least significant first, the
// high bit set on every byte but the last. lib0 writes at most 53 bits.
func (d *decoder) uint() uint64 {
	var v uint64
	for shift := uint(0); !d.bad; shift += 7 {
		if len(d.b) == 0 || shift > 49 {
			d.bad = true
			break
		}
		c := d.b[0]
		d.b = d.b[1:]
		v |= uint64(c&0x7f) << shift
		if c < 0x80 {
			return v
		}
	}
	return 0
}

// bytes consumes a varUint length and that many bytes.
func (d *decoder) bytes() {
	n := d.uint()
	if d.bad || n > uint64(len(d.b)) {
		d.bad = true
		return
	}
	d.b = d.b[n:]
}
