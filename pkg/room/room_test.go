package room

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/hanzoai/collab/pkg/metrics"
)

func init() { metrics.Register(nil) }

// memStore is an in-memory Store for tests.
type memStore struct {
	mu   sync.Mutex
	data map[string][]byte
}

func newMem() *memStore { return &memStore{data: map[string][]byte{}} }

func (m *memStore) Append(_ context.Context, id string, b []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.data[id] = append(m.data[id], b...)
	return nil
}
func (m *memStore) Load(_ context.Context, id string) ([]byte, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if v, ok := m.data[id]; ok {
		return append([]byte(nil), v...), nil
	}
	return nil, nil
}
func (m *memStore) Close() error { return nil }

func TestRegistryJoinLeave(t *testing.T) {
	ms := newMem()
	_ = ms.Append(context.Background(), "doc", []byte("hello"))

	r := NewRegistry(ms)

	p1 := &Peer{ID: "a", Send: make(chan []byte, 4)}
	rm, state, err := r.Join(context.Background(), "doc", p1)
	if err != nil {
		t.Fatal(err)
	}
	if string(state) != "hello" {
		t.Fatalf("state mismatch: %q", state)
	}
	if rm.PeerCount() != 1 {
		t.Fatalf("want 1 peer, got %d", rm.PeerCount())
	}

	p2 := &Peer{ID: "b", Send: make(chan []byte, 4)}
	rm2, _, err := r.Join(context.Background(), "doc", p2)
	if err != nil {
		t.Fatal(err)
	}
	if rm != rm2 {
		t.Fatal("expected same Room reference")
	}
	if rm.PeerCount() != 2 {
		t.Fatalf("want 2 peers, got %d", rm.PeerCount())
	}

	r.Leave("doc", p1)
	if rm.PeerCount() != 1 {
		t.Fatalf("want 1 peer after leave, got %d", rm.PeerCount())
	}
	r.Leave("doc", p2)
	// room should be GC'd; rejoining should yield a fresh room
	p3 := &Peer{ID: "c", Send: make(chan []byte, 4)}
	rm3, _, err := r.Join(context.Background(), "doc", p3)
	if err != nil {
		t.Fatal(err)
	}
	if rm3 == rm {
		t.Fatal("expected new room after full leave")
	}
	r.Leave("doc", p3)
}

func TestRoomBroadcast(t *testing.T) {
	ms := newMem()
	r := NewRegistry(ms)

	p1 := &Peer{ID: "a", Send: make(chan []byte, 4)}
	p2 := &Peer{ID: "b", Send: make(chan []byte, 4)}
	p3 := &Peer{ID: "c", Send: make(chan []byte, 4)}
	rm, _, _ := r.Join(context.Background(), "doc", p1)
	_, _, _ = r.Join(context.Background(), "doc", p2)
	_, _, _ = r.Join(context.Background(), "doc", p3)

	msg := []byte("update")
	rm.Broadcast(context.Background(), p1, msg)

	for _, p := range []*Peer{p2, p3} {
		select {
		case got := <-p.Send:
			if string(got) != "update" {
				t.Fatalf("peer %s got %q", p.ID, got)
			}
		case <-time.After(time.Second):
			t.Fatalf("peer %s never received", p.ID)
		}
	}

	select {
	case <-p1.Send:
		t.Fatal("sender must not receive its own broadcast")
	case <-time.After(100 * time.Millisecond):
	}

	// Persistence happens asynchronously; allow a brief moment.
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		got, _ := ms.Load(context.Background(), "doc")
		if string(got) == "update" {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("persistence never observed")
}
