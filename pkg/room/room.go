// Package room is the in-memory registry of doc-keyed broadcast hubs.
//
// We use a dumb relay (no server-side Y.js merging). Justification:
// y-websocket already converges client state via the sync + awareness
// protocols; the server only needs to (a) fan out binary messages and
// (b) persist the ones that carry document content so a fresh peer can
// replay the state. Merging CRDT updates server-side would buy us nothing
// for single-region relay and pulls in a non-trivial dep (peerdb-io/y-go).
// Revisit only when we need cross-region merge.
package room

import (
	"context"
	"errors"
	"sync"

	"github.com/hanzoai/collab/pkg/frame"
	"github.com/hanzoai/collab/pkg/metrics"
	"github.com/hanzoai/collab/pkg/store"
)

// Peer is a single connected WS participant.
type Peer struct {
	ID   string
	Send chan []byte
}

// Room fans out binary messages between Peers of the same docID and
// persists the ones that carry document content to the backing Store.
type Room struct {
	DocID string
	store store.Store

	mu    sync.Mutex
	peers map[*Peer]struct{}
}

// Registry maps docID → Room. One Room per doc, reference-counted on
// peer add/remove. When the last peer leaves we GC the room.
type Registry struct {
	store store.Store

	mu    sync.Mutex
	rooms map[string]*Room
}

// NewRegistry builds a Registry over the given Store.
func NewRegistry(s store.Store) *Registry {
	return &Registry{store: s, rooms: map[string]*Room{}}
}

// Join attaches peer to docID's Room (creating it if needed) and
// returns the persisted frames to replay to the new peer, in order.
//
// The state is loaded before the registry is touched, so a failed Load
// leaves no Room behind. The Room is then found or created and the peer
// added under one r.mu hold, the same lock Leave takes to remove an empty
// Room, so a joiner can never enter a Room that Leave has just removed.
func (r *Registry) Join(ctx context.Context, docID string, peer *Peer) (*Room, [][]byte, error) {
	frames, err := r.store.Load(ctx, docID)
	if err != nil {
		return nil, nil, err
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	rm, ok := r.rooms[docID]
	if !ok {
		rm = &Room{DocID: docID, store: r.store, peers: map[*Peer]struct{}{}}
		r.rooms[docID] = rm
		metrics.Rooms.Inc()
	}
	rm.mu.Lock()
	rm.peers[peer] = struct{}{}
	rm.mu.Unlock()
	metrics.Peers.Inc()
	return rm, frames, nil
}

// Leave detaches peer; when the last peer leaves the Room is removed.
func (r *Registry) Leave(docID string, peer *Peer) {
	r.mu.Lock()
	rm, ok := r.rooms[docID]
	if !ok {
		r.mu.Unlock()
		return
	}
	rm.mu.Lock()
	delete(rm.peers, peer)
	empty := len(rm.peers) == 0
	rm.mu.Unlock()
	if empty {
		delete(r.rooms, docID)
		metrics.Rooms.Dec()
	}
	r.mu.Unlock()
	metrics.Peers.Dec()
}

// Broadcast relays msg to all peers in the room EXCEPT the sender, then
// persists it if it carries document content. The append runs in the
// caller's goroutine, so a connection reads its next frame only after this
// one is stored: each connection has at most one append in flight.
func (rm *Room) Broadcast(ctx context.Context, sender *Peer, msg []byte) {
	rm.mu.Lock()
	targets := make([]*Peer, 0, len(rm.peers))
	for p := range rm.peers {
		if p == sender {
			continue
		}
		targets = append(targets, p)
	}
	rm.mu.Unlock()

	for _, p := range targets {
		select {
		case p.Send <- msg:
		default:
			// Drop messages for slow peers rather than block the relay.
			metrics.Errors.WithLabelValues("peer_slow").Inc()
		}
	}

	// Sync step 1 and awareness (sent on every cursor move) are relayed but
	// not stored: replaying them to a later peer replays nothing of the
	// document.
	if !frame.Content(msg) {
		return
	}
	switch err := rm.store.Append(ctx, rm.DocID, msg); {
	case err == nil:
	case errors.Is(err, store.ErrFull):
		metrics.Errors.WithLabelValues("store_full").Inc()
	default:
		metrics.Errors.WithLabelValues("store_append").Inc()
	}
}

// PeerCount returns the current number of peers in this room.
func (rm *Room) PeerCount() int {
	rm.mu.Lock()
	defer rm.mu.Unlock()
	return len(rm.peers)
}
