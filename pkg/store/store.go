// Package store persists Y.js document state.
//
// A document is the ordered list of the frames that carried its content. A
// frame is appended as it arrives and never rewritten, so an append costs the
// size of the frame, not the size of the document. Load returns the frames in
// order, and the server replays each as its own WebSocket message.
package store

import (
	"context"
	"errors"
)

// ErrFull is returned by Append when the frame would take its document past
// the store's per-document cap.
var ErrFull = errors.New("store: document full")

// Store is the persistence boundary. Implementations are safe for concurrent
// use.
type Store interface {
	// Append persists one frame for docID, after every frame appended before it.
	Append(ctx context.Context, docID string, frame []byte) error
	// Load returns docID's frames in append order, or none if the doc is new.
	Load(ctx context.Context, docID string) ([][]byte, error)
	// Close releases adapter resources.
	Close() error
}
