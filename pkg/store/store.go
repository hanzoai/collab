// Package store persists Y.js document state.
//
// We treat doc state as an opaque blob of CRDT updates. Updates are
// appended; load returns the concatenation. Periodic compaction is the
// adapter's job (in S3 we coalesce on a watermark; in SQLite the row is
// the canonical state and writes overwrite).
package store

import "context"

// Store is the persistence boundary.
//
// One way: every adapter MUST be safe for concurrent calls on the same
// docID; the server treats Append + Load as the only operations.
type Store interface {
	// Append persists a Y.js update for docID.
	Append(ctx context.Context, docID string, update []byte) error
	// Load returns the merged document state, or nil if the doc is new.
	Load(ctx context.Context, docID string) ([]byte, error)
	// Close releases adapter resources.
	Close() error
}
