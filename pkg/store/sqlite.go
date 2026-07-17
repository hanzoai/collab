package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	_ "github.com/hanzoai/sqlite"
)

// SQLite stores each doc as a single row. We do not split updates into
// rows because for dev/local the doc is small and Load is the hot path.
type SQLite struct {
	db *sql.DB
}

// NewSQLite opens (and migrates) a SQLite DB at path.
func NewSQLite(path string) (*SQLite, error) {
	db, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, err
	}
	if _, err := db.ExecContext(context.Background(), `
		CREATE TABLE IF NOT EXISTS docs (
			doc_id TEXT PRIMARY KEY,
			state  BLOB NOT NULL
		)`); err != nil {
		db.Close()
		return nil, fmt.Errorf("create table: %w", err)
	}
	return &SQLite{db: db}, nil
}

// Append concatenates the update onto the existing state. Y.js update
// merging is associative + commutative, so concatenation is a valid
// representation of the merged document (client merges on load).
func (s *SQLite) Append(ctx context.Context, docID string, update []byte) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	var existing []byte
	row := tx.QueryRowContext(ctx, `SELECT state FROM docs WHERE doc_id = ?`, docID)
	if err := row.Scan(&existing); err != nil && !errors.Is(err, sql.ErrNoRows) {
		return err
	}
	merged := append(existing, update...)
	if _, err := tx.ExecContext(ctx, `
		INSERT INTO docs(doc_id, state) VALUES(?, ?)
		ON CONFLICT(doc_id) DO UPDATE SET state = excluded.state`,
		docID, merged); err != nil {
		return err
	}
	return tx.Commit()
}

// Load returns the merged state or nil if absent.
func (s *SQLite) Load(ctx context.Context, docID string) ([]byte, error) {
	var state []byte
	row := s.db.QueryRowContext(ctx, `SELECT state FROM docs WHERE doc_id = ?`, docID)
	if err := row.Scan(&state); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, nil
		}
		return nil, err
	}
	return state, nil
}

func (s *SQLite) Close() error { return s.db.Close() }
