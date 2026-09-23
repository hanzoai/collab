package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strconv"

	"github.com/hanzoai/sqlite"
)

// pageSize is the page size a new database is created with; maxFile is
// converted to max_page_count in these units.
const pageSize = 4096

// SQLite keeps one row per document in doc, carrying the document's size, and
// one row per frame in frame. It uses a single connection, so appends never
// contend for the write lock: callers queue on the pool, each holding only the
// frame it is persisting.
//
// Two bounds hold. Append refuses a frame that would take its document past
// maxDoc bytes. max_page_count holds the database file to maxFile bytes, which
// SQLite enforces on every write with SQLITE_FULL; the rollback journal holds
// only the pages one transaction changes, so the files on disk never exceed
// twice maxFile.
type SQLite struct {
	db     *sql.DB
	maxDoc int64
}

// NewSQLite opens (and migrates) the SQLite database at path, capping each
// document at maxDoc bytes of frames and the database file at maxFile bytes.
// The pragmas are applied to every connection on either driver backend.
func NewSQLite(path string, maxDoc, maxFile int64) (*SQLite, error) {
	db, err := sqlite.OpenPragma("file:"+path, []sqlite.Pragma{
		{Name: "busy_timeout", Value: "5000"},
		{Name: "page_size", Value: strconv.Itoa(pageSize)},
		{Name: "journal_mode", Value: "DELETE"},
		{Name: "max_page_count", Value: strconv.FormatInt(maxFile/pageSize, 10)},
	})
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(1)
	if _, err := db.ExecContext(context.Background(), `
		CREATE TABLE IF NOT EXISTS doc (
			id   TEXT PRIMARY KEY,
			size INTEGER NOT NULL
		);
		CREATE TABLE IF NOT EXISTS frame (
			seq  INTEGER PRIMARY KEY,
			doc  TEXT NOT NULL,
			data BLOB NOT NULL
		);
		CREATE INDEX IF NOT EXISTS frame_doc ON frame(doc, seq)`); err != nil {
		db.Close()
		return nil, fmt.Errorf("create tables: %w", err)
	}
	return &SQLite{db: db, maxDoc: maxDoc}, nil
}

// Append inserts frame after docID's existing frames and adds its length to
// the document's size, in one transaction. It returns ErrFull, and writes
// nothing, when the frame would take the document past maxDoc.
func (s *SQLite) Append(ctx context.Context, docID string, frame []byte) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	var size int64
	err = tx.QueryRowContext(ctx, `SELECT size FROM doc WHERE id = ?`, docID).Scan(&size)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return err
	}
	if size+int64(len(frame)) > s.maxDoc {
		return ErrFull
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO frame(doc, data) VALUES(?, ?)`, docID, frame); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `
		INSERT INTO doc(id, size) VALUES(?, ?)
		ON CONFLICT(id) DO UPDATE SET size = size + excluded.size`,
		docID, len(frame)); err != nil {
		return err
	}
	return tx.Commit()
}

// Load returns docID's frames in append order, or nil if it has none.
func (s *SQLite) Load(ctx context.Context, docID string) ([][]byte, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT data FROM frame WHERE doc = ? ORDER BY seq`, docID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var frames [][]byte
	for rows.Next() {
		var f []byte
		if err := rows.Scan(&f); err != nil {
			return nil, err
		}
		frames = append(frames, f)
	}
	return frames, rows.Err()
}

func (s *SQLite) Close() error { return s.db.Close() }
