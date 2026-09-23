package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"strings"

	"github.com/hanzoai/sqlite"

	"github.com/hanzoai/collab/pkg/frame"
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

// NewSQLite opens the SQLite database at path, capping each document at maxDoc
// bytes of frames and the database file at maxFile bytes, and moves a 0.1.x
// database's documents into the current tables (see migrate). The pragmas are
// applied here to every connection on either driver backend, so path is a file
// path, never a "file:" URI carrying its own.
func NewSQLite(path string, maxDoc, maxFile int64) (*SQLite, error) {
	if strings.HasPrefix(path, "file:") || strings.ContainsRune(path, '?') {
		return nil, fmt.Errorf("%q is a URI: give a file path; the pragmas are set here", path)
	}
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
	if err := migrate(context.Background(), db); err != nil {
		db.Close()
		return nil, fmt.Errorf("migrate 0.1 docs: %w", err)
	}
	return &SQLite{db: db, maxDoc: maxDoc}, nil
}

// migrate moves the documents of a 0.1.x database into doc and frame, then
// drops its table. 0.1.x kept one row per document, docs(doc_id, state), where
// state is every binary message the document received, concatenated.
//
// A state that splits into whole y-websocket messages keeps its content
// messages, in order, as frames: what Append stores for the same messages. A
// state that does not split is kept whole, as one frame, which is what 0.1.x
// replayed. The migrated frames follow any the document already has, its size
// counts them, and maxDoc does not refuse them.
//
// It is one transaction, so a failure (SQLITE_FULL when the frames and their
// index outgrow maxFile) leaves the 0.1.x table as it was and NewSQLite fails.
// Each document's row is deleted before its frames are written, so the frames
// reuse the pages the row freed.
func migrate(ctx context.Context, db *sql.DB) error {
	var n int
	if err := db.QueryRowContext(ctx,
		`SELECT count(*) FROM sqlite_master WHERE type = 'table' AND name = 'docs'`).Scan(&n); err != nil || n == 0 {
		return err
	}
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	rows, err := tx.QueryContext(ctx, `SELECT doc_id FROM docs ORDER BY rowid`)
	if err != nil {
		return err
	}
	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			rows.Close()
			return err
		}
		ids = append(ids, id)
	}
	if err := errors.Join(rows.Err(), rows.Close()); err != nil {
		return err
	}

	for _, id := range ids {
		var state []byte
		if err := tx.QueryRowContext(ctx, `SELECT state FROM docs WHERE doc_id = ?`, id).Scan(&state); err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, `DELETE FROM docs WHERE doc_id = ?`, id); err != nil {
			return err
		}
		frames := [][]byte{state}
		if msgs, ok := frame.Split(state); ok {
			frames = slices.DeleteFunc(msgs, func(m []byte) bool { return !frame.Content(m) })
		}
		size := 0
		for _, f := range frames {
			if len(f) == 0 {
				continue
			}
			if _, err := tx.ExecContext(ctx, `INSERT INTO frame(doc, data) VALUES(?, ?)`, id, f); err != nil {
				return err
			}
			size += len(f)
		}
		if size > 0 {
			if _, err := tx.ExecContext(ctx, `
				INSERT INTO doc(id, size) VALUES(?, ?)
				ON CONFLICT(id) DO UPDATE SET size = size + excluded.size`,
				id, size); err != nil {
				return err
			}
		}
	}
	if _, err := tx.ExecContext(ctx, `DROP TABLE docs`); err != nil {
		return err
	}
	return tx.Commit()
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
