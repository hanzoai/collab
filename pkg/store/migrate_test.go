package store

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
)

// Messages as y-websocket writes them, captured from yjs 13.6 and y-protocols
// 1.0 (the same as pkg/frame's tests): update inserts "hi" into Y.Text "t".
var (
	update    = []byte{0, 2, 16, 1, 1, 192, 217, 245, 190, 7, 0, 4, 1, 1, 116, 2, 104, 105, 0}
	step1     = []byte{0, 0, 1, 0}
	step2     = []byte{0, 1, 2, 0, 0}
	awareness = []byte{1, 20, 1, 192, 217, 245, 190, 7, 1, 12, 123, 34, 117, 115, 101, 114, 34, 58, 34, 97, 34, 125}
)

type doc01 struct {
	id   string
	msgs [][]byte
}

// seed01 writes a database as collab 0.1.x left it: its one table, created
// with 0.1.x's statement, and each document's state the concatenation of its
// messages in order, written with 0.1.x's upsert (0.1.x's Append concatenated
// one message at a time; the row it leaves is the same).
func seed01(t *testing.T, path string, docs ...doc01) {
	t.Helper()
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	ctx := context.Background()
	if _, err := db.ExecContext(ctx, `
		CREATE TABLE IF NOT EXISTS docs (
			doc_id TEXT PRIMARY KEY,
			state  BLOB NOT NULL
		)`); err != nil {
		t.Fatal(err)
	}
	for _, d := range docs {
		var existing []byte
		err := db.QueryRowContext(ctx, `SELECT state FROM docs WHERE doc_id = ?`, d.id).Scan(&existing)
		if err != nil && !errors.Is(err, sql.ErrNoRows) {
			t.Fatal(err)
		}
		if _, err := db.ExecContext(ctx, `
			INSERT INTO docs(doc_id, state) VALUES(?, ?)
			ON CONFLICT(doc_id) DO UPDATE SET state = excluded.state`,
			d.id, bytes.Join(append([][]byte{existing}, d.msgs...), nil)); err != nil {
			t.Fatal(err)
		}
	}
}

// hasTable reports whether s's database has a table named name.
func hasTable(t *testing.T, s *SQLite, name string) bool {
	t.Helper()
	var n int
	if err := s.db.QueryRow(`SELECT count(*) FROM sqlite_master WHERE type = 'table' AND name = ?`, name).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n == 1
}

// docSize returns the size doc records for id, 0 when it has no row.
func docSize(t *testing.T, s *SQLite, id string) int64 {
	t.Helper()
	var size int64
	err := s.db.QueryRow(`SELECT size FROM doc WHERE id = ?`, id).Scan(&size)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		t.Fatal(err)
	}
	return size
}

// A 0.1.x database opens with its documents: each keeps its content messages
// as frames, in order, and the 0.1.x table is gone. A state that is not
// y-websocket messages is kept whole. Reopening migrates nothing twice, and the
// migrated document takes appends after its frames.
func TestSQLiteMigrates01(t *testing.T) {
	path := filepath.Join(t.TempDir(), "collab.db")
	relay := []byte("update-from-A-hanzo:blue:relay1790152991638")
	seed01(t, path,
		doc01{"hanzo:w:note", [][]byte{step1, awareness, update, step2, awareness}},
		doc01{"hanzo:w:raw", [][]byte{relay}},
		doc01{"hanzo:w:presence", [][]byte{step1, awareness}},
	)

	want := map[string][][]byte{
		"hanzo:w:note":     {update, step2},
		"hanzo:w:raw":      {relay},
		"hanzo:w:presence": nil,
	}
	for range 2 {
		s, err := NewSQLite(path, 1<<20, 64<<20)
		if err != nil {
			t.Fatal(err)
		}
		if hasTable(t, s, "docs") {
			t.Fatal("0.1.x table docs is still there")
		}
		for id, w := range want {
			got, err := s.Load(context.Background(), id)
			if err != nil {
				t.Fatal(err)
			}
			if fmt.Sprint(got) != fmt.Sprint(w) {
				t.Fatalf("%s: frames %v, want %v", id, got, w)
			}
			var size int64
			for _, f := range w {
				size += int64(len(f))
			}
			if got := docSize(t, s, id); got != size {
				t.Fatalf("%s: size %d, want %d", id, got, size)
			}
		}
		s.Close()
	}

	s := open(t, path, 1<<20, 64<<20)
	if err := s.Append(context.Background(), "hanzo:w:note", update); err != nil {
		t.Fatal(err)
	}
	if got := frames(t, s, "hanzo:w:note"); len(got) != 3 || got[2] != string(update) {
		t.Fatalf("frames after append: %q", got)
	}
}

// A document that already has frames (0.1.x run again over a current
// database) keeps them, and its 0.1.x messages follow.
func TestSQLiteMigrationFollowsFrames(t *testing.T) {
	path := filepath.Join(t.TempDir(), "collab.db")
	s, err := NewSQLite(path, 1<<20, 64<<20)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Append(context.Background(), "hanzo:w:note", step2); err != nil {
		t.Fatal(err)
	}
	s.Close()
	seed01(t, path, doc01{"hanzo:w:note", [][]byte{step1, update}})

	s = open(t, path, 1<<20, 64<<20)
	if got, want := fmt.Sprint(frames(t, s, "hanzo:w:note")), fmt.Sprint([]string{string(step2), string(update)}); got != want {
		t.Fatalf("frames %q, want %q", got, want)
	}
	if got := docSize(t, s, "hanzo:w:note"); got != int64(len(step2)+len(update)) {
		t.Fatalf("size %d, want %d", got, len(step2)+len(update))
	}
}

// The migration fits in the file cap when one 0.1.x document takes more than
// half of it: its frames reuse the pages its row freed.
func TestSQLiteMigrationReusesPages(t *testing.T) {
	const maxFile = 1 << 20
	path := filepath.Join(t.TempDir(), "collab.db")
	big := append([]byte{0, 2, 0xe0, 0xd4, 0x03}, make([]byte, 60000)...) // one 60000-byte update
	msgs := slices.Repeat([][]byte{big}, 10)
	seed01(t, path, doc01{"hanzo:w:note", msgs})
	fi, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if fi.Size() < maxFile/2 || fi.Size() > maxFile {
		t.Fatalf("seeded %d bytes, want between half the cap and the cap", fi.Size())
	}

	s := open(t, path, 1<<30, maxFile)
	got, err := s.Load(context.Background(), "hanzo:w:note")
	if err != nil || !slices.EqualFunc(got, msgs, bytes.Equal) {
		t.Fatalf("%d frames, want the %d updates (%v)", len(got), len(msgs), err)
	}
	if fi, err := os.Stat(path); err != nil || fi.Size() > maxFile {
		t.Fatalf("file after the migration: %v bytes, cap %d (%v)", fi.Size(), maxFile, err)
	}
}

// A migration that cannot fit fails NewSQLite and leaves the 0.1.x table
// byte for byte; a larger cap then migrates it.
func TestSQLiteMigrationFailureKeepsDocs(t *testing.T) {
	path := filepath.Join(t.TempDir(), "collab.db")
	msgs := make([][]byte, 20000) // many small frames: their rows and index outgrow the state
	for i := range msgs {
		msgs[i] = update
	}
	seed01(t, path, doc01{"hanzo:w:note", msgs})
	state := bytes.Repeat(update, len(msgs))

	if _, err := NewSQLite(path, 1<<30, 512<<10); err == nil || !strings.Contains(err.Error(), "migrate") {
		t.Fatalf("NewSQLite = %v, want a migrate error", err)
	}
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	var got []byte
	if err := db.QueryRow(`SELECT state FROM docs WHERE doc_id = 'hanzo:w:note'`).Scan(&got); err != nil || !bytes.Equal(got, state) {
		t.Fatalf("0.1.x state after a failed migration: %d bytes, want %d (%v)", len(got), len(state), err)
	}
	var n int
	if err := db.QueryRow(`SELECT count(*) FROM frame`).Scan(&n); err != nil || n != 0 {
		t.Fatalf("%d frames written by a failed migration (%v)", n, err)
	}
	db.Close()

	s := open(t, path, 1<<30, 8<<20)
	if got := frames(t, s, "hanzo:w:note"); len(got) != len(msgs) {
		t.Fatalf("%d frames after the retry, want %d", len(got), len(msgs))
	}
}

// A "file:" URI, such as the DSN collab 0.1.1 is configured with, is refused
// by name before anything is opened: the store sets its own pragmas on a file
// path.
func TestSQLiteRefusesURI(t *testing.T) {
	dir := t.TempDir()
	for _, p := range []string{
		"file:" + dir + "/collab.db?_pragma=busy_timeout(5000)&_pragma=journal_mode(DELETE)&_pragma=max_page_count(16384)&_txlock=immediate",
		"file:" + dir + "/collab.db",
		dir + "/collab.db?cache=shared",
	} {
		s, err := NewSQLite(p, 1<<20, 64<<20)
		if err == nil {
			s.Close()
		}
		if err == nil || !strings.Contains(err.Error(), "URI") {
			t.Fatalf("NewSQLite(%q) = %v, want it refused as a URI", p, err)
		}
	}
	if entries, _ := os.ReadDir(dir); len(entries) != 0 {
		t.Fatalf("refused URIs left files: %v", entries)
	}
}
