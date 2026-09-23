package store

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
)

func open(t *testing.T, path string, maxDoc, maxFile int64) *SQLite {
	t.Helper()
	s, err := NewSQLite(path, maxDoc, maxFile)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	return s
}

func frames(t *testing.T, s *SQLite, doc string) []string {
	t.Helper()
	got, err := s.Load(context.Background(), doc)
	if err != nil {
		t.Fatal(err)
	}
	out := make([]string, len(got))
	for i, f := range got {
		out[i] = string(f)
	}
	return out
}

func TestSQLiteAppendLoad(t *testing.T) {
	s := open(t, filepath.Join(t.TempDir(), "t.db"), 1<<20, 64<<20)
	ctx := context.Background()

	if got := frames(t, s, "doc1"); len(got) != 0 {
		t.Fatalf("new doc has frames: %q", got)
	}
	for _, f := range []string{"aa", "bb", "cc"} {
		if err := s.Append(ctx, "doc1", []byte(f)); err != nil {
			t.Fatal(err)
		}
	}
	if err := s.Append(ctx, "doc2", []byte("xx")); err != nil {
		t.Fatal(err)
	}
	if got := fmt.Sprint(frames(t, s, "doc1")); got != "[aa bb cc]" {
		t.Fatalf("doc1 frames %s, want [aa bb cc]", got)
	}
	if got := fmt.Sprint(frames(t, s, "doc2")); got != "[xx]" {
		t.Fatalf("doc2 frames %s, want [xx]", got)
	}
}

// A frame that would take its document past the cap is refused whole, and the
// document keeps what it had; other documents are unaffected.
func TestSQLiteDocCap(t *testing.T) {
	s := open(t, filepath.Join(t.TempDir(), "t.db"), 10, 64<<20)
	ctx := context.Background()

	if err := s.Append(ctx, "doc", []byte("0123456")); err != nil {
		t.Fatal(err)
	}
	if err := s.Append(ctx, "doc", []byte("7890")); !errors.Is(err, ErrFull) {
		t.Fatalf("append past cap: %v, want ErrFull", err)
	}
	if err := s.Append(ctx, "doc", []byte("789")); err != nil {
		t.Fatalf("append up to cap: %v", err)
	}
	if got := fmt.Sprint(frames(t, s, "doc")); got != "[0123456 789]" {
		t.Fatalf("frames %s", got)
	}
	if err := s.Append(ctx, "other", []byte("0123456789")); err != nil {
		t.Fatalf("other doc refused: %v", err)
	}
}

// maxFile bounds the database file across all documents: once it is reached
// every append fails, the file stays inside the cap, and what it holds still
// reads back.
func TestSQLiteFileCap(t *testing.T) {
	const maxFile = 1 << 20
	path := filepath.Join(t.TempDir(), "t.db")
	s := open(t, path, 1<<30, maxFile)
	ctx := context.Background()

	frame := make([]byte, 16<<10)
	var full int
	for i := range 200 {
		err := s.Append(ctx, fmt.Sprintf("o:w:%d", i), frame)
		if err == nil {
			continue
		}
		if !strings.Contains(err.Error(), "full") {
			t.Fatalf("append %d: %v", i, err)
		}
		full++
	}
	if full == 0 {
		t.Fatal("file cap never reached")
	}
	fi, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if fi.Size() > maxFile {
		t.Fatalf("file is %d bytes, cap %d", fi.Size(), maxFile)
	}
	if got := frames(t, s, "o:w:0"); len(got) != 1 {
		t.Fatalf("stored doc unreadable after the cap: %d frames", len(got))
	}
}

// Concurrent appends all land: one connection serializes them, so none fails
// on the write lock.
func TestSQLiteConcurrentAppends(t *testing.T) {
	s := open(t, filepath.Join(t.TempDir(), "t.db"), 1<<20, 64<<20)
	var wg sync.WaitGroup
	errs := make(chan error, 200)
	for range 200 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := s.Append(context.Background(), "doc", []byte("0123456789")); err != nil {
				errs <- err
			}
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatalf("append failed: %v", err)
	}
	if n := len(frames(t, s, "doc")); n != 200 {
		t.Fatalf("%d frames stored, want 200", n)
	}
}
