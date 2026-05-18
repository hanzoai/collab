package store

import (
	"context"
	"path/filepath"
	"testing"
)

func TestSQLiteAppendLoad(t *testing.T) {
	dir := t.TempDir()
	s, err := NewSQLite(filepath.Join(dir, "test.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	ctx := context.Background()

	got, err := s.Load(ctx, "doc1")
	if err != nil {
		t.Fatalf("load empty: %v", err)
	}
	if got != nil {
		t.Fatalf("expected nil for missing doc, got %v", got)
	}

	if err := s.Append(ctx, "doc1", []byte("aa")); err != nil {
		t.Fatal(err)
	}
	if err := s.Append(ctx, "doc1", []byte("bb")); err != nil {
		t.Fatal(err)
	}
	got, err = s.Load(ctx, "doc1")
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != "aabb" {
		t.Fatalf("want aabb, got %q", got)
	}

	// distinct doc
	if err := s.Append(ctx, "doc2", []byte("xx")); err != nil {
		t.Fatal(err)
	}
	got, err = s.Load(ctx, "doc2")
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != "xx" {
		t.Fatalf("want xx, got %q", got)
	}
}
