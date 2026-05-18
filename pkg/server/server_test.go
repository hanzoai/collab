package server

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"

	"github.com/hanzoai/collab/pkg/auth"
	"github.com/hanzoai/collab/pkg/metrics"
	"github.com/hanzoai/collab/pkg/store"
)

func init() { metrics.Register(nil) }

type fakeVerifier struct {
	owner string
	sub   string
	fail  bool
}

func (f *fakeVerifier) Verify(_ context.Context, _ string) (*auth.Claims, error) {
	if f.fail {
		return nil, errors.New("nope")
	}
	c := &auth.Claims{Sub: f.sub, Owner: f.owner}
	c.RegisteredClaims.Subject = f.sub
	return c, nil
}

func newTestServer(t *testing.T, v Verifier) *httptest.Server {
	t.Helper()
	s, err := store.NewSQLite(filepath.Join(t.TempDir(), "t.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })

	srv := New(Config{Version: "test", Verify: v, Store: s})
	return httptest.NewServer(srv.Handler())
}

func TestHealth(t *testing.T) {
	ts := newTestServer(t, &fakeVerifier{})
	defer ts.Close()
	resp, err := http.Get(ts.URL + "/v1/health")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("status %d", resp.StatusCode)
	}
	var body map[string]string
	_ = json.NewDecoder(resp.Body).Decode(&body)
	if body["status"] != "ok" || body["version"] != "test" {
		t.Fatalf("bad health: %v", body)
	}
}

func TestUnauthorizedNoToken(t *testing.T) {
	ts := newTestServer(t, &fakeVerifier{})
	defer ts.Close()
	resp, err := http.Get(ts.URL + "/v1/collab/org1:ws1:doc1")
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("want 401, got %d", resp.StatusCode)
	}
}

func TestForbiddenWrongOrg(t *testing.T) {
	ts := newTestServer(t, &fakeVerifier{owner: "other", sub: "u1"})
	defer ts.Close()
	resp, err := http.Get(ts.URL + "/v1/collab/org1:ws1:doc1?token=x")
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusForbidden {
		t.Fatalf("want 403, got %d", resp.StatusCode)
	}
}

func TestBadDocID(t *testing.T) {
	ts := newTestServer(t, &fakeVerifier{owner: "org1", sub: "u1"})
	defer ts.Close()
	resp, err := http.Get(ts.URL + "/v1/collab/notscoped?token=x")
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("want 400, got %d", resp.StatusCode)
	}
}

func TestWSRelayBetweenPeers(t *testing.T) {
	ts := newTestServer(t, &fakeVerifier{owner: "org1", sub: "u1"})
	defer ts.Close()

	url := "ws" + strings.TrimPrefix(ts.URL, "http") + "/v1/collab/org1:ws1:doc1?token=x"

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	a, _, err := websocket.Dial(ctx, url, &websocket.DialOptions{
		Subprotocols: []string{"yjs"},
	})
	if err != nil {
		t.Fatalf("dial a: %v", err)
	}
	defer a.Close(websocket.StatusNormalClosure, "")

	b, _, err := websocket.Dial(ctx, url, &websocket.DialOptions{
		Subprotocols: []string{"yjs"},
	})
	if err != nil {
		t.Fatalf("dial b: %v", err)
	}
	defer b.Close(websocket.StatusNormalClosure, "")

	// Give the room registry a tick to register b.
	time.Sleep(50 * time.Millisecond)

	payload := []byte{0x01, 0x02, 0x03}
	if err := a.Write(ctx, websocket.MessageBinary, payload); err != nil {
		t.Fatalf("write: %v", err)
	}

	readCtx, c := context.WithTimeout(ctx, 2*time.Second)
	defer c()
	typ, data, err := b.Read(readCtx)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if typ != websocket.MessageBinary {
		t.Fatalf("want binary, got %v", typ)
	}
	if string(data) != string(payload) {
		t.Fatalf("relay mismatch: want %v got %v", payload, data)
	}
}

func TestWSReplaysPersistedState(t *testing.T) {
	ts := newTestServer(t, &fakeVerifier{owner: "org1", sub: "u1"})
	defer ts.Close()

	url := "ws" + strings.TrimPrefix(ts.URL, "http") + "/v1/collab/org1:ws1:doc-replay?token=x"

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// First peer sends an update, then disconnects.
	a, _, err := websocket.Dial(ctx, url, &websocket.DialOptions{Subprotocols: []string{"yjs"}})
	if err != nil {
		t.Fatalf("dial a: %v", err)
	}
	payload := []byte{0xaa, 0xbb}
	if err := a.Write(ctx, websocket.MessageBinary, payload); err != nil {
		t.Fatalf("write: %v", err)
	}
	// Give persistence a moment.
	time.Sleep(150 * time.Millisecond)
	_ = a.Close(websocket.StatusNormalClosure, "")
	time.Sleep(50 * time.Millisecond)

	// Second peer joins fresh — should immediately receive persisted state.
	b, _, err := websocket.Dial(ctx, url, &websocket.DialOptions{Subprotocols: []string{"yjs"}})
	if err != nil {
		t.Fatalf("dial b: %v", err)
	}
	defer b.Close(websocket.StatusNormalClosure, "")
	readCtx, c := context.WithTimeout(ctx, 2*time.Second)
	defer c()
	_, got, err := b.Read(readCtx)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if string(got) != string(payload) {
		t.Fatalf("replay mismatch: want %v got %v", payload, got)
	}
}
