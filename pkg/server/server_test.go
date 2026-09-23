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
	s, err := store.NewSQLite(filepath.Join(t.TempDir(), "t.db"), 1<<20, 64<<20)
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

// authGet sends a GET carrying a bearer token in the Authorization header.
func authGet(t *testing.T, url string) *http.Response {
	t.Helper()
	req, _ := http.NewRequest("GET", url, nil)
	req.Header.Set("Authorization", "Bearer x")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	return resp
}

// A token in the query string is not a credential: the edge logs it.
func TestQueryTokenRefused(t *testing.T) {
	ts := newTestServer(t, &fakeVerifier{owner: "org1", sub: "u1"})
	defer ts.Close()
	resp, err := http.Get(ts.URL + "/v1/collab/org1:ws1:doc1?token=x")
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
	resp := authGet(t, ts.URL+"/v1/collab/org1:ws1:doc1")
	if resp.StatusCode != http.StatusForbidden {
		t.Fatalf("want 403, got %d", resp.StatusCode)
	}
}

func TestBadDocID(t *testing.T) {
	ts := newTestServer(t, &fakeVerifier{owner: "org1", sub: "u1"})
	defer ts.Close()
	resp := authGet(t, ts.URL+"/v1/collab/notscoped")
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("want 400, got %d", resp.StatusCode)
	}
}

// dial opens a WebSocket the way a browser does: the token rides the
// subprotocol list beside yjs.
func dial(t *testing.T, ctx context.Context, ts *httptest.Server, doc string) *websocket.Conn {
	t.Helper()
	url := "ws" + strings.TrimPrefix(ts.URL, "http") + "/v1/collab/" + doc
	c, _, err := websocket.Dial(ctx, url, &websocket.DialOptions{Subprotocols: []string{"yjs", "bearer.x"}})
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	if c.Subprotocol() != "yjs" {
		t.Fatalf("negotiated %q, want yjs", c.Subprotocol())
	}
	return c
}

func TestWSRelayBetweenPeers(t *testing.T) {
	ts := newTestServer(t, &fakeVerifier{owner: "org1", sub: "u1"})
	defer ts.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	a := dial(t, ctx, ts, "org1:ws1:doc1")
	defer a.Close(websocket.StatusNormalClosure, "")
	b := dial(t, ctx, ts, "org1:ws1:doc1")
	defer b.Close(websocket.StatusNormalClosure, "")

	// A frame is relayed only to peers already joined, so a resends until b,
	// whose join races the first write, receives one. Duplicates are harmless:
	// b reads the first. b reads under the test's context, once: a Read whose
	// context expires closes the connection.
	type read struct {
		typ  websocket.MessageType
		data []byte
		err  error
	}
	got := make(chan read, 1)
	go func() {
		typ, data, err := b.Read(ctx)
		got <- read{typ, data, err}
	}()
	payload := []byte{0x01, 0x02, 0x03}
	resend := time.NewTicker(100 * time.Millisecond) // well under the frame-rate limit
	defer resend.Stop()
	for {
		if err := a.Write(ctx, websocket.MessageBinary, payload); err != nil {
			t.Fatalf("write: %v", err)
		}
		select {
		case <-resend.C:
			continue
		case r := <-got:
			if r.err != nil {
				t.Fatalf("read: %v", r.err)
			}
			if r.typ != websocket.MessageBinary {
				t.Fatalf("want binary, got %v", r.typ)
			}
			if string(r.data) != string(payload) {
				t.Fatalf("relay mismatch: want %v got %v", payload, r.data)
			}
			return
		}
	}
}

// A peer joining later receives every stored update as its own message, in
// order, since y-websocket decodes one message per WebSocket frame.
func TestWSReplaysEachFrame(t *testing.T) {
	ts := newTestServer(t, &fakeVerifier{owner: "org1", sub: "u1"})
	defer ts.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	sent := [][]byte{{0, 2, 0xaa}, {0, 2, 0xbb, 0xcc}}
	a := dial(t, ctx, ts, "org1:ws1:doc-replay")
	for _, f := range sent {
		if err := a.Write(ctx, websocket.MessageBinary, f); err != nil {
			t.Fatalf("write: %v", err)
		}
	}
	_ = a.Close(websocket.StatusNormalClosure, "")
	time.Sleep(100 * time.Millisecond)

	b := dial(t, ctx, ts, "org1:ws1:doc-replay")
	defer b.Close(websocket.StatusNormalClosure, "")
	for i, want := range sent {
		readCtx, c := context.WithTimeout(ctx, 2*time.Second)
		_, got, err := b.Read(readCtx)
		c()
		if err != nil {
			t.Fatalf("read %d: %v", i, err)
		}
		if string(got) != string(want) {
			t.Fatalf("replay %d: want %v got %v", i, want, got)
		}
	}
}

// A connection that sends frames faster than the limit is closed with 1008.
func TestWSRateLimitCloses(t *testing.T) {
	ts := newTestServer(t, &fakeVerifier{owner: "org1", sub: "u1"})
	defer ts.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	a := dial(t, ctx, ts, "org1:ws1:doc-flood")
	defer a.CloseNow()
	for range frameBurst + 50 {
		if err := a.Write(ctx, websocket.MessageBinary, []byte{1, 0}); err != nil {
			break
		}
	}
	_, _, err := a.Read(ctx)
	if got := websocket.CloseStatus(err); got != websocket.StatusPolicyViolation {
		t.Fatalf("close status %v (%v), want 1008", got, err)
	}
}

// A frame past maxFrame closes the connection with 1009 before it is relayed.
func TestWSOversizeFrameCloses(t *testing.T) {
	ts := newTestServer(t, &fakeVerifier{owner: "org1", sub: "u1"})
	defer ts.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	a := dial(t, ctx, ts, "org1:ws1:doc-big")
	defer a.CloseNow()
	_ = a.Write(ctx, websocket.MessageBinary, make([]byte, maxFrame+1))
	_, _, err := a.Read(ctx)
	if got := websocket.CloseStatus(err); got != websocket.StatusMessageTooBig {
		t.Fatalf("close status %v (%v), want 1009", got, err)
	}
}
