// Package server wires HTTP, WS, metrics, auth, and the room registry.
package server

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"time"

	"github.com/coder/websocket"

	"github.com/hanzoai/collab/pkg/auth"
	"github.com/hanzoai/collab/pkg/metrics"
	"github.com/hanzoai/collab/pkg/room"
	"github.com/hanzoai/collab/pkg/store"
)

// Verifier validates IAM bearer tokens. Defined as an interface so tests
// can inject a fake without spinning up JWKS HTTP.
type Verifier interface {
	Verify(ctx context.Context, raw string) (*auth.Claims, error)
}

// Config bundles runtime config for Server.
type Config struct {
	Version string
	Verify  Verifier
	Store   store.Store
}

// Server holds all live state.
type Server struct {
	cfg      Config
	registry *room.Registry
}

// New builds a Server.
func New(cfg Config) *Server {
	return &Server{cfg: cfg, registry: room.NewRegistry(cfg.Store)}
}

// Handler returns the root mux: /v1/health, /v1/collab/:doc_id, /v1/metrics.
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/health", s.health)
	mux.Handle("/v1/metrics", metrics.Handler())
	mux.HandleFunc("/v1/collab/", s.collab)
	return mux
}

func (s *Server) health(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]string{
		"status":  "ok",
		"version": s.cfg.Version,
	})
}

// collab handles the WS upgrade after auth + authz.
//
// docID format is `<orgID>:<workspaceID>:<docID>`. The JWT's `owner`
// claim MUST equal the orgID segment.
func (s *Server) collab(w http.ResponseWriter, r *http.Request) {
	docID := strings.TrimPrefix(r.URL.Path, "/v1/collab/")
	if docID == "" || strings.Contains(docID, "/") {
		http.Error(w, "bad doc_id", http.StatusBadRequest)
		return
	}

	tok := auth.ExtractToken(r)
	if tok == "" {
		metrics.Errors.WithLabelValues("no_token").Inc()
		http.Error(w, "auth required", http.StatusUnauthorized)
		return
	}
	claims, err := s.cfg.Verify.Verify(r.Context(), tok)
	if err != nil {
		metrics.Errors.WithLabelValues("bad_token").Inc()
		http.Error(w, "invalid token", http.StatusUnauthorized)
		return
	}

	orgID, _, _, err := parseDocID(docID)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if claims.Owner != orgID {
		metrics.Errors.WithLabelValues("forbidden_org").Inc()
		http.Error(w, "forbidden", http.StatusForbidden)
		return
	}

	conn, err := websocket.Accept(w, r, &websocket.AcceptOptions{
		Subprotocols:    []string{"yjs"},
		OriginPatterns:  []string{"*"},
		CompressionMode: websocket.CompressionDisabled,
	})
	if err != nil {
		metrics.Errors.WithLabelValues("ws_accept").Inc()
		return
	}
	defer conn.CloseNow()

	s.serveWS(r.Context(), conn, docID, claims.Sub)
}

func parseDocID(docID string) (org, workspace, doc string, err error) {
	parts := strings.SplitN(docID, ":", 3)
	if len(parts) != 3 || parts[0] == "" || parts[1] == "" || parts[2] == "" {
		return "", "", "", errors.New("doc_id must be <orgID>:<workspaceID>:<docID>")
	}
	return parts[0], parts[1], parts[2], nil
}

// Per-connection limits. A frame larger than maxFrame closes the connection
// (1009); more than frameRate frames a second, beyond a burst of frameBurst,
// closes it with 1008. persistTimeout bounds one frame's append, which outlives
// the connection so a peer's last frame is stored even as it disconnects.
const (
	maxFrame       = 1 << 20
	frameRate      = 50
	frameBurst     = 200
	persistTimeout = 10 * time.Second
)

func (s *Server) serveWS(parent context.Context, conn *websocket.Conn, docID, peerID string) {
	ctx, cancel := context.WithCancel(parent)
	defer cancel()

	conn.SetReadLimit(maxFrame)

	peer := &room.Peer{ID: peerID, Send: make(chan []byte, 64)}
	rm, frames, err := s.registry.Join(ctx, docID, peer)
	if err != nil {
		metrics.Errors.WithLabelValues("join").Inc()
		conn.Close(websocket.StatusInternalError, "join failed")
		return
	}
	defer s.registry.Leave(docID, peer)

	// Replay persisted frames to the new peer, one message per frame, so a
	// y-websocket client decodes each of them.
	for _, f := range frames {
		if err := conn.Write(ctx, websocket.MessageBinary, f); err != nil {
			return
		}
		metrics.Messages.WithLabelValues("out").Inc()
	}

	// Writer: drain peer.Send into the socket.
	writerDone := make(chan struct{})
	go func() {
		defer close(writerDone)
		for {
			select {
			case <-ctx.Done():
				return
			case msg, ok := <-peer.Send:
				if !ok {
					return
				}
				wctx, c := context.WithTimeout(ctx, 10*time.Second)
				err := conn.Write(wctx, websocket.MessageBinary, msg)
				c()
				if err != nil {
					return
				}
				metrics.Messages.WithLabelValues("out").Inc()
			}
		}
	}()

	// Reader: forward inbound binary frames to the room, one at a time.
	limit := bucket{tokens: frameBurst, last: time.Now()}
	for {
		typ, data, err := conn.Read(ctx)
		if err != nil {
			break
		}
		if !limit.take(time.Now()) {
			metrics.Errors.WithLabelValues("rate_limited").Inc()
			conn.Close(websocket.StatusPolicyViolation, "rate limit")
			break
		}
		if typ != websocket.MessageBinary {
			continue
		}
		metrics.Messages.WithLabelValues("in").Inc()
		pctx, c := context.WithTimeout(context.WithoutCancel(ctx), persistTimeout)
		rm.Broadcast(pctx, peer, data)
		c()
	}

	cancel()
	<-writerDone
}

// bucket is a token bucket refilled at frameRate tokens a second up to
// frameBurst. One connection's reader owns it, so it needs no lock.
type bucket struct {
	tokens float64
	last   time.Time
}

// take spends one token, refilling first for the time since the last call. It
// reports false, spending nothing, when the bucket is empty.
func (b *bucket) take(now time.Time) bool {
	b.tokens = min(frameBurst, b.tokens+now.Sub(b.last).Seconds()*frameRate)
	b.last = now
	if b.tokens < 1 {
		return false
	}
	b.tokens--
	return true
}
