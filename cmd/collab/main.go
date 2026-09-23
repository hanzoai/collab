// Command collab is the Hanzo realtime collaboration relay.
//
// It accepts Y.js sync messages over WebSocket at /v1/collab/<doc_id>
// and relays them between peers in the same room, persisting document
// updates to SQLite. It replaces Huly's collaborator Node service.
package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/hanzoai/collab/pkg/auth"
	"github.com/hanzoai/collab/pkg/metrics"
	"github.com/hanzoai/collab/pkg/server"
	"github.com/hanzoai/collab/pkg/store"
)

// Version is set at build time via -ldflags "-X main.Version=v0.1.0".
var Version = "dev"

func main() {
	if err := run(); err != nil {
		log.Fatalf("collab: %v", err)
	}
}

func run() error {
	addr := envOr("COLLAB_ADDR", ":3078")
	jwksURL := envOr("IAM_JWKS_URL", "https://hanzo.id/v1/iam/.well-known/jwks")

	s, err := newStore()
	if err != nil {
		return fmt.Errorf("store: %w", err)
	}
	defer s.Close()

	metrics.Register(nil)

	srv := server.New(server.Config{
		Version: Version,
		Verify:  auth.New(jwksURL),
		Store:   s,
	})

	httpSrv := &http.Server{
		Addr:              addr,
		Handler:           srv.Handler(),
		ReadHeaderTimeout: 10 * time.Second,
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	errCh := make(chan error, 1)
	go func() {
		log.Printf("collab %s listening on %s (jwks=%s)", Version, addr, jwksURL)
		if err := httpSrv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errCh <- err
		}
		close(errCh)
	}()

	select {
	case <-ctx.Done():
		log.Println("collab: shutdown signal received")
	case err := <-errCh:
		if err != nil {
			return err
		}
	}

	shutCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	return httpSrv.Shutdown(shutCtx)
}

// maxDoc caps one document's stored frames; a frame that would pass it is
// relayed but not stored.
const maxDoc = 16 << 20

// newStore opens the SQLite store at COLLAB_SQLITE_PATH. COLLAB_STORE_BYTES
// caps the database file (default 64 MiB); the files on disk never exceed
// twice that, so size the volume from it.
func newStore() (store.Store, error) {
	maxFile, err := strconv.ParseInt(envOr("COLLAB_STORE_BYTES", "67108864"), 10, 64)
	if err != nil || maxFile <= 0 {
		return nil, fmt.Errorf("COLLAB_STORE_BYTES must be a positive byte count")
	}
	return store.NewSQLite(envOr("COLLAB_SQLITE_PATH", "collab.db"), maxDoc, maxFile)
}

func envOr(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}
