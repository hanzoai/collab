// Command collab is the Hanzo realtime collaboration relay.
//
// It accepts Y.js sync messages over WebSocket at /v1/collab/<doc_id>
// and relays them between peers in the same room, persisting updates
// to a pluggable store. It replaces Huly's collaborator Node service.
package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
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

func newStore() (store.Store, error) {
	backend := strings.ToLower(envOr("COLLAB_STORAGE", "sqlite"))
	switch backend {
	case "sqlite":
		path := envOr("COLLAB_SQLITE_PATH", "collab.db")
		return store.NewSQLite(path)
	case "s3":
		return store.NewS3(context.Background(), store.S3Config{
			Endpoint:  os.Getenv("S3_ENDPOINT"),
			Region:    os.Getenv("S3_REGION"),
			Bucket:    os.Getenv("S3_BUCKET"),
			AccessKey: os.Getenv("S3_ACCESS_KEY"),
			SecretKey: os.Getenv("S3_SECRET_KEY"),
		})
	default:
		return nil, fmt.Errorf("unknown COLLAB_STORAGE=%q", backend)
	}
}

func envOr(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}
