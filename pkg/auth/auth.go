// Package auth verifies Hanzo IAM JWTs via the published JWKS.
//
// We do NOT share code with team-go/extract because the WS upgrade
// path needs to extract the token from either the Authorization header
// or the ?token= query string (browsers cannot set headers on the WS
// handshake), and we want a tiny self-contained dependency surface.
package auth

import (
	"context"
	"crypto/rsa"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"math/big"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/golang-jwt/jwt/v5"
)

// Claims is the subset of IAM token claims the collab service uses.
//
// `owner` is the org slug (Hanzo IAM convention — see HIP-0026).
type Claims struct {
	Sub   string `json:"sub"`
	Email string `json:"email"`
	Owner string `json:"owner"`
	jwt.RegisteredClaims
}

// Verifier resolves JWKS keys and validates tokens. Safe for concurrent use.
type Verifier struct {
	jwksURL string
	client  *http.Client

	mu      sync.RWMutex
	keys    map[string]*rsa.PublicKey
	fetched time.Time
	ttl     time.Duration

	// A token whose kid the cached set lacks forces a fetch, and anyone can
	// send one, so fetches are spaced at least minRefresh apart: a caller
	// inside the window gets the last fetch's outcome instead of a new fetch.
	refreshMu  sync.Mutex
	attempted  time.Time
	lastErr    error
	minRefresh time.Duration
}

// New constructs a Verifier against the IAM JWKS URL.
func New(jwksURL string) *Verifier {
	return &Verifier{
		jwksURL:    jwksURL,
		client:     &http.Client{Timeout: 10 * time.Second},
		keys:       map[string]*rsa.PublicKey{},
		ttl:        10 * time.Minute,
		minRefresh: 30 * time.Second,
	}
}

// Verify parses + validates a raw bearer token, returning the typed claims.
func (v *Verifier) Verify(ctx context.Context, raw string) (*Claims, error) {
	claims := &Claims{}
	tok, err := jwt.ParseWithClaims(raw, claims, func(t *jwt.Token) (any, error) {
		if _, ok := t.Method.(*jwt.SigningMethodRSA); !ok {
			return nil, fmt.Errorf("unexpected signing method: %v", t.Header["alg"])
		}
		kid, _ := t.Header["kid"].(string)
		return v.lookupKey(ctx, kid)
	})
	if err != nil {
		return nil, err
	}
	if !tok.Valid {
		return nil, errors.New("token invalid")
	}
	return claims, nil
}

// ExtractToken pulls a bearer token from the Authorization header or
// the `token` query parameter. WS upgrades from browsers MUST use the
// query string because the browser WS API forbids custom headers.
func ExtractToken(r *http.Request) string {
	if h := r.Header.Get("Authorization"); h != "" {
		if strings.HasPrefix(strings.ToLower(h), "bearer ") {
			return strings.TrimSpace(h[7:])
		}
	}
	return r.URL.Query().Get("token")
}

func (v *Verifier) lookupKey(ctx context.Context, kid string) (*rsa.PublicKey, error) {
	v.mu.RLock()
	if k, ok := v.keys[kid]; ok && time.Since(v.fetched) < v.ttl {
		v.mu.RUnlock()
		return k, nil
	}
	v.mu.RUnlock()

	if err := v.refresh(ctx); err != nil {
		return nil, err
	}
	v.mu.RLock()
	defer v.mu.RUnlock()
	k, ok := v.keys[kid]
	if !ok {
		return nil, fmt.Errorf("kid %q not found", kid)
	}
	return k, nil
}

type jwk struct {
	Kty string `json:"kty"`
	Kid string `json:"kid"`
	N   string `json:"n"`
	E   string `json:"e"`
}

type jwks struct {
	Keys []jwk `json:"keys"`
}

// refresh fetches the JWKS at most once per minRefresh and returns that
// fetch's result to every caller inside the window. The fetch ignores the
// caller's cancellation, so one aborted handshake cannot record a failure
// for everyone; the client timeout still bounds it.
func (v *Verifier) refresh(ctx context.Context) error {
	v.refreshMu.Lock()
	defer v.refreshMu.Unlock()
	if time.Since(v.attempted) < v.minRefresh {
		return v.lastErr
	}
	v.attempted = time.Now()
	v.lastErr = v.fetch(context.WithoutCancel(ctx))
	return v.lastErr
}

func (v *Verifier) fetch(ctx context.Context) error {
	u, err := url.Parse(v.jwksURL)
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
	if err != nil {
		return err
	}
	resp, err := v.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("jwks fetch: %s", resp.Status)
	}
	var doc jwks
	if err := json.NewDecoder(resp.Body).Decode(&doc); err != nil {
		return err
	}
	next := map[string]*rsa.PublicKey{}
	for _, k := range doc.Keys {
		if k.Kty != "RSA" {
			continue
		}
		pk, err := decodeRSA(k.N, k.E)
		if err != nil {
			continue
		}
		next[k.Kid] = pk
	}
	v.mu.Lock()
	v.keys = next
	v.fetched = time.Now()
	v.mu.Unlock()
	return nil
}

func decodeRSA(nB64, eB64 string) (*rsa.PublicKey, error) {
	n, err := base64.RawURLEncoding.DecodeString(nB64)
	if err != nil {
		return nil, err
	}
	e, err := base64.RawURLEncoding.DecodeString(eB64)
	if err != nil {
		return nil, err
	}
	ei := new(big.Int).SetBytes(e).Int64()
	return &rsa.PublicKey{
		N: new(big.Int).SetBytes(n),
		E: int(ei),
	}, nil
}
