package auth

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"math/big"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
)

func TestExtractToken(t *testing.T) {
	cases := []struct {
		name string
		req  func() *http.Request
		want string
	}{
		{
			name: "bearer header",
			req: func() *http.Request {
				r := httptest.NewRequest("GET", "/v1/collab/x", nil)
				r.Header.Set("Authorization", "Bearer abc.def.ghi")
				return r
			},
			want: "abc.def.ghi",
		},
		{
			name: "bearer header lowercase",
			req: func() *http.Request {
				r := httptest.NewRequest("GET", "/v1/collab/x", nil)
				r.Header.Set("Authorization", "bearer foo")
				return r
			},
			want: "foo",
		},
		{
			name: "query param",
			req: func() *http.Request {
				return httptest.NewRequest("GET", "/v1/collab/x?token=q1.q2.q3", nil)
			},
			want: "q1.q2.q3",
		},
		{
			name: "none",
			req: func() *http.Request {
				return httptest.NewRequest("GET", "/v1/collab/x", nil)
			},
			want: "",
		},
		{
			name: "header beats query",
			req: func() *http.Request {
				r := httptest.NewRequest("GET", "/v1/collab/x?token=q", nil)
				r.Header.Set("Authorization", "Bearer h")
				return r
			},
			want: "h",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := ExtractToken(tc.req())
			if got != tc.want {
				t.Fatalf("got %q want %q", got, tc.want)
			}
		})
	}
}

// jwksServer serves one RSA key under kid and counts the fetches it answers.
func jwksServer(t *testing.T, kid string, pub *rsa.PublicKey) (*httptest.Server, *atomic.Int32) {
	t.Helper()
	var fetches atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		fetches.Add(1)
		_ = json.NewEncoder(w).Encode(map[string]any{"keys": []map[string]string{{
			"kty": "RSA",
			"kid": kid,
			"n":   base64.RawURLEncoding.EncodeToString(pub.N.Bytes()),
			"e":   base64.RawURLEncoding.EncodeToString(big.NewInt(int64(pub.E)).Bytes()),
		}}})
	}))
	t.Cleanup(srv.Close)
	return srv, &fetches
}

func sign(t *testing.T, key *rsa.PrivateKey, kid string) string {
	t.Helper()
	tok := jwt.NewWithClaims(jwt.SigningMethodRS256, &Claims{
		Owner:            "hanzo",
		RegisteredClaims: jwt.RegisteredClaims{ExpiresAt: jwt.NewNumericDate(time.Now().Add(time.Hour))},
	})
	tok.Header["kid"] = kid
	raw, err := tok.SignedString(key)
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

// A token naming an unknown kid must not buy a JWKS fetch: inside the
// refresh window every such token is refused on the cached set.
func TestUnknownKidFetchesAtMostOncePerWindow(t *testing.T) {
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	srv, fetches := jwksServer(t, "good", &key.PublicKey)
	v := New(srv.URL)

	if _, err := v.Verify(context.Background(), sign(t, key, "good")); err != nil {
		t.Fatalf("valid token refused: %v", err)
	}
	var wg sync.WaitGroup
	for i := range 50 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, err := v.Verify(context.Background(), sign(t, key, fmt.Sprintf("forged-%d", i))); err == nil {
				t.Error("token with unknown kid accepted")
			}
		}()
	}
	wg.Wait()
	if n := fetches.Load(); n != 1 {
		t.Fatalf("JWKS fetched %d times, want 1", n)
	}
	if _, err := v.Verify(context.Background(), sign(t, key, "good")); err != nil {
		t.Fatalf("valid token refused after forged ones: %v", err)
	}
}

// Past the window an unknown kid fetches again, once, so a key IAM has just
// rotated in is picked up.
func TestUnknownKidRefetchesAfterWindow(t *testing.T) {
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	srv, fetches := jwksServer(t, "good", &key.PublicKey)
	v := New(srv.URL)
	v.minRefresh = 50 * time.Millisecond

	if _, err := v.Verify(context.Background(), sign(t, key, "new")); err == nil {
		t.Fatal("token with unknown kid accepted")
	}
	time.Sleep(60 * time.Millisecond)
	for range 5 {
		_, _ = v.Verify(context.Background(), sign(t, key, "new"))
	}
	if n := fetches.Load(); n != 2 {
		t.Fatalf("JWKS fetched %d times, want 2", n)
	}
}

// A handshake whose context is already cancelled must not record a failed
// fetch that every other caller inside the window would then inherit.
func TestCancelledCallerDoesNotPoisonWindow(t *testing.T) {
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	srv, _ := jwksServer(t, "good", &key.PublicKey)
	v := New(srv.URL)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := v.Verify(ctx, sign(t, key, "good")); err != nil {
		t.Fatalf("cancelled caller refused a valid token: %v", err)
	}
	if _, err := v.Verify(context.Background(), sign(t, key, "good")); err != nil {
		t.Fatalf("valid token refused after a cancelled caller: %v", err)
	}
}
