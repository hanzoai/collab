package auth

import (
	"net/http"
	"net/http/httptest"
	"testing"
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
