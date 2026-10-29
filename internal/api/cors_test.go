package api

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"vino/internal/agent"
)

func corsRequest(method, path, origin string, headers map[string]string) *http.Request {
	req := httptest.NewRequest(method, path, nil)
	if origin != "" {
		req.Header.Set("Origin", origin)
	}
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	return req
}

func TestCORS_DisabledByDefault(t *testing.T) {
	s := newTestServerCORS(t, &fakeLLM{}, &fakeRegistry{}, CORSConfig{})

	rr := httptest.NewRecorder()
	s.ServeHTTP(rr, corsRequest(http.MethodGet, "/health", "http://localhost:5173", nil))
	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rr.Code)
	}
	if got := rr.Header().Get("Access-Control-Allow-Origin"); got != "" {
		t.Fatalf("Access-Control-Allow-Origin = %q, want empty (CORS disabled)", got)
	}

	// Preflight falls through to the mux, which rejects the unmatched method.
	rr = httptest.NewRecorder()
	s.ServeHTTP(rr, corsRequest(http.MethodOptions, "/runs", "http://localhost:5173", map[string]string{"Access-Control-Request-Method": "POST"}))
	if rr.Code != http.StatusMethodNotAllowed {
		t.Fatalf("OPTIONS status = %d, want 405 (no preflight handling)", rr.Code)
	}
	if got := rr.Header().Get("Access-Control-Allow-Origin"); got != "" {
		t.Fatalf("Access-Control-Allow-Origin = %q, want empty", got)
	}
}

func TestCORS_WildcardOrigin(t *testing.T) {
	s := newTestServerCORS(t, &fakeLLM{}, &fakeRegistry{}, CORSConfig{Origins: []string{"*"}})

	rr := httptest.NewRecorder()
	s.ServeHTTP(rr, corsRequest(http.MethodGet, "/health", "http://localhost:5173", nil))

	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rr.Code)
	}
	if got := rr.Header().Get("Access-Control-Allow-Origin"); got != "*" {
		t.Fatalf("Access-Control-Allow-Origin = %q, want *", got)
	}
	if got := rr.Header().Get("Vary"); got != "" {
		t.Fatalf("Vary = %q, want empty for wildcard", got)
	}
}

func TestCORS_Preflight(t *testing.T) {
	s := newTestServerCORS(t, &fakeLLM{}, &fakeRegistry{}, CORSConfig{Origins: []string{"*"}})

	rr := httptest.NewRecorder()
	s.ServeHTTP(rr, corsRequest(http.MethodOptions, "/runs", "http://localhost:5173", map[string]string{
		"Access-Control-Request-Method":  "POST",
		"Access-Control-Request-Headers": "content-type, x-trace",
	}))

	if rr.Code != http.StatusNoContent {
		t.Fatalf("status = %d, want 204", rr.Code)
	}
	if got := rr.Header().Get("Access-Control-Allow-Origin"); got != "*" {
		t.Fatalf("Access-Control-Allow-Origin = %q, want *", got)
	}
	if got := rr.Header().Get("Access-Control-Allow-Methods"); got != "GET, POST, OPTIONS" {
		t.Fatalf("Access-Control-Allow-Methods = %q, want GET, POST, OPTIONS", got)
	}
	allowHeaders := rr.Header().Get("Access-Control-Allow-Headers")
	for _, want := range []string{"accept", "authorization", "content-type", "x-trace"} {
		if !strings.Contains(allowHeaders, want) {
			t.Fatalf("Access-Control-Allow-Headers = %q, want it to contain %q", allowHeaders, want)
		}
	}
	if got := rr.Header().Get("Access-Control-Max-Age"); got != "600" {
		t.Fatalf("Access-Control-Max-Age = %q, want 600", got)
	}
}

func TestCORS_SpecificOriginEchoed(t *testing.T) {
	s := newTestServerCORS(t, &fakeLLM{}, &fakeRegistry{}, CORSConfig{Origins: []string{"http://localhost:5173"}})

	rr := httptest.NewRecorder()
	s.ServeHTTP(rr, corsRequest(http.MethodGet, "/health", "http://localhost:5173", nil))

	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rr.Code)
	}
	if got := rr.Header().Get("Access-Control-Allow-Origin"); got != "http://localhost:5173" {
		t.Fatalf("Access-Control-Allow-Origin = %q, want http://localhost:5173", got)
	}
	if got := rr.Header().Get("Vary"); got != "Origin" {
		t.Fatalf("Vary = %q, want Origin", got)
	}
}

func TestCORS_DisallowedOriginRejected(t *testing.T) {
	s := newTestServerCORS(t, &fakeLLM{}, &fakeRegistry{}, CORSConfig{Origins: []string{"http://localhost:5173"}})

	rr := httptest.NewRecorder()
	s.ServeHTTP(rr, corsRequest(http.MethodPost, "/runs", "http://evil.example", map[string]string{"Content-Type": "application/json"}))

	if rr.Code != http.StatusForbidden {
		t.Fatalf("status = %d, want 403", rr.Code)
	}
	if got := rr.Header().Get("Access-Control-Allow-Origin"); got != "" {
		t.Fatalf("Access-Control-Allow-Origin = %q, want empty for rejected origin", got)
	}
}

func TestCORS_MissingOriginRejectedForList(t *testing.T) {
	s := newTestServerCORS(t, &fakeLLM{}, &fakeRegistry{}, CORSConfig{Origins: []string{"http://localhost:5173"}})

	rr := httptest.NewRecorder()
	s.ServeHTTP(rr, corsRequest(http.MethodGet, "/health", "", nil))

	if rr.Code != http.StatusForbidden {
		t.Fatalf("status = %d, want 403 for a missing Origin with a specific list", rr.Code)
	}
}

func TestCORS_AllowCredentials(t *testing.T) {
	s := newTestServerCORS(t, &fakeLLM{}, &fakeRegistry{}, CORSConfig{
		Origins:          []string{"http://localhost:5173"},
		AllowCredentials: true,
	})

	rr := httptest.NewRecorder()
	s.ServeHTTP(rr, corsRequest(http.MethodGet, "/health", "http://localhost:5173", nil))

	if got := rr.Header().Get("Access-Control-Allow-Credentials"); got != "true" {
		t.Fatalf("Access-Control-Allow-Credentials = %q, want true", got)
	}
}

func TestCORS_SSEResponseCarriesHeader(t *testing.T) {
	llm := &fakeLLM{steps: []fakeStep{{resp: agent.Response{Content: "hi"}}}}
	s := newTestServerCORS(t, llm, &fakeRegistry{}, CORSConfig{Origins: []string{"*"}})

	req := httptest.NewRequest(http.MethodPost, "/runs", strings.NewReader(`{"messages":[{"role":"user","content":"hi"}]}`))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Origin", "http://localhost:5173")
	rr := httptest.NewRecorder()
	s.ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rr.Code)
	}
	if got := rr.Header().Get("Access-Control-Allow-Origin"); got != "*" {
		t.Fatalf("Access-Control-Allow-Origin = %q, want * on the SSE response", got)
	}
	if ct := rr.Header().Get("Content-Type"); !strings.HasPrefix(ct, "text/event-stream") {
		t.Fatalf("Content-Type = %q, want text/event-stream", ct)
	}
}
