package api

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"

	"vino/internal/durable"
	"vino/internal/llm"
	"vino/internal/tools"
)

// Server is the durable-agent HTTP server. It routes /health and /runs, and
// runs every request through a durable harness so runs survive restarts.
type Server struct {
	harness *durable.Harness
	mux     *http.ServeMux
}

// NewServer builds a Server from a validated Config: it constructs the LLM
// client and tool registry, opens a harness over the durable JSONL log, and
// reconciles any run left mid-flight by a previous process.
func NewServer(cfg Config) (*Server, error) {
	p := cfg.Providers[cfg.Default]
	client, err := llm.New(llm.Config{APIKey: p.APIKey, BaseURL: p.BaseURL, Model: p.Model})
	if err != nil {
		return nil, fmt.Errorf("api: default provider: %w", err)
	}
	registry, err := tools.New(tools.Options{RootDir: cfg.Tools.RootDir, WorkDir: cfg.Tools.WorkDir})
	if err != nil {
		return nil, fmt.Errorf("api: tools: %w", err)
	}

	store, err := durable.OpenJSONL(cfg.DurablePath, false)
	if err != nil {
		return nil, fmt.Errorf("api: durable store: %w", err)
	}
	harness, err := durable.Open(context.Background(), store, durable.HarnessOptions{
		LLM:   client,
		Tools: registry,
		Model: p.Model,
	})
	if err != nil {
		return nil, fmt.Errorf("api: durable harness: %w", err)
	}
	return newServer(harness), nil
}

// newServer wires a harness into a Server with its routes registered. Exposed
// as a function so tests can inject a harness over in-memory storage.
func newServer(harness *durable.Harness) *Server {
	s := &Server{harness: harness}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", s.handleHealth)
	mux.HandleFunc("POST /runs", s.handleRun)
	mux.HandleFunc("GET /conversations", s.handleListConversations)
	mux.HandleFunc("GET /conversations/{id}", s.handleGetConversation)
	mux.HandleFunc("POST /conversations/{id}/runs", s.handleResumeConversation)
	s.mux = mux
	return s
}

func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.mux.ServeHTTP(w, r)
}

func (s *Server) handleHealth(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]bool{"ok": true})
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}
