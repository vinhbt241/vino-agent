package api

import (
	"encoding/json"
	"fmt"
	"net/http"

	"vino/internal/agent"
	"vino/internal/llm"
	"vino/internal/tools"
)

// Server is the durable-agent HTTP server. It holds no per-request state and
// is safe for concurrent use as long as its LLM and tool registry are.
type Server struct {
	llmClient agent.LLM
	tools     agent.ToolRegistry
	mux       *http.ServeMux
}

// NewServer builds a Server from a validated Config, constructing the LLM
// client for the default provider and the tool registry.
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
	return newServer(client, registry), nil
}

// newServer wires an LLM and tool registry into a Server with its routes
// registered. Exposed as a function so tests can inject fakes.
func newServer(llmClient agent.LLM, registry agent.ToolRegistry) *Server {
	s := &Server{llmClient: llmClient, tools: registry}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", s.handleHealth)
	mux.HandleFunc("POST /runs", s.handleRun)
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
