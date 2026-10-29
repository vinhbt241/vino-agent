package llm

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"vino/internal/agent"
)

func TestNew_RequiresConfig(t *testing.T) {
	tests := []struct {
		name string
		cfg  Config
	}{
		{"empty APIKey", Config{BaseURL: "http://x", Model: "m"}},
		{"empty BaseURL", Config{APIKey: "k", Model: "m"}},
		{"empty Model", Config{APIKey: "k", BaseURL: "http://x"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := New(tt.cfg); err == nil {
				t.Fatal("New() error = nil, want error")
			}
		})
	}
}

// chunk encodes a delta as one SSE data line plus its blank terminator.
func chunk(delta chatDelta) string {
	b, _ := json.Marshal(chatChunk{Choices: []chatChoice{{Delta: delta}}})
	return "data: " + string(b) + "\n\n"
}

func TestChat_StreamsTextDeltas(t *testing.T) {
	var gotReq chatRequest
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer k" {
			t.Errorf("Authorization = %q, want %q", r.Header.Get("Authorization"), "Bearer k")
		}
		if r.URL.Path != "/chat/completions" {
			t.Errorf("path = %q, want /chat/completions", r.URL.Path)
		}
		if err := json.NewDecoder(r.Body).Decode(&gotReq); err != nil {
			t.Errorf("decode request: %v", err)
		}
		w.Header().Set("Content-Type", "text/event-stream")
		io.WriteString(w, chunk(chatDelta{Content: "Hel"}))
		io.WriteString(w, chunk(chatDelta{Content: "lo"}))
		io.WriteString(w, "data: [DONE]\n\n")
	}))
	defer srv.Close()

	client, err := New(Config{APIKey: "k", BaseURL: srv.URL, Model: "test-model"})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	var deltas []string
	resp, err := client.Chat(context.Background(), []agent.Message{{Role: agent.RoleUser, Content: "hi"}}, nil, func(d string) error {
		deltas = append(deltas, d)
		return nil
	})
	if err != nil {
		t.Fatalf("Chat() error = %v", err)
	}
	if resp.Content != "Hello" {
		t.Fatalf("Content = %q, want %q", resp.Content, "Hello")
	}
	if len(deltas) != 2 || deltas[0] != "Hel" || deltas[1] != "lo" {
		t.Fatalf("deltas = %#v, want [Hel lo]", deltas)
	}
	if gotReq.Model != "test-model" {
		t.Fatalf("request model = %q, want test-model", gotReq.Model)
	}
	if !gotReq.Stream {
		t.Fatal("request stream = false, want true")
	}
	if len(gotReq.Messages) != 1 || gotReq.Messages[0].Role != "user" || gotReq.Messages[0].Content != "hi" {
		t.Fatalf("request messages = %#v, want [user hi]", gotReq.Messages)
	}
}

func TestChat_AccumulatesToolCalls(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		io.WriteString(w, chunk(chatDelta{ToolCalls: []toolCallDelta{
			{Index: 0, ID: "call_1", Function: toolCallFunction{Name: "read", Arguments: `{"path":`}},
		}}))
		io.WriteString(w, chunk(chatDelta{ToolCalls: []toolCallDelta{
			{Index: 0, Function: toolCallFunction{Arguments: `"/etc/hosts"}`}},
		}}))
		io.WriteString(w, "data: [DONE]\n\n")
	}))
	defer srv.Close()

	client, err := New(Config{APIKey: "k", BaseURL: srv.URL, Model: "m"})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	resp, err := client.Chat(context.Background(), []agent.Message{{Role: agent.RoleUser, Content: "go"}}, nil, func(string) error { return nil })
	if err != nil {
		t.Fatalf("Chat() error = %v", err)
	}
	if len(resp.ToolCalls) != 1 {
		t.Fatalf("len(ToolCalls) = %d, want 1: %#v", len(resp.ToolCalls), resp.ToolCalls)
	}
	tc := resp.ToolCalls[0]
	if tc.ID != "call_1" || tc.Name != "read" {
		t.Fatalf("ToolCall = %#v, want id=call_1 name=read", tc)
	}
	if string(tc.Arguments) != `{"path":"/etc/hosts"}` {
		t.Fatalf("Arguments = %s, want accumulated JSON", tc.Arguments)
	}
}

func TestChat_MapsMessagesToWire(t *testing.T) {
	var gotReq chatRequest
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		json.NewDecoder(r.Body).Decode(&gotReq)
		w.Header().Set("Content-Type", "text/event-stream")
		io.WriteString(w, "data: [DONE]\n\n")
	}))
	defer srv.Close()

	client, err := New(Config{APIKey: "k", BaseURL: srv.URL, Model: "m"})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	msgs := []agent.Message{
		{Role: agent.RoleSystem, Content: "sys"},
		{Role: agent.RoleUser, Content: "q"},
		{Role: agent.RoleAssistant, Content: "", ToolCalls: []agent.ToolCall{{ID: "c1", Name: "read", Arguments: json.RawMessage(`{"path":"/x"}`)}}},
		{Role: agent.RoleTool, ToolCallID: "c1", Name: "read", Content: "result"},
	}
	_, err = client.Chat(context.Background(), msgs, nil, func(string) error { return nil })
	if err != nil {
		t.Fatalf("Chat() error = %v", err)
	}

	if len(gotReq.Messages) != 4 {
		t.Fatalf("len(Messages) = %d, want 4: %#v", len(gotReq.Messages), gotReq.Messages)
	}
	if gotReq.Messages[0].Role != "system" || gotReq.Messages[0].Content != "sys" {
		t.Fatalf("Messages[0] = %#v, want system sys", gotReq.Messages[0])
	}
	asst := gotReq.Messages[2]
	if asst.Role != "assistant" || len(asst.ToolCalls) != 1 {
		t.Fatalf("Messages[2] = %#v, want assistant with 1 tool call", asst)
	}
	ac := asst.ToolCalls[0]
	if ac.ID != "c1" || ac.Type != "function" || ac.Function.Name != "read" || ac.Function.Arguments != `{"path":"/x"}` {
		t.Fatalf("assistant tool call = %#v, want arguments as JSON string", ac)
	}
	tool := gotReq.Messages[3]
	if tool.Role != "tool" || tool.ToolCallID != "c1" || tool.Content != "result" {
		t.Fatalf("Messages[3] = %#v, want tool message with result", tool)
	}
}

func TestChat_EmitsContentForEmptyToolResult(t *testing.T) {
	var rawBody []byte
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		rawBody, _ = io.ReadAll(r.Body)
		w.Header().Set("Content-Type", "text/event-stream")
		io.WriteString(w, "data: [DONE]\n\n")
	}))
	defer srv.Close()

	client, err := New(Config{APIKey: "k", BaseURL: srv.URL, Model: "m"})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	msgs := []agent.Message{
		{Role: agent.RoleUser, Content: "clean up"},
		{Role: agent.RoleAssistant, Content: "", ToolCalls: []agent.ToolCall{{ID: "c1", Name: "bash", Arguments: json.RawMessage(`{"command":"rm -rf x"}`)}}},
		{Role: agent.RoleTool, ToolCallID: "c1", Name: "bash", Content: ""},
	}
	if _, err := client.Chat(context.Background(), msgs, nil, func(string) error { return nil }); err != nil {
		t.Fatalf("Chat() error = %v", err)
	}

	// DeepSeek and other strict OpenAI-compatible providers reject requests
	// where a tool (or tool-call-only assistant) message omits the content key.
	// The raw request body must contain "content":"" for empty tool results.
	if !strings.Contains(string(rawBody), `"content":""`) {
		t.Fatalf("request body missing empty content field: %s", rawBody)
	}
}

func TestChat_IncludesToolsInRequest(t *testing.T) {
	var gotReq chatRequest
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		json.NewDecoder(r.Body).Decode(&gotReq)
		w.Header().Set("Content-Type", "text/event-stream")
		io.WriteString(w, "data: [DONE]\n\n")
	}))
	defer srv.Close()

	client, err := New(Config{APIKey: "k", BaseURL: srv.URL, Model: "m"})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	specs := []agent.ToolSpec{{Name: "read", Description: "read a file", Parameters: json.RawMessage(`{"type":"object"}`)}}
	_, err = client.Chat(context.Background(), []agent.Message{{Role: agent.RoleUser, Content: "go"}}, specs, func(string) error { return nil })
	if err != nil {
		t.Fatalf("Chat() error = %v", err)
	}

	if len(gotReq.Tools) != 1 {
		t.Fatalf("len(Tools) = %d, want 1: %#v", len(gotReq.Tools), gotReq.Tools)
	}
	f := gotReq.Tools[0].Function
	if gotReq.Tools[0].Type != "function" || f.Name != "read" || f.Description != "read a file" || string(f.Parameters) != `{"type":"object"}` {
		t.Fatalf("Tools[0] = %#v, want function read with schema", gotReq.Tools[0])
	}
}

func TestChat_OmitsToolsWhenNone(t *testing.T) {
	var gotReq chatRequest
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		json.NewDecoder(r.Body).Decode(&gotReq)
		w.Header().Set("Content-Type", "text/event-stream")
		io.WriteString(w, "data: [DONE]\n\n")
	}))
	defer srv.Close()

	client, err := New(Config{APIKey: "k", BaseURL: srv.URL, Model: "m"})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	_, err = client.Chat(context.Background(), []agent.Message{{Role: agent.RoleUser, Content: "go"}}, nil, func(string) error { return nil })
	if err != nil {
		t.Fatalf("Chat() error = %v", err)
	}
	if gotReq.Tools != nil {
		t.Fatalf("Tools = %#v, want nil (omitted)", gotReq.Tools)
	}
}

func TestChat_Non2xxReturnsError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		io.WriteString(w, "boom")
	}))
	defer srv.Close()

	client, err := New(Config{APIKey: "k", BaseURL: srv.URL, Model: "m"})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	_, err = client.Chat(context.Background(), []agent.Message{{Role: agent.RoleUser, Content: "go"}}, nil, func(string) error { return nil })
	if err == nil || !strings.Contains(err.Error(), "500") || !strings.Contains(err.Error(), "boom") {
		t.Fatalf("Chat() error = %v, want status 500 with body", err)
	}
}

func TestChat_MalformedChunkReturnsError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		io.WriteString(w, "data: not-json\n\n")
	}))
	defer srv.Close()

	client, err := New(Config{APIKey: "k", BaseURL: srv.URL, Model: "m"})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	_, err = client.Chat(context.Background(), []agent.Message{{Role: agent.RoleUser, Content: "go"}}, nil, func(string) error { return nil })
	if err == nil {
		t.Fatal("Chat() error = nil, want decode error")
	}
}

func TestChat_ContextCancelled(t *testing.T) {
	client, err := New(Config{APIKey: "k", BaseURL: "http://127.0.0.1:1", Model: "m"})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err = client.Chat(ctx, []agent.Message{{Role: agent.RoleUser, Content: "go"}}, nil, func(string) error { return nil })
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Chat() error = %v, want context.Canceled", err)
	}
}

func TestChat_OnDeltaErrorAborts(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		io.WriteString(w, chunk(chatDelta{Content: "a"}))
	}))
	defer srv.Close()

	client, err := New(Config{APIKey: "k", BaseURL: srv.URL, Model: "m"})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	stop := errors.New("sink stop")
	_, err = client.Chat(context.Background(), []agent.Message{{Role: agent.RoleUser, Content: "go"}}, nil, func(string) error {
		return stop
	})
	if !errors.Is(err, stop) {
		t.Fatalf("Chat() error = %v, want sink stop error", err)
	}
}

func TestChat_Timeout(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(200 * time.Millisecond)
		w.Header().Set("Content-Type", "text/event-stream")
		io.WriteString(w, "data: [DONE]\n\n")
	}))
	defer srv.Close()

	client, err := New(Config{APIKey: "k", BaseURL: srv.URL, Model: "m", Timeout: 10 * time.Millisecond})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	_, err = client.Chat(context.Background(), []agent.Message{{Role: agent.RoleUser, Content: "go"}}, nil, func(string) error { return nil })
	if err == nil {
		t.Fatal("Chat() error = nil, want timeout error")
	}
}
