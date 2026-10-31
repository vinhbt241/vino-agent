package api

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"vino/internal/agent"
	"vino/internal/durable"
)

// blockLLM blocks its first Chat until the context is cancelled, holding a run
// in flight so a second submission can observe "busy".
type blockLLM struct {
	started chan struct{}
}

func (b *blockLLM) Chat(ctx context.Context, _ []agent.Message, _ []agent.ToolSpec, _ func(string) error) (agent.Response, error) {
	close(b.started)
	<-ctx.Done()
	return agent.Response{}, ctx.Err()
}

// steerLLM blocks its first Chat until the context is cancelled (mirroring an
// in-flight model request a steer interrupts), then returns a scripted
// response on the second call.
type steerLLM struct {
	started chan struct{}
	resp    agent.Response

	mu    sync.Mutex
	calls int
}

func (s *steerLLM) Chat(ctx context.Context, _ []agent.Message, _ []agent.ToolSpec, _ func(string) error) (agent.Response, error) {
	s.mu.Lock()
	s.calls++
	n := s.calls
	s.mu.Unlock()
	if n == 1 {
		close(s.started)
		<-ctx.Done()
		return agent.Response{}, ctx.Err()
	}
	return s.resp, nil
}

func TestResumeRejectWhenBusy(t *testing.T) {
	llm := &blockLLM{started: make(chan struct{})}
	s := newTestServer(t, llm, &fakeRegistry{})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sub, err := s.harness.Start(ctx, durable.StartInput{Messages: []agent.Message{{Role: agent.RoleUser, Content: "first"}}}, nil)
	if err != nil {
		t.Fatal(err)
	}
	go func() { _, _ = sub.Wait(ctx) }()
	<-llm.started

	id := formatID(sub.ConversationID())
	rr := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/conversations/"+id+"/runs", strings.NewReader(`{"messages":[{"role":"user","content":"rejected"}],"whenBusy":"reject"}`))
	req.Header.Set("Content-Type", "application/json")
	s.ServeHTTP(rr, req)

	if rr.Code != http.StatusConflict {
		t.Fatalf("status = %d, want 409", rr.Code)
	}
	if !strings.Contains(rr.Body.String(), "conversation is busy") {
		t.Fatalf("body = %q, want busy error", rr.Body.String())
	}
}

func TestResumeBadWhenBusy(t *testing.T) {
	s := newTestServer(t, &fakeLLM{}, &fakeRegistry{})
	sub, err := s.harness.Start(context.Background(), durable.StartInput{Messages: []agent.Message{{Role: agent.RoleUser, Content: "first"}}}, nil)
	if err != nil {
		t.Fatal(err)
	}
	id := formatID(sub.ConversationID())

	rr := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/conversations/"+id+"/runs", strings.NewReader(`{"messages":[{"role":"user","content":"x"}],"whenBusy":"bogus"}`))
	req.Header.Set("Content-Type", "application/json")
	s.ServeHTTP(rr, req)

	if rr.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", rr.Code)
	}
}

func TestResumeSteer(t *testing.T) {
	llm := &steerLLM{started: make(chan struct{}), resp: agent.Response{Content: "final"}}
	s := newTestServer(t, llm, &fakeRegistry{})
	ctx := context.Background()

	sub, err := s.harness.Start(ctx, durable.StartInput{Messages: []agent.Message{{Role: agent.RoleUser, Content: "first"}}}, nil)
	if err != nil {
		t.Fatal(err)
	}
	go func() { _, _ = sub.Wait(ctx) }()
	<-llm.started

	id := formatID(sub.ConversationID())
	rr := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/conversations/"+id+"/runs", strings.NewReader(`{"messages":[{"role":"user","content":"steered"}],"whenBusy":"steer"}`))
	req.Header.Set("Content-Type", "application/json")
	s.ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rr.Code)
	}
	frames := parseSSE(rr.Body.String())
	if len(frames) < 2 || frames[0].name != "run_started" {
		t.Fatalf("frames = %#v, want leading run_started", frames)
	}
	var started struct {
		Resumed bool `json:"resumed"`
	}
	if err := json.Unmarshal([]byte(frames[0].data), &started); err != nil {
		t.Fatal(err)
	}
	if !started.Resumed {
		t.Fatal("run_started.resumed = false, want true for a steer")
	}
	last := frames[len(frames)-1]
	if last.name != "run_finished" {
		t.Fatalf("last frame = %q, want run_finished", last.name)
	}
	if !strings.Contains(last.data, "final") {
		t.Fatalf("run_finished data = %q, want the steered answer \"final\"", last.data)
	}
}
