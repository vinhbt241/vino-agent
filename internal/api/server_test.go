package api

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"vino/internal/agent"
)

type sseFrame struct {
	name string
	data string
}

// parseSSE splits an SSE body into (event name, data JSON) pairs.
func parseSSE(body string) []sseFrame {
	var frames []sseFrame
	for _, block := range strings.Split(strings.TrimSpace(body), "\n\n") {
		var name, data string
		for _, line := range strings.Split(block, "\n") {
			switch {
			case strings.HasPrefix(line, "event: "):
				name = strings.TrimPrefix(line, "event: ")
			case strings.HasPrefix(line, "data: "):
				data = strings.TrimPrefix(line, "data: ")
			}
		}
		frames = append(frames, sseFrame{name: name, data: data})
	}
	return frames
}

func doRun(t *testing.T, s *Server, body string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(http.MethodPost, "/runs", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rr := httptest.NewRecorder()
	s.ServeHTTP(rr, req)
	return rr
}

func TestHealth(t *testing.T) {
	s := newTestServer(t, &fakeLLM{}, &fakeRegistry{})

	rr := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/health", nil)
	s.ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rr.Code)
	}
	if rr.Body.String() != "{\"ok\":true}\n" {
		t.Fatalf("body = %q, want {\"ok\":true}", rr.Body.String())
	}
}

func TestRun_StreamsTextAndFinish(t *testing.T) {
	llm := &fakeLLM{steps: []fakeStep{{deltas: []string{"Hel", "lo"}, resp: agent.Response{Content: "Hello"}}}}
	s := newTestServer(t, llm, &fakeRegistry{})

	rr := doRun(t, s, `{"systemPrompt":"be brief","messages":[{"role":"user","content":"hi"}]}`)

	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rr.Code)
	}
	if ct := rr.Header().Get("Content-Type"); !strings.HasPrefix(ct, "text/event-stream") {
		t.Fatalf("Content-Type = %q, want text/event-stream", ct)
	}

	frames := parseSSE(rr.Body.String())
	if len(frames) != 4 {
		t.Fatalf("len(frames) = %d, want 4: %#v", len(frames), frames)
	}
	if frames[0].name != "run_started" {
		t.Fatalf("frames[0] = %#v, want run_started", frames[0])
	}
	if frames[1].name != "text_delta" || frames[1].data != `{"text":"Hel"}` {
		t.Fatalf("frames[1] = %#v, want text_delta Hel", frames[1])
	}
	if frames[2].name != "text_delta" || frames[2].data != `{"text":"lo"}` {
		t.Fatalf("frames[2] = %#v, want text_delta lo", frames[2])
	}
	if frames[3].name != "run_finished" {
		t.Fatalf("frames[3] = %#v, want run_finished", frames[3])
	}
	var fin struct {
		FinalMessage agent.Message `json:"finalMessage"`
	}
	if err := json.Unmarshal([]byte(frames[3].data), &fin); err != nil {
		t.Fatalf("unmarshal run_finished data: %v", err)
	}
	if fin.FinalMessage.Content != "Hello" {
		t.Fatalf("finalMessage.content = %q, want Hello", fin.FinalMessage.Content)
	}
}

func TestRun_ToolCallEvents(t *testing.T) {
	llm := &fakeLLM{steps: []fakeStep{
		{resp: agent.Response{ToolCalls: []agent.ToolCall{{ID: "c1", Name: "read", Arguments: json.RawMessage(`{"path":"/x"}`)}}}},
		{resp: agent.Response{Content: "done"}},
	}}
	reg := &fakeRegistry{result: "file contents"}
	s := newTestServer(t, llm, reg)

	rr := doRun(t, s, `{"messages":[{"role":"user","content":"read /x"}]}`)

	frames := parseSSE(rr.Body.String())
	if len(frames) != 4 {
		t.Fatalf("len(frames) = %d, want 4: %#v", len(frames), frames)
	}
	if frames[0].name != "run_started" {
		t.Fatalf("frames[0] = %#v, want run_started", frames[0])
	}
	if frames[1].name != "tool_call_started" || frames[1].data != `{"id":"c1","name":"read","arguments":{"path":"/x"}}` {
		t.Fatalf("frames[1] = %#v, want tool_call_started", frames[1])
	}
	if frames[2].name != "tool_result" || frames[2].data != `{"id":"c1","name":"read","result":"file contents"}` {
		t.Fatalf("frames[2] = %#v, want tool_result", frames[2])
	}
	if frames[3].name != "run_finished" {
		t.Fatalf("frames[3] = %#v, want run_finished", frames[3])
	}
}

func TestRun_EmitsRunStartedFirst(t *testing.T) {
	llm := &fakeLLM{steps: []fakeStep{{resp: agent.Response{Content: "hi"}}}}
	s := newTestServer(t, llm, &fakeRegistry{})

	rr := doRun(t, s, `{"messages":[{"role":"user","content":"hi"}]}`)
	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rr.Code)
	}

	frames := parseSSE(rr.Body.String())
	if len(frames) < 2 {
		t.Fatalf("frames = %#v, want run_started then run_finished", frames)
	}
	if frames[0].name != "run_started" {
		t.Fatalf("first frame = %q, want run_started", frames[0].name)
	}
	var started struct {
		ConversationID string `json:"conversationId"`
		SubmissionID   string `json:"submissionId"`
		Resumed        bool   `json:"resumed"`
	}
	if err := json.Unmarshal([]byte(frames[0].data), &started); err != nil {
		t.Fatalf("unmarshal run_started: %v", err)
	}
	if started.ConversationID == "" || started.SubmissionID == "" {
		t.Fatalf("run_started ids = %#v, want non-empty strings", started)
	}
	if started.Resumed {
		t.Fatal("run_started.resumed = true, want false for a new run")
	}

	last := frames[len(frames)-1]
	if last.name != "run_finished" {
		t.Fatalf("last frame = %q, want run_finished", last.name)
	}
	var fin struct {
		FinalMessage   agent.Message `json:"finalMessage"`
		ConversationID string        `json:"conversationId"`
	}
	if err := json.Unmarshal([]byte(last.data), &fin); err != nil {
		t.Fatalf("unmarshal run_finished: %v", err)
	}
	if fin.ConversationID != started.ConversationID {
		t.Fatalf("run_finished.conversationId = %q, want %q", fin.ConversationID, started.ConversationID)
	}
}

func TestRun_EmitsRunStartedBeforeError(t *testing.T) {
	llm := &fakeLLM{steps: []fakeStep{{err: errors.New("boom")}}}
	s := newTestServer(t, llm, &fakeRegistry{})

	rr := doRun(t, s, `{"messages":[{"role":"user","content":"hi"}]}`)

	frames := parseSSE(rr.Body.String())
	if len(frames) != 2 {
		t.Fatalf("frames = %#v, want run_started then error", frames)
	}
	if frames[0].name != "run_started" {
		t.Fatalf("first frame = %q, want run_started", frames[0].name)
	}
	if frames[1].name != "error" {
		t.Fatalf("second frame = %q, want error", frames[1].name)
	}
}

func TestRun_LLMErrorEmitsErrorEvent(t *testing.T) {
	llm := &fakeLLM{steps: []fakeStep{{err: errors.New("llm down")}}}
	s := newTestServer(t, llm, &fakeRegistry{})

	rr := doRun(t, s, `{"messages":[{"role":"user","content":"hi"}]}`)

	frames := parseSSE(rr.Body.String())
	if len(frames) != 2 {
		t.Fatalf("len(frames) = %d, want 2: %#v", len(frames), frames)
	}
	if frames[0].name != "run_started" {
		t.Fatalf("frames[0].name = %q, want run_started", frames[0].name)
	}
	if frames[1].name != "error" {
		t.Fatalf("frames[1].name = %q, want error", frames[1].name)
	}
	var ev struct {
		Error string `json:"error"`
	}
	if err := json.Unmarshal([]byte(frames[1].data), &ev); err != nil {
		t.Fatalf("unmarshal error data: %v", err)
	}
	if ev.Error != "llm down" {
		t.Fatalf("error data = %q, want llm down", ev.Error)
	}
}

func TestRun_MaxIterationsEmitsErrorEvent(t *testing.T) {
	steps := make([]fakeStep, 5)
	for i := range steps {
		steps[i] = fakeStep{resp: agent.Response{ToolCalls: []agent.ToolCall{{ID: "c", Name: "nop", Arguments: json.RawMessage(`{}`)}}}}
	}
	llm := &fakeLLM{steps: steps}
	s := newTestServer(t, llm, &fakeRegistry{result: "ok"})

	rr := doRun(t, s, `{"messages":[{"role":"user","content":"go"}],"maxIterations":2}`)

	frames := parseSSE(rr.Body.String())
	if len(frames) < 1 || frames[len(frames)-1].name != "error" {
		t.Fatalf("frames = %#v, want final error frame", frames)
	}
	if !strings.Contains(frames[len(frames)-1].data, "max iterations") {
		t.Fatalf("error data = %q, want max iterations message", frames[len(frames)-1].data)
	}
}

func TestRun_ForwardsMessagesToLLM(t *testing.T) {
	llm := &fakeLLM{steps: []fakeStep{{resp: agent.Response{Content: "ok"}}}}
	s := newTestServer(t, llm, &fakeRegistry{})

	doRun(t, s, `{"systemPrompt":"sys","messages":[{"role":"user","content":"hi"},{"role":"assistant","content":"yo"}]}`)

	if len(llm.calls) != 1 {
		t.Fatalf("len(llm.calls) = %d, want 1", len(llm.calls))
	}
	msgs := llm.calls[0]
	if len(msgs) != 3 {
		t.Fatalf("len(messages) = %d, want 3: %#v", len(msgs), msgs)
	}
	if msgs[0].Role != agent.RoleSystem || msgs[0].Content != "sys" {
		t.Fatalf("msgs[0] = %#v, want system sys", msgs[0])
	}
	if msgs[1].Role != agent.RoleUser || msgs[1].Content != "hi" {
		t.Fatalf("msgs[1] = %#v, want user hi", msgs[1])
	}
	if msgs[2].Role != agent.RoleAssistant || msgs[2].Content != "yo" {
		t.Fatalf("msgs[2] = %#v, want assistant yo", msgs[2])
	}
}

func TestRun_ValidationErrors(t *testing.T) {
	tests := []struct {
		name string
		body string
	}{
		{"invalid json", `{`},
		{"empty messages", `{"messages":[]}`},
		{"bad role", `{"messages":[{"role":"system","content":"x"}]}`},
		{"empty content", `{"messages":[{"role":"user","content":""}]}`},
		{"bad timeout", `{"messages":[{"role":"user","content":"hi"}],"toolTimeout":"abc"}`},
		{"negative timeout", `{"messages":[{"role":"user","content":"hi"}],"toolTimeout":"-5s"}`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := newTestServer(t, &fakeLLM{}, &fakeRegistry{})
			rr := doRun(t, s, tt.body)
			if rr.Code != http.StatusBadRequest {
				t.Fatalf("status = %d, want 400", rr.Code)
			}
		})
	}
}

func TestParseTimeout(t *testing.T) {
	tests := []struct {
		in      string
		want    time.Duration
		wantErr bool
	}{
		{"", 0, false},
		{"30s", 30 * time.Second, false},
		{"1m", time.Minute, false},
		{"abc", 0, true},
		{"-5s", 0, true},
	}
	for _, tt := range tests {
		t.Run(tt.in, func(t *testing.T) {
			got, err := parseTimeout(tt.in)
			if (err != nil) != tt.wantErr {
				t.Fatalf("parseTimeout(%q) error = %v, wantErr %v", tt.in, err, tt.wantErr)
			}
			if got != tt.want {
				t.Fatalf("parseTimeout(%q) = %v, want %v", tt.in, got, tt.want)
			}
		})
	}
}

func TestRun_RequestIDDedup(t *testing.T) {
	llm := &fakeLLM{steps: []fakeStep{{resp: agent.Response{Content: "answer"}}}}
	s := newTestServer(t, llm, &fakeRegistry{})
	body := `{"requestId":"job-1","messages":[{"role":"user","content":"hi"}]}`

	rr1 := doRun(t, s, body)
	rr2 := doRun(t, s, body)

	frames1 := parseSSE(rr1.Body.String())
	if len(frames1) != 2 || frames1[0].name != "run_started" || frames1[1].name != "run_finished" {
		t.Fatalf("first frames = %#v, want [run_started run_finished]", frames1)
	}
	frames2 := parseSSE(rr2.Body.String())
	if len(frames2) != 2 || frames2[0].name != "run_started" || frames2[1].name != "run_finished" {
		t.Fatalf("retry frames = %#v, want [run_started run_finished] replayed from storage", frames2)
	}
	var started struct {
		Resumed bool `json:"resumed"`
	}
	if err := json.Unmarshal([]byte(frames2[0].data), &started); err != nil {
		t.Fatalf("unmarshal retry run_started: %v", err)
	}
	if !started.Resumed {
		t.Fatalf("retry run_started.resumed = false, want true")
	}
	if !strings.Contains(frames2[1].data, "answer") {
		t.Fatalf("retry frame data = %q, want stored answer", frames2[1].data)
	}
	if len(llm.calls) != 1 {
		t.Fatalf("llm calls = %d, want 1 (dedup must not start a second run)", len(llm.calls))
	}
}

func TestNewServer_WiresDurableStore(t *testing.T) {
	dir := t.TempDir()
	cfgPath := filepath.Join(dir, "vino.json")
	cfg := `{
		"addr": "127.0.0.1:0",
		"default": "openai",
		"providers": {"openai": {"apiKey": "k", "baseURL": "https://example.com/v1", "model": "m"}},
		"tools": {"rootDir": "` + dir + `", "workDir": "` + dir + `"},
		"durablePath": "` + filepath.Join(dir, "vino.jsonl") + `"
	}`
	if err := os.WriteFile(cfgPath, []byte(cfg), 0o600); err != nil {
		t.Fatal(err)
	}
	loaded, err := LoadConfig(cfgPath)
	if err != nil {
		t.Fatal(err)
	}
	s, err := NewServer(loaded)
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}
	rr := httptest.NewRecorder()
	s.ServeHTTP(rr, httptest.NewRequest(http.MethodGet, "/health", nil))
	if rr.Code != http.StatusOK {
		t.Fatalf("health status = %d, want 200", rr.Code)
	}
	// The durable log was created.
	if _, err := os.Stat(loaded.DurablePath); err != nil {
		t.Fatalf("durable log not created: %v", err)
	}
}

func TestRestartRecovery_ResumesInterruptedRun(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "vino.jsonl")
	body := `{"requestId":"job-1","messages":[{"role":"user","content":"find"}]}`

	// --- First server: a run that dies mid-tool-call ---
	llm1 := &fakeLLM{steps: []fakeStep{
		{resp: agent.Response{ToolCalls: []agent.ToolCall{{ID: "c1", Name: "search", Arguments: json.RawMessage(`{}`)}}}},
	}}
	reg1 := &blockingRegistry{started: make(chan struct{}), release: make(chan struct{})}
	h1 := openHarness(t, path, llm1, reg1)
	s1 := newServer(h1)

	unwound := make(chan struct{})
	go func() {
		defer close(unwound)
		req := httptest.NewRequest(http.MethodPost, "/runs", strings.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		// Blocks inside the tool; abandoned like a dead process.
		s1.ServeHTTP(httptest.NewRecorder(), req)
	}()

	select {
	case <-reg1.started:
	case <-time.After(10 * time.Second):
		t.Fatal("tool never started")
	}

	// Crash: close the first process's storage mid-run.
	if err := h1.Close(context.Background()); err != nil {
		t.Fatalf("close crashed harness: %v", err)
	}

	// --- Second server over the same log resumes the run ---
	llm2 := &fakeLLM{steps: []fakeStep{{resp: agent.Response{Content: "recovered"}}}}
	reg2 := &fakeRegistry{result: "hit"}
	h2 := openHarness(t, path, llm2, reg2)
	s2 := newServer(h2)

	rr := doRun(t, s2, body)
	if rr.Code != http.StatusOK {
		t.Fatalf("resumed run status = %d, want 200", rr.Code)
	}
	frames := parseSSE(rr.Body.String())
	if len(frames) < 2 {
		t.Fatalf("frames = %#v, want tool_result then run_finished", frames)
	}
	if frames[len(frames)-1].name != "run_finished" {
		t.Fatalf("last frame = %#v, want run_finished", frames[len(frames)-1])
	}
	if !strings.Contains(frames[len(frames)-1].data, "recovered") {
		t.Fatalf("run_finished data = %q, want recovered answer", frames[len(frames)-1].data)
	}
	// The safe tool re-ran in the new process.
	foundToolResult := false
	for _, fr := range frames {
		if fr.name == "tool_result" && strings.Contains(fr.data, "hit") {
			foundToolResult = true
		}
	}
	if !foundToolResult {
		t.Fatalf("frames = %#v, want a tool_result with the re-run tool output", frames)
	}

	// Cleanup: release the first process's blocked tool so its goroutine can
	// unwind (its storage is closed, so the commit fails and it just exits).
	close(reg1.release)
	select {
	case <-unwound:
	case <-time.After(10 * time.Second):
		t.Fatal("crashed run goroutine did not unwind")
	}
}
