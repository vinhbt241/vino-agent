package api

import (
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
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
	s := newServer(&fakeLLM{}, &fakeRegistry{})

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
	s := newServer(llm, &fakeRegistry{})

	rr := doRun(t, s, `{"systemPrompt":"be brief","messages":[{"role":"user","content":"hi"}]}`)

	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rr.Code)
	}
	if ct := rr.Header().Get("Content-Type"); !strings.HasPrefix(ct, "text/event-stream") {
		t.Fatalf("Content-Type = %q, want text/event-stream", ct)
	}

	frames := parseSSE(rr.Body.String())
	if len(frames) != 3 {
		t.Fatalf("len(frames) = %d, want 3: %#v", len(frames), frames)
	}
	if frames[0].name != "text_delta" || frames[0].data != `{"text":"Hel"}` {
		t.Fatalf("frames[0] = %#v, want text_delta Hel", frames[0])
	}
	if frames[1].name != "text_delta" || frames[1].data != `{"text":"lo"}` {
		t.Fatalf("frames[1] = %#v, want text_delta lo", frames[1])
	}
	if frames[2].name != "run_finished" {
		t.Fatalf("frames[2] = %#v, want run_finished", frames[2])
	}
	var fin struct {
		FinalMessage agent.Message `json:"finalMessage"`
	}
	if err := json.Unmarshal([]byte(frames[2].data), &fin); err != nil {
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
	s := newServer(llm, reg)

	rr := doRun(t, s, `{"messages":[{"role":"user","content":"read /x"}]}`)

	frames := parseSSE(rr.Body.String())
	if len(frames) != 3 {
		t.Fatalf("len(frames) = %d, want 3: %#v", len(frames), frames)
	}
	if frames[0].name != "tool_call_started" || frames[0].data != `{"id":"c1","name":"read","arguments":{"path":"/x"}}` {
		t.Fatalf("frames[0] = %#v, want tool_call_started", frames[0])
	}
	if frames[1].name != "tool_result" || frames[1].data != `{"id":"c1","name":"read","result":"file contents"}` {
		t.Fatalf("frames[1] = %#v, want tool_result", frames[1])
	}
	if frames[2].name != "run_finished" {
		t.Fatalf("frames[2] = %#v, want run_finished", frames[2])
	}
}

func TestRun_LLMErrorEmitsErrorEvent(t *testing.T) {
	llm := &fakeLLM{steps: []fakeStep{{err: errors.New("llm down")}}}
	s := newServer(llm, &fakeRegistry{})

	rr := doRun(t, s, `{"messages":[{"role":"user","content":"hi"}]}`)

	frames := parseSSE(rr.Body.String())
	if len(frames) != 1 {
		t.Fatalf("len(frames) = %d, want 1: %#v", len(frames), frames)
	}
	if frames[0].name != "error" {
		t.Fatalf("frames[0].name = %q, want error", frames[0].name)
	}
	var ev struct {
		Error string `json:"error"`
	}
	if err := json.Unmarshal([]byte(frames[0].data), &ev); err != nil {
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
	s := newServer(llm, &fakeRegistry{result: "ok"})

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
	s := newServer(llm, &fakeRegistry{})

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
			s := newServer(&fakeLLM{}, &fakeRegistry{})
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
