package agent

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
)

func TestRun_NoToolCalls_StreamsTextAndFinishes(t *testing.T) {
	llm := &fakeLLM{
		steps: []fakeStep{
			{deltas: []string{"Hel", "lo"}, resp: Response{Content: "Hello"}},
		},
	}
	a := Agent{LLM: llm}

	var events []Event
	sink := func(e Event) error {
		events = append(events, e)
		return nil
	}

	res, err := a.Run(context.Background(), RunInput{
		Model:        "test-model",
		SystemPrompt: "You are a test.",
		Messages:     []Message{{Role: RoleUser, Content: "hi"}},
	}, sink)

	if err != nil {
		t.Fatalf("Run() error = %v, want nil", err)
	}
	if res.FinalMessage.Content != "Hello" {
		t.Fatalf("FinalMessage.Content = %q, want %q", res.FinalMessage.Content, "Hello")
	}
	if res.Iterations != 1 {
		t.Fatalf("Iterations = %d, want 1", res.Iterations)
	}

	// Event stream: two text deltas, then run finished.
	if len(events) != 3 {
		t.Fatalf("len(events) = %d, want 3: %#v", len(events), events)
	}
	if events[0].Type != EventTextDelta || events[0].TextDelta == nil || events[0].TextDelta.Text != "Hel" {
		t.Fatalf("events[0] = %#v, want TextDelta(Hel)", events[0])
	}
	if events[1].Type != EventTextDelta || events[1].TextDelta == nil || events[1].TextDelta.Text != "lo" {
		t.Fatalf("events[1] = %#v, want TextDelta(lo)", events[1])
	}
	if events[2].Type != EventRunFinished || events[2].RunFinished == nil || events[2].RunFinished.FinalMessage.Content != "Hello" {
		t.Fatalf("events[2] = %#v, want RunFinished(Hello)", events[2])
	}

	// The LLM must have received system + user messages, in order.
	if len(llm.calls) != 1 {
		t.Fatalf("len(llm.calls) = %d, want 1", len(llm.calls))
	}
	msgs := llm.calls[0]
	if len(msgs) != 2 {
		t.Fatalf("len(messages to LLM) = %d, want 2: %#v", len(msgs), msgs)
	}
	if msgs[0].Role != RoleSystem || msgs[0].Content != "You are a test." {
		t.Fatalf("msgs[0] = %#v, want system prompt", msgs[0])
	}
	if msgs[1].Role != RoleUser || msgs[1].Content != "hi" {
		t.Fatalf("msgs[1] = %#v, want user message", msgs[1])
	}

	// RunResult.Messages carries the full history: system, user, assistant.
	if len(res.Messages) != 3 {
		t.Fatalf("len(res.Messages) = %d, want 3: %#v", len(res.Messages), res.Messages)
	}
	if res.Messages[2].Role != RoleAssistant || res.Messages[2].Content != "Hello" {
		t.Fatalf("res.Messages[2] = %#v, want assistant(Hello)", res.Messages[2])
	}
}

func TestRun_ToolCall_ExecutesAndContinues(t *testing.T) {
	llm := &fakeLLM{
		steps: []fakeStep{
			{resp: Response{ToolCalls: []ToolCall{{ID: "c1", Name: "read", Arguments: json.RawMessage(`{"path":"/x"}`)}}}},
			{deltas: []string{"done"}, resp: Response{Content: "done"}},
		},
	}
	reg := &fakeRegistry{
		specs: []ToolSpec{{Name: "read", Description: "read a file", Parameters: json.RawMessage(`{"type":"object"}`)}},
		results: map[string]fakeToolResult{
			"read": {result: "file contents"},
		},
	}
	a := Agent{LLM: llm}

	var events []Event
	sink := func(e Event) error {
		events = append(events, e)
		return nil
	}

	res, err := a.Run(context.Background(), RunInput{
		Model:    "m",
		Messages: []Message{{Role: RoleUser, Content: "read /x"}},
		Tools:    reg,
	}, sink)

	if err != nil {
		t.Fatalf("Run() error = %v, want nil", err)
	}
	if res.FinalMessage.Content != "done" {
		t.Fatalf("FinalMessage.Content = %q, want %q", res.FinalMessage.Content, "done")
	}
	if res.Iterations != 2 {
		t.Fatalf("Iterations = %d, want 2", res.Iterations)
	}

	// Event stream: ToolCall, ToolResult, TextDelta(done), RunFinished.
	if len(events) != 4 {
		t.Fatalf("len(events) = %d, want 4: %#v", len(events), events)
	}
	if events[0].Type != EventToolCall || events[0].ToolCall == nil || events[0].ToolCall.Name != "read" {
		t.Fatalf("events[0] = %#v, want ToolCall(read)", events[0])
	}
	if events[1].Type != EventToolResult || events[1].ToolResult == nil || events[1].ToolResult.Result != "file contents" || events[1].ToolResult.Error != "" {
		t.Fatalf("events[1] = %#v, want ToolResult(file contents)", events[1])
	}
	if events[2].Type != EventTextDelta || events[2].TextDelta == nil || events[2].TextDelta.Text != "done" {
		t.Fatalf("events[2] = %#v, want TextDelta(done)", events[2])
	}
	if events[3].Type != EventRunFinished {
		t.Fatalf("events[3] = %#v, want RunFinished", events[3])
	}

	// The registry was called once with the right name and raw args.
	if len(reg.calls) != 1 {
		t.Fatalf("len(reg.calls) = %d, want 1", len(reg.calls))
	}
	if reg.calls[0].name != "read" {
		t.Fatalf("reg.calls[0].name = %q, want %q", reg.calls[0].name, "read")
	}
	if string(reg.calls[0].args) != `{"path":"/x"}` {
		t.Fatalf("reg.calls[0].args = %s, want raw args", reg.calls[0].args)
	}

	// The second LLM call saw user, assistant(with tool_calls), then the tool result.
	if len(llm.calls) != 2 {
		t.Fatalf("len(llm.calls) = %d, want 2", len(llm.calls))
	}
	msgs := llm.calls[1]
	if len(msgs) != 3 {
		t.Fatalf("len(second LLM messages) = %d, want 3: %#v", len(msgs), msgs)
	}
	if msgs[1].Role != RoleAssistant || len(msgs[1].ToolCalls) != 1 {
		t.Fatalf("msgs[1] = %#v, want assistant with 1 tool call", msgs[1])
	}
	if msgs[2].Role != RoleTool || msgs[2].ToolCallID != "c1" || msgs[2].Name != "read" || msgs[2].Content != "file contents" {
		t.Fatalf("msgs[2] = %#v, want tool message with result", msgs[2])
	}
}

func TestRun_ToolError_IsFedBackToModel(t *testing.T) {
	llm := &fakeLLM{
		steps: []fakeStep{
			{resp: Response{ToolCalls: []ToolCall{{ID: "c1", Name: "boom", Arguments: json.RawMessage(`{}`)}}}},
			{resp: Response{Content: "recovered"}},
		},
	}
	reg := &fakeRegistry{
		results: map[string]fakeToolResult{
			"boom": {err: errors.New("kaboom")},
		},
	}
	a := Agent{LLM: llm}

	var events []Event
	sink := func(e Event) error {
		events = append(events, e)
		return nil
	}

	res, err := a.Run(context.Background(), RunInput{
		Model:    "m",
		Messages: []Message{{Role: RoleUser, Content: "go"}},
		Tools:    reg,
	}, sink)

	if err != nil {
		t.Fatalf("Run() error = %v, want nil (tool errors are not fatal)", err)
	}
	if res.FinalMessage.Content != "recovered" {
		t.Fatalf("FinalMessage.Content = %q, want %q", res.FinalMessage.Content, "recovered")
	}

	// The ToolResult event carried the error text and the run continued.
	if len(events) < 2 || events[1].Type != EventToolResult || events[1].ToolResult.Error != "kaboom" {
		t.Fatalf("events = %#v, want ToolResult with Error=kaboom at index 1", events)
	}

	// The model received the error text as the tool message content.
	msgs := llm.calls[1]
	if len(msgs) != 3 {
		t.Fatalf("len(second LLM messages) = %d, want 3: %#v", len(msgs), msgs)
	}
	if msgs[2].Role != RoleTool || msgs[2].Content != "kaboom" {
		t.Fatalf("msgs[2] = %#v, want tool message with error text", msgs[2])
	}
}

func TestRun_MaxIterationsExceeded(t *testing.T) {
	steps := make([]fakeStep, 5)
	for i := range steps {
		steps[i] = fakeStep{resp: Response{ToolCalls: []ToolCall{{ID: "c", Name: "nop", Arguments: json.RawMessage(`{}`)}}}}
	}
	llm := &fakeLLM{steps: steps}
	reg := &fakeRegistry{results: map[string]fakeToolResult{"nop": {result: "ok"}}}
	a := Agent{LLM: llm}

	_, err := a.Run(context.Background(), RunInput{
		Model:         "m",
		Messages:      []Message{{Role: RoleUser, Content: "go"}},
		Tools:         reg,
		MaxIterations: 3,
	}, func(Event) error { return nil })

	if !errors.Is(err, ErrMaxIterations) {
		t.Fatalf("Run() error = %v, want ErrMaxIterations", err)
	}
	if len(llm.calls) != 3 {
		t.Fatalf("len(llm.calls) = %d, want 3 (cap enforced)", len(llm.calls))
	}
	if len(reg.calls) != 3 {
		t.Fatalf("len(reg.calls) = %d, want 3", len(reg.calls))
	}
}

func TestRun_DefaultMaxIterations(t *testing.T) {
	steps := make([]fakeStep, DefaultMaxIterations+5)
	for i := range steps {
		steps[i] = fakeStep{resp: Response{ToolCalls: []ToolCall{{ID: "c", Name: "nop", Arguments: json.RawMessage(`{}`)}}}}
	}
	llm := &fakeLLM{steps: steps}
	reg := &fakeRegistry{results: map[string]fakeToolResult{"nop": {result: "ok"}}}
	a := Agent{LLM: llm}

	_, err := a.Run(context.Background(), RunInput{
		Model:    "m",
		Messages: []Message{{Role: RoleUser, Content: "go"}},
		Tools:    reg, // MaxIterations 0 → default
	}, func(Event) error { return nil })

	if !errors.Is(err, ErrMaxIterations) {
		t.Fatalf("Run() error = %v, want ErrMaxIterations", err)
	}
	if len(llm.calls) != DefaultMaxIterations {
		t.Fatalf("len(llm.calls) = %d, want %d", len(llm.calls), DefaultMaxIterations)
	}
}

func TestRun_ContextCancelled(t *testing.T) {
	a := Agent{LLM: &fakeLLM{}}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := a.Run(ctx, RunInput{Model: "m", Messages: []Message{{Role: RoleUser, Content: "go"}}}, func(Event) error { return nil })

	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Run() error = %v, want context.Canceled", err)
	}
}

func TestRun_SinkErrorAborts(t *testing.T) {
	llm := &fakeLLM{
		steps: []fakeStep{
			{deltas: []string{"a"}, resp: Response{Content: "a"}},
		},
	}
	a := Agent{LLM: llm}

	stop := errors.New("sink abort")
	_, err := a.Run(context.Background(), RunInput{Model: "m", Messages: []Message{{Role: RoleUser, Content: "go"}}}, func(Event) error {
		return stop
	})

	if !errors.Is(err, stop) {
		t.Fatalf("Run() error = %v, want sink abort error", err)
	}
}

func TestRun_LLMErrorAborts(t *testing.T) {
	llmDown := errors.New("llm down")
	a := Agent{LLM: &fakeLLM{steps: []fakeStep{{err: llmDown}}}}

	_, err := a.Run(context.Background(), RunInput{Model: "m", Messages: []Message{{Role: RoleUser, Content: "go"}}}, func(Event) error { return nil })

	if !errors.Is(err, llmDown) {
		t.Fatalf("Run() error = %v, want llm down error", err)
	}
}

func TestRun_ToolCallWithoutRegistry(t *testing.T) {
	llm := &fakeLLM{
		steps: []fakeStep{
			{resp: Response{ToolCalls: []ToolCall{{ID: "c1", Name: "read", Arguments: json.RawMessage(`{}`)}}}},
		},
	}
	a := Agent{LLM: llm}

	_, err := a.Run(context.Background(), RunInput{
		Model:    "m",
		Messages: []Message{{Role: RoleUser, Content: "go"}},
		Tools:    nil,
	}, func(Event) error { return nil })

	if err == nil {
		t.Fatal("Run() error = nil, want error for tool call without registry")
	}
}

func TestRun_MultipleToolCallsInOneResponse(t *testing.T) {
	llm := &fakeLLM{
		steps: []fakeStep{
			{resp: Response{ToolCalls: []ToolCall{
				{ID: "c1", Name: "a", Arguments: json.RawMessage(`{"n":1}`)},
				{ID: "c2", Name: "b", Arguments: json.RawMessage(`{"n":2}`)},
			}}},
			{resp: Response{Content: "all done"}},
		},
	}
	reg := &fakeRegistry{results: map[string]fakeToolResult{
		"a": {result: "A"},
		"b": {result: "B"},
	}}
	a := Agent{LLM: llm}

	var events []Event
	res, err := a.Run(context.Background(), RunInput{
		Model:    "m",
		Messages: []Message{{Role: RoleUser, Content: "go"}},
		Tools:    reg,
	}, func(e Event) error {
		events = append(events, e)
		return nil
	})

	if err != nil {
		t.Fatalf("Run() error = %v, want nil", err)
	}
	if res.FinalMessage.Content != "all done" {
		t.Fatalf("FinalMessage.Content = %q, want %q", res.FinalMessage.Content, "all done")
	}

	// Events: ToolCall(a), ToolResult(a), ToolCall(b), ToolResult(b), then
	// RunFinished (the second response scripted no text deltas).
	if len(events) != 5 {
		t.Fatalf("len(events) = %d, want 5: %#v", len(events), events)
	}
	if events[0].Type != EventToolCall || events[0].ToolCall.Name != "a" ||
		events[2].Type != EventToolCall || events[2].ToolCall.Name != "b" {
		t.Fatalf("events = %#v, want tool calls a then b", events)
	}
	if events[4].Type != EventRunFinished {
		t.Fatalf("events[4] = %#v, want RunFinished", events[4])
	}
	if len(reg.calls) != 2 || reg.calls[0].name != "a" || reg.calls[1].name != "b" {
		t.Fatalf("reg.calls = %#v, want [a b]", reg.calls)
	}

	// Both tool results were appended to the conversation.
	msgs := llm.calls[1]
	if len(msgs) != 4 {
		t.Fatalf("len(second LLM messages) = %d, want 4: %#v", len(msgs), msgs)
	}
	if msgs[2].Role != RoleTool || msgs[2].Name != "a" || msgs[3].Role != RoleTool || msgs[3].Name != "b" {
		t.Fatalf("msgs[2:4] = %#v, want tool messages a then b", msgs[2:4])
	}
}

func TestRun_NilSinkDoesNotPanic(t *testing.T) {
	a := Agent{LLM: &fakeLLM{steps: []fakeStep{{resp: Response{Content: "ok"}}}}}

	res, err := a.Run(context.Background(), RunInput{Model: "m", Messages: []Message{{Role: RoleUser, Content: "go"}}}, nil)

	if err != nil {
		t.Fatalf("Run() error = %v, want nil", err)
	}
	if res.FinalMessage.Content != "ok" {
		t.Fatalf("FinalMessage.Content = %q, want %q", res.FinalMessage.Content, "ok")
	}
}

func TestRun_NoSystemPrompt(t *testing.T) {
	llm := &fakeLLM{steps: []fakeStep{{resp: Response{Content: "ok"}}}}
	a := Agent{LLM: llm}

	_, err := a.Run(context.Background(), RunInput{
		Model:    "m",
		Messages: []Message{{Role: RoleUser, Content: "go"}}, // no SystemPrompt
	}, func(Event) error { return nil })

	if err != nil {
		t.Fatalf("Run() error = %v, want nil", err)
	}
	msgs := llm.calls[0]
	if len(msgs) != 1 || msgs[0].Role != RoleUser {
		t.Fatalf("messages = %#v, want a single user message (no system prompt)", msgs)
	}
}
