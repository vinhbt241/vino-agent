package durable

import (
	"context"
	"encoding/json"
	"errors"
	"path/filepath"
	"testing"

	"vino/internal/agent"
)

// memoTaskDef is a test task that exercises rt.Memo/rt.MemoValue. It records
// each Memo call's "wrote" result and any MemoValue read.
type memoTaskDef struct {
	steps []memoStep
	wrote []bool
	got   json.RawMessage
}

type memoStep struct {
	action string // "memo", "memoFail", "memoValue"
	key    string
	value  json.RawMessage
}

func (d *memoTaskDef) Kind() string { return "test.memo" }
func (d *memoTaskDef) Version() int { return 1 }
func (d *memoTaskDef) Initial(json.RawMessage) json.RawMessage {
	return mustJSON(0)
}
func (d *memoTaskDef) Step(_ context.Context, checkpoint json.RawMessage, rt *TaskRuntime) (NextState, error) {
	var i int
	if len(checkpoint) > 0 {
		_ = json.Unmarshal(checkpoint, &i)
	}
	if i >= len(d.steps) {
		return NextState{Status: TaskTerminal, Outcome: &Outcome{Status: OutcomeCompleted}}, nil
	}
	s := d.steps[i]
	i++
	next := mustJSON(i)

	switch s.action {
	case "memo":
		wrote, err := rt.Memo(s.key, s.value)
		if err != nil {
			return NextState{}, err
		}
		d.wrote = append(d.wrote, wrote)
	case "memoFail":
		if _, err := rt.Memo(s.key, s.value); err != nil {
			return NextState{}, err
		}
		return NextState{}, errors.New("boom after memo")
	case "memoValue":
		d.got = rt.MemoValue(s.key)
	}
	return NextState{Status: TaskRunning, Checkpoint: next}, nil
}

// seedMemoTask writes a conversation and a pending "test.memo" task, returning
// both IDs.
func seedMemoTask(t *testing.T, st Storage, ctx context.Context) (ID, ID) {
	t.Helper()
	convID := mint(t, st, ctx)
	taskID := mint(t, st, ctx)
	write(t, st, ctx, []StorageWrite{
		{Type: WriteConversation, Value: &ConversationRecord{ID: RootConversationID}},
		{Type: WriteConversation, Value: &ConversationRecord{ID: convID}},
		{Type: WriteTask, Value: &TaskRecord{ID: taskID, ConversationID: convID, Kind: "test.memo", Version: 1,
			Input: json.RawMessage(`{}`), State: TaskState{Status: TaskPending, Checkpoint: json.RawMessage(`0`)}}},
	})
	return convID, taskID
}

func TestMemoFirstWriterWins(t *testing.T) {
	ctx := context.Background()
	def := &memoTaskDef{steps: []memoStep{
		{action: "memo", key: "k", value: json.RawMessage(`"v1"`)},
		{action: "memo", key: "k", value: json.RawMessage(`"v2"`)},
		{action: "memoValue", key: "k"},
	}}
	h, err := Open(ctx, NewMemoryStorage(), HarnessOptions{LLM: &fakeLLM{}, Definitions: []TaskDefinition{def}})
	if err != nil {
		t.Fatal(err)
	}
	_, taskID := seedMemoTask(t, h.session.storage, ctx)
	if err := h.Run(ctx); err != nil {
		t.Fatal(err)
	}

	if len(def.wrote) != 2 || !def.wrote[0] || def.wrote[1] {
		t.Fatalf("wrote = %v, want [true false]", def.wrote)
	}
	if string(def.got) != `"v1"` {
		t.Fatalf("MemoValue = %s, want committed \"v1\"", def.got)
	}
	task, err := h.session.storage.Task(ctx, taskID)
	if err != nil {
		t.Fatal(err)
	}
	if string(task.Memos["k"]) != `"v1"` {
		t.Fatalf("committed Memos[k] = %s, want \"v1\"", task.Memos["k"])
	}
}

func TestMemoAtomicWithFailedStep(t *testing.T) {
	ctx := context.Background()
	def := &memoTaskDef{steps: []memoStep{{action: "memoFail", key: "k", value: json.RawMessage(`"v"`)}}}
	h, err := Open(ctx, NewMemoryStorage(), HarnessOptions{LLM: &fakeLLM{}, Definitions: []TaskDefinition{def}})
	if err != nil {
		t.Fatal(err)
	}
	_, taskID := seedMemoTask(t, h.session.storage, ctx)
	if err := h.Run(ctx); err != nil {
		t.Fatal(err)
	}
	task, err := h.session.storage.Task(ctx, taskID)
	if err != nil {
		t.Fatal(err)
	}
	if len(task.Memos) != 0 {
		t.Fatalf("Memos = %v, want empty (staged memo discarded with the failed step)", task.Memos)
	}
}

func TestMemoSurvivesJSONLReopen(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "agent.jsonl")
	def := &memoTaskDef{steps: []memoStep{{action: "memo", key: "k", value: json.RawMessage(`"v"`)}}}

	open := func() (*Harness, error) {
		st, err := OpenJSONL(path, false)
		if err != nil {
			return nil, err
		}
		return Open(ctx, st, HarnessOptions{LLM: &fakeLLM{}, Definitions: []TaskDefinition{def}})
	}

	h, err := open()
	if err != nil {
		t.Fatal(err)
	}
	_, taskID := seedMemoTask(t, h.session.storage, ctx)
	if err := h.Run(ctx); err != nil {
		t.Fatal(err)
	}
	if err := h.Close(ctx); err != nil {
		t.Fatal(err)
	}

	h, err = open()
	if err != nil {
		t.Fatal(err)
	}
	task, err := h.session.storage.Task(ctx, taskID)
	if err != nil {
		t.Fatal(err)
	}
	if string(task.Memos["k"]) != `"v"` {
		t.Fatalf("Memos[k] after reopen = %s, want \"v\"", task.Memos["k"])
	}
}

// extTaskDef is a minimal extension task for registration tests.
type extTaskDef struct{ ran int }

func (d *extTaskDef) Kind() string { return "test.ext" }
func (d *extTaskDef) Version() int { return 1 }
func (d *extTaskDef) Initial(json.RawMessage) json.RawMessage {
	return mustJSON(0)
}
func (d *extTaskDef) Step(_ context.Context, _ json.RawMessage, _ *TaskRuntime) (NextState, error) {
	d.ran++
	return NextState{Status: TaskTerminal, Outcome: &Outcome{Status: OutcomeCompleted}}, nil
}

func TestExtensionToolsExposed(t *testing.T) {
	ctx := context.Background()
	base := &fakeRegistry{specs: []agent.ToolSpec{{Name: "base.read"}}}
	ext := &fakeRegistry{specs: []agent.ToolSpec{{Name: "ext.search"}}}
	llm := &fakeLLM{steps: []llmStep{{resp: agent.Response{Content: "ok"}}}}
	h, err := Open(ctx, NewMemoryStorage(), HarnessOptions{LLM: llm, Tools: base, Extensions: []Extension{{Name: "x", Tools: ext}}})
	if err != nil {
		t.Fatal(err)
	}
	root := h.Root()
	if err := root.Configure(ctx, AgentConfig{Extensions: []string{"x"}}); err != nil {
		t.Fatal(err)
	}
	sub, err := root.Submit(ctx, SubmitInput{Content: "hi"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := sub.Wait(ctx); err != nil {
		t.Fatal(err)
	}
	if len(llm.specs) != 1 || len(llm.specs[0]) != 2 {
		t.Fatalf("specs = %#v, want one call with base + extension tools", llm.specs)
	}
	names := map[string]bool{}
	for _, s := range llm.specs[0] {
		names[s.Name] = true
	}
	if !names["base.read"] || !names["ext.search"] {
		t.Fatalf("spec names = %v, want base.read and ext.search", names)
	}
}

func TestExtensionToolExecutes(t *testing.T) {
	ctx := context.Background()
	base := &fakeRegistry{specs: []agent.ToolSpec{{Name: "base.read"}}}
	ext := &fakeRegistry{specs: []agent.ToolSpec{{Name: "ext.search"}}, results: map[string]string{"ext.search": "found"}}
	llm := &fakeLLM{steps: []llmStep{
		{resp: agent.Response{ToolCalls: []agent.ToolCall{{ID: "c1", Name: "ext.search", Arguments: json.RawMessage(`{}`)}}}},
		{resp: agent.Response{Content: "done"}},
	}}
	h, err := Open(ctx, NewMemoryStorage(), HarnessOptions{LLM: llm, Tools: base, Extensions: []Extension{{Name: "x", Tools: ext}}})
	if err != nil {
		t.Fatal(err)
	}
	root := h.Root()
	if err := root.Configure(ctx, AgentConfig{Extensions: []string{"x"}}); err != nil {
		t.Fatal(err)
	}
	sub, err := root.Submit(ctx, SubmitInput{Content: "hi"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := sub.Wait(ctx); err != nil {
		t.Fatal(err)
	}
	if len(ext.calls) != 1 || ext.calls[0] != "ext.search" {
		t.Fatalf("extension calls = %v, want [ext.search]", ext.calls)
	}
	if len(base.calls) != 0 {
		t.Fatalf("base calls = %v, want none (routed to the extension)", base.calls)
	}
}

func TestExtensionSystemPromptSections(t *testing.T) {
	ctx := context.Background()
	llm := &fakeLLM{steps: []llmStep{{resp: agent.Response{Content: "ok"}}}}
	ext := Extension{Name: "x", SystemPrompt: []string{"section A", "section B"}}
	h, err := Open(ctx, NewMemoryStorage(), HarnessOptions{LLM: llm, SystemPrompt: "base", Extensions: []Extension{ext}})
	if err != nil {
		t.Fatal(err)
	}
	root := h.Root()
	if err := root.Configure(ctx, AgentConfig{Extensions: []string{"x"}}); err != nil {
		t.Fatal(err)
	}
	sub, err := root.Submit(ctx, SubmitInput{Content: "hi"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := sub.Wait(ctx); err != nil {
		t.Fatal(err)
	}
	if len(llm.messages) != 1 || len(llm.messages[0]) == 0 {
		t.Fatalf("messages = %#v, want one call with a system message", llm.messages)
	}
	sys := llm.messages[0][0]
	if sys.Role != agent.RoleSystem || sys.Content != "base\n\nsection A\n\nsection B" {
		t.Fatalf("system message = %#v, want base + sections in order", sys)
	}
}

func TestExtensionTaskRuns(t *testing.T) {
	ctx := context.Background()
	def := &extTaskDef{}
	h, err := Open(ctx, NewMemoryStorage(), HarnessOptions{LLM: &fakeLLM{}, Extensions: []Extension{{Name: "x", Tasks: []TaskDefinition{def}}}})
	if err != nil {
		t.Fatal(err)
	}
	convID := mint(t, h.session.storage, ctx)
	write(t, h.session.storage, ctx, []StorageWrite{
		{Type: WriteConversation, Value: &ConversationRecord{ID: RootConversationID}},
		{Type: WriteConversation, Value: &ConversationRecord{ID: convID}},
		{Type: WriteTask, Value: &TaskRecord{ID: mint(t, h.session.storage, ctx), ConversationID: convID, Kind: "test.ext", Version: 1,
			Input: json.RawMessage(`{}`), State: TaskState{Status: TaskPending, Checkpoint: json.RawMessage(`0`)}}},
	})
	if err := h.Run(ctx); err != nil {
		t.Fatal(err)
	}
	if def.ran != 1 {
		t.Fatalf("extension task ran %d times, want 1", def.ran)
	}
}

func TestAllowListGatesBaseOnly(t *testing.T) {
	ctx := context.Background()
	base := &fakeRegistry{specs: []agent.ToolSpec{{Name: "read"}, {Name: "write"}}}
	ext := &fakeRegistry{specs: []agent.ToolSpec{{Name: "ext.search"}}}
	llm := &fakeLLM{steps: []llmStep{{resp: agent.Response{Content: "ok"}}}}
	h, err := Open(ctx, NewMemoryStorage(), HarnessOptions{LLM: llm, Tools: base, Extensions: []Extension{{Name: "x", Tools: ext}}})
	if err != nil {
		t.Fatal(err)
	}
	root := h.Root()
	if err := root.Configure(ctx, AgentConfig{Tools: []string{"read"}, Extensions: []string{"x"}}); err != nil {
		t.Fatal(err)
	}
	sub, err := root.Submit(ctx, SubmitInput{Content: "hi"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := sub.Wait(ctx); err != nil {
		t.Fatal(err)
	}
	names := map[string]bool{}
	for _, s := range llm.specs[0] {
		names[s.Name] = true
	}
	if !names["read"] || names["write"] || !names["ext.search"] {
		t.Fatalf("spec names = %v, want read + ext.search (write gated, extension not gated)", names)
	}
}

func TestExtensionShadowsBaseTool(t *testing.T) {
	ctx := context.Background()
	base := &fakeRegistry{specs: []agent.ToolSpec{{Name: "read", Description: "base desc"}}, results: map[string]string{"read": "base result"}}
	ext := &fakeRegistry{specs: []agent.ToolSpec{{Name: "read", Description: "ext desc"}}, results: map[string]string{"read": "ext result"}}
	llm := &fakeLLM{steps: []llmStep{
		{resp: agent.Response{ToolCalls: []agent.ToolCall{{ID: "c1", Name: "read", Arguments: json.RawMessage(`{}`)}}}},
		{resp: agent.Response{Content: "done"}},
	}}
	h, err := Open(ctx, NewMemoryStorage(), HarnessOptions{LLM: llm, Tools: base, Extensions: []Extension{{Name: "x", Tools: ext}}})
	if err != nil {
		t.Fatal(err)
	}
	root := h.Root()
	if err := root.Configure(ctx, AgentConfig{Extensions: []string{"x"}}); err != nil {
		t.Fatal(err)
	}
	sub, err := root.Submit(ctx, SubmitInput{Content: "hi"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := sub.Wait(ctx); err != nil {
		t.Fatal(err)
	}
	// The model sees the extension's spec for the shadowed name.
	for _, s := range llm.specs[0] {
		if s.Name == "read" && s.Description != "ext desc" {
			t.Fatalf("read spec = %+v, want extension's spec", s)
		}
	}
	if len(ext.calls) != 1 || len(base.calls) != 0 {
		t.Fatalf("ext calls = %v base calls = %v, want routed to extension only", ext.calls, base.calls)
	}
}

func TestUnknownExtensionSkipped(t *testing.T) {
	ctx := context.Background()
	llm := &fakeLLM{steps: []llmStep{{resp: agent.Response{Content: "ok"}}}}
	h, err := Open(ctx, NewMemoryStorage(), HarnessOptions{LLM: llm, SystemPrompt: "base"})
	if err != nil {
		t.Fatal(err)
	}
	root := h.Root()
	if err := root.Configure(ctx, AgentConfig{Extensions: []string{"nope"}}); err != nil {
		t.Fatal(err)
	}
	sub, err := root.Submit(ctx, SubmitInput{Content: "hi"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := sub.Wait(ctx); err != nil {
		t.Fatal(err)
	}
	if len(llm.messages) != 1 || llm.messages[0][0].Content != "base" {
		t.Fatalf("system message = %#v, want plain base (unknown extension contributes nothing)", llm.messages[0][0])
	}
}

func TestDuplicateExtensionNameErrors(t *testing.T) {
	ctx := context.Background()
	_, err := Open(ctx, NewMemoryStorage(), HarnessOptions{LLM: &fakeLLM{}, Extensions: []Extension{{Name: "x"}, {Name: "x"}}})
	if err == nil {
		t.Fatal("expected duplicate extension name to fail Open")
	}
}

func TestDuplicateTaskKindErrors(t *testing.T) {
	ctx := context.Background()
	_, err := Open(ctx, NewMemoryStorage(), HarnessOptions{LLM: &fakeLLM{}, Extensions: []Extension{
		{Name: "x", Tasks: []TaskDefinition{&extTaskDef{}}},
		{Name: "y", Tasks: []TaskDefinition{&extTaskDef{}}},
	}})
	if err == nil {
		t.Fatal("expected duplicate task kind to fail Open")
	}
}
