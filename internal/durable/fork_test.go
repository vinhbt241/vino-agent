package durable

import (
	"context"
	"path/filepath"
	"testing"

	"vino/internal/agent"
)

// seedParent builds a conversation with two answered turns and returns the
// harness, its LLM, the conversation handle, and its four transcript entries
// (q1, answer1, q2, answer2).
func seedParent(t *testing.T, ctx context.Context) (*Harness, *fakeLLM, *Conversation, []EntryRecord) {
	t.Helper()
	llm := &fakeLLM{steps: []llmStep{
		{resp: agent.Response{Content: "answer1"}},
		{resp: agent.Response{Content: "answer2"}},
	}}
	h, err := Open(ctx, NewMemoryStorage(), HarnessOptions{LLM: llm})
	if err != nil {
		t.Fatal(err)
	}
	sub, err := h.Start(ctx, StartInput{Messages: []agent.Message{{Role: agent.RoleUser, Content: "q1"}}}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := sub.Wait(ctx); err != nil {
		t.Fatal(err)
	}
	parent, err := h.Conversation(ctx, sub.ConversationID())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := parent.Submit(ctx, SubmitInput{Content: "q2"}); err != nil {
		t.Fatal(err)
	}
	if err := h.Run(ctx); err != nil {
		t.Fatal(err)
	}
	entries, err := parent.Entries(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 4 {
		t.Fatalf("parent entries = %d, want 4", len(entries))
	}
	return h, llm, parent, entries
}

func TestForkSeesParentHistoryUpToPoint(t *testing.T) {
	ctx := context.Background()
	_, llm, parent, entries := seedParent(t, ctx)

	// Fork at entries[1] (the first assistant answer).
	at := &entries[1]
	fork, err := parent.Fork(ctx, at, AgentConfig{})
	if err != nil {
		t.Fatal(err)
	}
	rec, err := fork.Record(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if rec.Parent == nil || rec.Parent.ConversationID != parent.ID() || rec.Parent.At != at.ID {
		t.Fatalf("fork Parent = %+v, want {conversationId:%d at:%d}", rec.Parent, parent.ID(), at.ID)
	}

	llm.steps = append(llm.steps, llmStep{resp: agent.Response{Content: "review done"}})
	sub, err := fork.Submit(ctx, SubmitInput{Content: "review this"})
	if err != nil {
		t.Fatal(err)
	}
	recSub, err := sub.Wait(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if recSub.Status != SubmissionDone {
		t.Fatalf("fork submission status = %s, want done", recSub.Status)
	}

	msgs := llm.messages[2]
	want := []agent.Message{
		{Role: agent.RoleUser, Content: "q1"},
		{Role: agent.RoleAssistant, Content: "answer1"},
		{Role: agent.RoleUser, Content: "review this"},
	}
	if len(msgs) != len(want) {
		t.Fatalf("fork context = %d messages, want %d: %#v", len(msgs), len(want), msgs)
	}
	for i, w := range want {
		if msgs[i].Role != w.Role || msgs[i].Content != w.Content {
			t.Fatalf("fork context[%d] = %+v, want %+v (full: %#v)", i, msgs[i], w, msgs)
		}
	}
}

func TestForkAtForeignEntryErrors(t *testing.T) {
	ctx := context.Background()
	llm := &fakeLLM{steps: []llmStep{{resp: agent.Response{Content: "a"}}, {resp: agent.Response{Content: "b"}}}}
	h, err := Open(ctx, NewMemoryStorage(), HarnessOptions{LLM: llm})
	if err != nil {
		t.Fatal(err)
	}
	s1, err := h.Start(ctx, StartInput{Messages: []agent.Message{{Role: agent.RoleUser, Content: "one"}}}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s1.Wait(ctx); err != nil {
		t.Fatal(err)
	}
	s2, err := h.Start(ctx, StartInput{Messages: []agent.Message{{Role: agent.RoleUser, Content: "two"}}}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s2.Wait(ctx); err != nil {
		t.Fatal(err)
	}

	c1, err := h.Conversation(ctx, s1.ConversationID())
	if err != nil {
		t.Fatal(err)
	}
	c2, err := h.Conversation(ctx, s2.ConversationID())
	if err != nil {
		t.Fatal(err)
	}
	foreign, err := c2.Entries(ctx)
	if err != nil {
		t.Fatal(err)
	}

	if _, err := c1.Fork(ctx, &foreign[0], AgentConfig{}); err == nil {
		t.Fatal("Fork with a foreign conversation's entry returned nil error")
	}
}

func TestForkOfForkWalksAncestorChain(t *testing.T) {
	ctx := context.Background()
	_, llm, parent, parentEntries := seedParent(t, ctx)

	// Fork the parent at the first answer, then give the child its own turn.
	child, err := parent.Fork(ctx, &parentEntries[1], AgentConfig{})
	if err != nil {
		t.Fatal(err)
	}
	llm.steps = append(llm.steps, llmStep{resp: agent.Response{Content: "child answer"}})
	childSub, err := child.Submit(ctx, SubmitInput{Content: "child q"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := childSub.Wait(ctx); err != nil {
		t.Fatal(err)
	}
	childEntries, err := child.Entries(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(childEntries) != 2 {
		t.Fatalf("child entries = %d, want 2", len(childEntries))
	}

	// Fork the child at its answer, then run the grandchild.
	llm.steps = append(llm.steps, llmStep{resp: agent.Response{Content: "grand done"}})
	grandchild, err := child.Fork(ctx, &childEntries[1], AgentConfig{})
	if err != nil {
		t.Fatal(err)
	}
	grandSub, err := grandchild.Submit(ctx, SubmitInput{Content: "grand q"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := grandSub.Wait(ctx); err != nil {
		t.Fatal(err)
	}

	msgs := llm.messages[3]
	want := []agent.Message{
		{Role: agent.RoleUser, Content: "q1"},
		{Role: agent.RoleAssistant, Content: "answer1"},
		{Role: agent.RoleUser, Content: "child q"},
		{Role: agent.RoleAssistant, Content: "child answer"},
		{Role: agent.RoleUser, Content: "grand q"},
	}
	if len(msgs) != len(want) {
		t.Fatalf("grandchild context = %d messages, want %d: %#v", len(msgs), len(want), msgs)
	}
	for i, w := range want {
		if msgs[i].Role != w.Role || msgs[i].Content != w.Content {
			t.Fatalf("grandchild context[%d] = %+v, want %+v (full: %#v)", i, msgs[i], w, msgs)
		}
	}
}

func TestConfigureSystemPrompt(t *testing.T) {
	ctx := context.Background()

	t.Run("configured prompt reaches the model", func(t *testing.T) {
		llm := &fakeLLM{steps: []llmStep{{resp: agent.Response{Content: "ok"}}}}
		h, err := Open(ctx, NewMemoryStorage(), HarnessOptions{LLM: llm, SystemPrompt: "harness-sys"})
		if err != nil {
			t.Fatal(err)
		}
		if err := h.Root().Configure(ctx, AgentConfig{SystemPrompt: "conv-sys"}); err != nil {
			t.Fatal(err)
		}
		if _, err := h.Root().Submit(ctx, SubmitInput{Content: "hi"}); err != nil {
			t.Fatal(err)
		}
		if err := h.Run(ctx); err != nil {
			t.Fatal(err)
		}
		if len(llm.messages) != 1 || len(llm.messages[0]) == 0 {
			t.Fatalf("messages = %#v", llm.messages)
		}
		m0 := llm.messages[0][0]
		if m0.Role != agent.RoleSystem || m0.Content != "conv-sys" {
			t.Fatalf("system prompt = %+v, want conv-sys", m0)
		}
	})

	t.Run("per-run override beats configured", func(t *testing.T) {
		llm := &fakeLLM{steps: []llmStep{{resp: agent.Response{Content: "ok"}}}}
		h, err := Open(ctx, NewMemoryStorage(), HarnessOptions{LLM: llm, SystemPrompt: "harness-sys"})
		if err != nil {
			t.Fatal(err)
		}
		if err := h.Root().Configure(ctx, AgentConfig{SystemPrompt: "conv-sys"}); err != nil {
			t.Fatal(err)
		}
		if _, err := h.Root().Submit(ctx, SubmitInput{Content: "hi", SystemPrompt: "run-sys"}); err != nil {
			t.Fatal(err)
		}
		if err := h.Run(ctx); err != nil {
			t.Fatal(err)
		}
		m0 := llm.messages[0][0]
		if m0.Role != agent.RoleSystem || m0.Content != "run-sys" {
			t.Fatalf("system prompt = %+v, want run-sys", m0)
		}
	})

	t.Run("empty config falls back to harness default", func(t *testing.T) {
		llm := &fakeLLM{steps: []llmStep{{resp: agent.Response{Content: "ok"}}}}
		h, err := Open(ctx, NewMemoryStorage(), HarnessOptions{LLM: llm, SystemPrompt: "harness-sys"})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := h.Root().Submit(ctx, SubmitInput{Content: "hi"}); err != nil {
			t.Fatal(err)
		}
		if err := h.Run(ctx); err != nil {
			t.Fatal(err)
		}
		m0 := llm.messages[0][0]
		if m0.Role != agent.RoleSystem || m0.Content != "harness-sys" {
			t.Fatalf("system prompt = %+v, want harness-sys", m0)
		}
	})
}

func TestConfigureModelSelection(t *testing.T) {
	ctx := context.Background()
	defaultLLM := &fakeLLM{steps: []llmStep{{resp: agent.Response{Content: "default"}}}}
	otherLLM := &fakeLLM{steps: []llmStep{{resp: agent.Response{Content: "other"}}}}
	h, err := Open(ctx, NewMemoryStorage(), HarnessOptions{
		LLM:    defaultLLM,
		Models: map[string]agent.LLM{"other": otherLLM},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := h.Root().Configure(ctx, AgentConfig{Model: "other"}); err != nil {
		t.Fatal(err)
	}
	if _, err := h.Root().Submit(ctx, SubmitInput{Content: "hi"}); err != nil {
		t.Fatal(err)
	}
	if err := h.Run(ctx); err != nil {
		t.Fatal(err)
	}
	if otherLLM.calls != 1 {
		t.Fatalf("otherLLM.calls = %d, want 1", otherLLM.calls)
	}
	if defaultLLM.calls != 0 {
		t.Fatalf("defaultLLM.calls = %d, want 0", defaultLLM.calls)
	}
}

func TestConfigureToolsAllowList(t *testing.T) {
	ctx := context.Background()
	llm := &fakeLLM{steps: []llmStep{{resp: agent.Response{Content: "ok"}}}}
	tools := &fakeRegistry{specs: []agent.ToolSpec{
		{Name: "read"},
		{Name: "write"},
	}}
	h, err := Open(ctx, NewMemoryStorage(), HarnessOptions{LLM: llm, Tools: tools})
	if err != nil {
		t.Fatal(err)
	}
	if err := h.Root().Configure(ctx, AgentConfig{Tools: []string{"read"}}); err != nil {
		t.Fatal(err)
	}
	if _, err := h.Root().Submit(ctx, SubmitInput{Content: "hi"}); err != nil {
		t.Fatal(err)
	}
	if err := h.Run(ctx); err != nil {
		t.Fatal(err)
	}
	if len(llm.specs) != 1 || len(llm.specs[0]) != 1 || llm.specs[0][0].Name != "read" {
		t.Fatalf("specs exposed to model = %#v, want [read]", llm.specs)
	}
}

func TestConfigurePersistsAcrossJSONLReopen(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "agent.jsonl")
	st, err := OpenJSONL(path, false)
	if err != nil {
		t.Fatal(err)
	}
	h, err := Open(ctx, st, HarnessOptions{LLM: &fakeLLM{}})
	if err != nil {
		t.Fatal(err)
	}
	cfg := AgentConfig{Model: "m1", SystemPrompt: "persist-sys", Tools: []string{"read"}}
	if err := h.Root().Configure(ctx, cfg); err != nil {
		t.Fatal(err)
	}
	if err := h.Close(ctx); err != nil {
		t.Fatal(err)
	}

	st2, err := OpenJSONL(path, false)
	if err != nil {
		t.Fatal(err)
	}
	rec, err := st2.Conversation(ctx, RootConversationID)
	if err != nil {
		t.Fatal(err)
	}
	if rec == nil || rec.Agent.Model != "m1" || rec.Agent.SystemPrompt != "persist-sys" || len(rec.Agent.Tools) != 1 || rec.Agent.Tools[0] != "read" {
		t.Fatalf("reopened Agent = %+v, want %+v", rec.Agent, cfg)
	}
}
