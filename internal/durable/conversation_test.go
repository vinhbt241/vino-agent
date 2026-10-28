package durable

import (
	"context"
	"errors"
	"testing"

	"vino/internal/agent"
)

func TestHarnessConversationLookup(t *testing.T) {
	ctx := context.Background()
	llm := &fakeLLM{steps: []llmStep{{resp: agent.Response{Content: "ok"}}}}
	h, err := Open(ctx, NewMemoryStorage(), HarnessOptions{LLM: llm})
	if err != nil {
		t.Fatal(err)
	}
	sub, err := h.Start(ctx, StartInput{Messages: []agent.Message{{Role: agent.RoleUser, Content: "hi"}}}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := sub.Wait(ctx); err != nil {
		t.Fatal(err)
	}

	id := sub.ConversationID()
	if id == 0 {
		t.Fatal("ConversationID() = 0, want a real conversation id")
	}

	c, err := h.Conversation(ctx, id)
	if err != nil {
		t.Fatalf("Conversation(%d): %v", id, err)
	}
	if c == nil || c.ID() != id {
		t.Fatalf("Conversation(%d) = %+v, want handle with id %d", id, c, id)
	}

	if _, err := h.Conversation(ctx, ID(999999)); !errors.Is(err, ErrConversationNotFound) {
		t.Fatalf("Conversation(unknown) error = %v, want ErrConversationNotFound", err)
	}
}

func TestSubmissionResumed(t *testing.T) {
	ctx := context.Background()
	llm := &fakeLLM{steps: []llmStep{{resp: agent.Response{Content: "a"}}, {resp: agent.Response{Content: "b"}}}}
	h, err := Open(ctx, NewMemoryStorage(), HarnessOptions{LLM: llm})
	if err != nil {
		t.Fatal(err)
	}

	in := StartInput{RequestID: "job", Messages: []agent.Message{{Role: agent.RoleUser, Content: "one"}}}
	s1, err := h.Start(ctx, in, nil)
	if err != nil {
		t.Fatal(err)
	}
	if s1.Resumed() {
		t.Fatal("first Start Resumed() = true, want false")
	}
	s2, err := h.Start(ctx, in, nil)
	if err != nil {
		t.Fatal(err)
	}
	if !s2.Resumed() {
		t.Fatal("requestId-matched Start Resumed() = false, want true")
	}

	// Same signal from the conversation resume path.
	s3, err := h.Root().Submit(ctx, SubmitInput{Content: "x", RequestID: "job"})
	if err != nil {
		t.Fatal(err)
	}
	if !s3.Resumed() {
		t.Fatal("requestId-matched Submit Resumed() = false, want true")
	}
	s4, err := h.Root().Submit(ctx, SubmitInput{Content: "y", RequestID: "fresh"})
	if err != nil {
		t.Fatal(err)
	}
	if s4.Resumed() {
		t.Fatal("new Submit Resumed() = true, want false")
	}
}

func TestConversationLatestMessage(t *testing.T) {
	ctx := context.Background()

	t.Run("last non-empty user or assistant", func(t *testing.T) {
		st := NewMemoryStorage()
		convID := ID(2)
		write(t, st, ctx, []StorageWrite{
			{Type: WriteConversation, Value: &ConversationRecord{ID: RootConversationID}},
			{Type: WriteConversation, Value: &ConversationRecord{ID: convID}},
			{Type: WriteEntry, Value: &EntryRecord{ID: 10, ConversationID: convID, Kind: kindUser, Model: []agent.Message{{Role: agent.RoleUser, Content: "first"}}}},
			{Type: WriteEntry, Value: &EntryRecord{ID: 11, ConversationID: convID, Kind: kindAssistant, Model: []agent.Message{{Role: agent.RoleAssistant, Content: "answer"}}}},
			{Type: WriteEntry, Value: &EntryRecord{ID: 12, ConversationID: convID, Kind: kindUser, Model: []agent.Message{{Role: agent.RoleUser, Content: "second"}}}},
			{Type: WriteEntry, Value: &EntryRecord{ID: 13, ConversationID: convID, Kind: kindAssistant, Model: []agent.Message{{Role: agent.RoleAssistant, ToolCalls: []agent.ToolCall{{ID: "c", Name: "read"}}}}}},
			{Type: WriteEntry, Value: &EntryRecord{ID: 14, ConversationID: convID, Kind: kindTool, Model: []agent.Message{{Role: agent.RoleTool, ToolCallID: "c", Content: "result"}}}},
			{Type: WriteEntry, Value: &EntryRecord{ID: 15, ConversationID: convID, Kind: kindAssistant, Model: []agent.Message{{Role: agent.RoleAssistant, Content: "final"}}}},
		})
		h, err := Open(ctx, st, HarnessOptions{LLM: &fakeLLM{}})
		if err != nil {
			t.Fatal(err)
		}
		c, err := h.Conversation(ctx, convID)
		if err != nil {
			t.Fatal(err)
		}
		e, err := c.LatestMessage(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if e == nil || e.ID != 15 {
			t.Fatalf("LatestMessage = %+v, want entry 15 (skips tool, prefers non-empty assistant)", e)
		}
	})

	t.Run("falls back over empty content", func(t *testing.T) {
		st := NewMemoryStorage()
		convID := ID(2)
		write(t, st, ctx, []StorageWrite{
			{Type: WriteConversation, Value: &ConversationRecord{ID: RootConversationID}},
			{Type: WriteConversation, Value: &ConversationRecord{ID: convID}},
			{Type: WriteEntry, Value: &EntryRecord{ID: 10, ConversationID: convID, Kind: kindUser, Model: []agent.Message{{Role: agent.RoleUser, Content: "q"}}}},
			{Type: WriteEntry, Value: &EntryRecord{ID: 11, ConversationID: convID, Kind: kindAssistant, Model: []agent.Message{{Role: agent.RoleAssistant, ToolCalls: []agent.ToolCall{{ID: "c", Name: "read"}}}}}},
		})
		h, err := Open(ctx, st, HarnessOptions{LLM: &fakeLLM{}})
		if err != nil {
			t.Fatal(err)
		}
		c, err := h.Conversation(ctx, convID)
		if err != nil {
			t.Fatal(err)
		}
		e, err := c.LatestMessage(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if e == nil || e.ID != 10 {
			t.Fatalf("LatestMessage = %+v, want entry 10 (empty assistant falls back to prior user)", e)
		}
	})

	t.Run("no user or assistant entries", func(t *testing.T) {
		st := NewMemoryStorage()
		convID := ID(2)
		write(t, st, ctx, []StorageWrite{
			{Type: WriteConversation, Value: &ConversationRecord{ID: RootConversationID}},
			{Type: WriteConversation, Value: &ConversationRecord{ID: convID}},
			{Type: WriteEntry, Value: &EntryRecord{ID: 10, ConversationID: convID, Kind: kindTool, Model: []agent.Message{{Role: agent.RoleTool, ToolCallID: "c", Content: "result"}}}},
		})
		h, err := Open(ctx, st, HarnessOptions{LLM: &fakeLLM{}})
		if err != nil {
			t.Fatal(err)
		}
		c, err := h.Conversation(ctx, convID)
		if err != nil {
			t.Fatal(err)
		}
		e, err := c.LatestMessage(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if e != nil {
			t.Fatalf("LatestMessage = %+v, want nil", e)
		}
	})
}

func TestConversationSubmitResumesWithHistory(t *testing.T) {
	ctx := context.Background()
	llm := &fakeLLM{steps: []llmStep{
		{resp: agent.Response{Content: "first"}},
		{resp: agent.Response{Content: "second"}},
		{resp: agent.Response{Content: "third"}},
	}}
	h, err := Open(ctx, NewMemoryStorage(), HarnessOptions{LLM: llm, SystemPrompt: "harness-sys"})
	if err != nil {
		t.Fatal(err)
	}
	root := h.Root()

	s1, err := root.Submit(ctx, SubmitInput{Content: "one"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s1.Wait(ctx); err != nil {
		t.Fatal(err)
	}
	s2, err := root.Submit(ctx, SubmitInput{Content: "two"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s2.Wait(ctx); err != nil {
		t.Fatal(err)
	}
	s3, err := root.Submit(ctx, SubmitInput{Content: "three", SystemPrompt: "override-sys"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s3.Wait(ctx); err != nil {
		t.Fatal(err)
	}

	if llm.calls != 3 {
		t.Fatalf("llm.calls = %d, want 3", llm.calls)
	}

	// Second model call must see the resumed history in ID order.
	msgs := llm.messages[1]
	if len(msgs) != 4 {
		t.Fatalf("second call messages = %d, want 4: %#v", len(msgs), msgs)
	}
	if msgs[0].Role != agent.RoleSystem || msgs[0].Content != "harness-sys" {
		t.Fatalf("msgs[0] = %#v, want system harness-sys", msgs[0])
	}
	if msgs[1].Role != agent.RoleUser || msgs[1].Content != "one" {
		t.Fatalf("msgs[1] = %#v, want user one", msgs[1])
	}
	if msgs[2].Role != agent.RoleAssistant || msgs[2].Content != "first" {
		t.Fatalf("msgs[2] = %#v, want assistant first", msgs[2])
	}
	if msgs[3].Role != agent.RoleUser || msgs[3].Content != "two" {
		t.Fatalf("msgs[3] = %#v, want user two", msgs[3])
	}

	// SubmitInput.SystemPrompt overrides the harness default.
	msgs = llm.messages[2]
	if msgs[0].Role != agent.RoleSystem || msgs[0].Content != "override-sys" {
		t.Fatalf("msgs[0] = %#v, want system override-sys", msgs[0])
	}
}
