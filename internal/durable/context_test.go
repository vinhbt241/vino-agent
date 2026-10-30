package durable

import (
	"context"
	"encoding/json"
	"reflect"
	"testing"

	"vino/internal/agent"
)

// TestContextMessagesAssemblesFullContext exercises the single context seam:
// system prompt, fork ancestors, compaction summary, live tail, and schema
// assembly all through one call.
func TestContextMessagesAssemblesFullContext(t *testing.T) {
	ctx := context.Background()
	st := NewMemoryStorage()

	const (
		parentID = ID(2)
		childID  = ID(3)
	)

	// Parent: one user turn. Child: forked at that turn, with a compaction
	// summary covering up to entry 30 and a live tail whose assistant
	// tool_calls turn has no tool result yet (dangling).
	write(t, st, ctx, []StorageWrite{
		{Type: WriteConversation, Value: &ConversationRecord{ID: RootConversationID}},
		{Type: WriteConversation, Value: &ConversationRecord{ID: parentID}},
		{Type: WriteConversation, Value: &ConversationRecord{ID: childID, Parent: &ForkRef{ConversationID: parentID, At: ID(10)}}},
		{Type: WriteEntry, Value: &EntryRecord{ID: 10, ConversationID: parentID, Kind: kindUser,
			Model: []agent.Message{{Role: agent.RoleUser, Content: "q1"}}}},
		{Type: WriteEntry, Value: &EntryRecord{ID: 20, ConversationID: childID, Kind: kindCompaction,
			Head:  ptrID(30),
			Model: []agent.Message{{Role: agent.RoleSystem, Content: "summary"}}}},
		{Type: WriteEntry, Value: &EntryRecord{ID: 30, ConversationID: childID, Kind: kindUser,
			Model: []agent.Message{{Role: agent.RoleUser, Content: "q2"}}}},
		{Type: WriteEntry, Value: &EntryRecord{ID: 31, ConversationID: childID, Kind: kindAssistant,
			Model: []agent.Message{{Role: agent.RoleAssistant, ToolCalls: []agent.ToolCall{{ID: "c1", Name: "search", Arguments: json.RawMessage(`{}`)}}}}}},
	})

	h, err := Open(ctx, st, HarnessOptions{LLM: &fakeLLM{}, SystemPrompt: "default"})
	if err != nil {
		t.Fatal(err)
	}

	got, err := h.contextMessages(ctx, childID, "")
	if err != nil {
		t.Fatal(err)
	}
	want := []agent.Message{
		{Role: agent.RoleSystem, Content: "default"},
		{Role: agent.RoleUser, Content: "q1"},
		{Role: agent.RoleSystem, Content: "summary"},
		{Role: agent.RoleAssistant, ToolCalls: []agent.ToolCall{{ID: "c1", Name: "search", Arguments: json.RawMessage(`{}`)}}},
		{Role: agent.RoleTool, ToolCallID: "c1", Name: "search", Content: cancelledToolResult},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("contextMessages() = %#v, want %#v", got, want)
	}
	if !toolTurnSchemaHolds(got) {
		t.Fatalf("contextMessages() output violates the tool-call schema: %#v", got)
	}

	// A caller-supplied prompt overrides the harness default.
	got, err = h.contextMessages(ctx, childID, "override")
	if err != nil {
		t.Fatal(err)
	}
	if len(got) == 0 || got[0].Role != agent.RoleSystem || got[0].Content != "override" {
		t.Fatalf("contextMessages(override) first message = %#v, want system \"override\"", got[0])
	}
}

func ptrID(id ID) *ID { return &id }
