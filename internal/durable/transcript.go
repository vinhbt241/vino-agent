package durable

import (
	"encoding/json"

	"vino/internal/agent"
)

// Placeholder tool-result contents. These are the only strings the durable
// loop uses to keep the provider's tool-call schema satisfied when a tool
// call does not run to a real result. They live here — the transcript write
// seam — so the schema's vocabulary has one home.
const (
	// abortedToolResult is recorded when the client cancels a run while a
	// tool was executing.
	abortedToolResult = "tool call was cancelled by the client"
	// cancelledToolResult fills a missing tool result when an assistant
	// tool_calls turn has no matching result (cancelled or crashed after the
	// assistant turn committed but before the tools ran).
	cancelledToolResult = "tool call was cancelled before it completed"
	// crashInterruptedToolResult is recorded when a non-replay-safe tool was
	// interrupted by a crash and must not be re-run.
	crashInterruptedToolResult = "tool call was interrupted by a crash and is not safe to re-run"
)

// appendAssistantEntry stages an assistant transcript entry carrying msg. It
// is the single place assistant entries are written, so the assistant half of
// the tool-call schema is owned here.
func appendAssistantEntry(rt *TaskRuntime, entryID ID, msg agent.Message, data json.RawMessage) {
	rt.AppendEntry(&EntryRecord{
		ID:             entryID,
		ConversationID: rt.ConversationID(),
		Kind:           kindAssistant,
		Model:          []agent.Message{msg},
		Data:           data,
	})
}

// appendToolResultEntry stages a tool-result transcript entry for one tool
// call. It is the single place tool entries are written: every tool result —
// real, failed, aborted, crash-interrupted, or restart-limited — keeps the
// assistant tool_calls schema satisfied through this one seam.
func appendToolResultEntry(rt *TaskRuntime, entryID ID, callID, name, content string) {
	rt.AppendEntry(&EntryRecord{
		ID:             entryID,
		ConversationID: rt.ConversationID(),
		Kind:           kindTool,
		Model: []agent.Message{{
			Role:       agent.RoleTool,
			ToolCallID: callID,
			Name:       name,
			Content:    content,
		}},
	})
}

// assembleToolTurns returns msgs with the tool-call schema enforced: every
// assistant tool_calls message is immediately followed by exactly one tool
// message per call, in call order. Tool results are reattached to their
// assistant turn by ToolCallID, so they stay in place even when a later write
// (a concurrent submit) landed between the assistant entry and its results in
// ID order. A call with no matching result is filled with cancelledToolResult;
// a tool message answering no call is dropped.
func assembleToolTurns(msgs []agent.Message) []agent.Message {
	// Index each tool message by its ToolCallID (first wins) so a result can
	// be found and pulled up next to its assistant turn regardless of where it
	// appears in the input order.
	toolByCall := make(map[string]agent.Message, len(msgs))
	for _, m := range msgs {
		if m.Role == agent.RoleTool {
			if _, seen := toolByCall[m.ToolCallID]; !seen {
				toolByCall[m.ToolCallID] = m
			}
		}
	}

	out := make([]agent.Message, 0, len(msgs))
	used := make(map[string]bool)
	for _, m := range msgs {
		switch m.Role {
		case agent.RoleTool:
			// Emitted only as part of its assistant turn, never on its own.
			continue
		case agent.RoleAssistant:
			out = append(out, m)
			for _, tc := range m.ToolCalls {
				if tm, ok := toolByCall[tc.ID]; ok && !used[tc.ID] {
					out = append(out, tm)
					used[tc.ID] = true
				} else {
					out = append(out, agent.Message{
						Role:       agent.RoleTool,
						ToolCallID: tc.ID,
						Name:       tc.Name,
						Content:    cancelledToolResult,
					})
				}
			}
		default:
			out = append(out, m)
		}
	}
	return out
}

// toolTurnSchemaHolds reports whether msgs satisfies the provider's tool-call
// schema: every assistant tool_calls message is immediately followed by one
// tool message per call, with no orphan tool messages and no interleaving
// non-tool messages. It is the single predicate the invariant is tested
// against.
func toolTurnSchemaHolds(msgs []agent.Message) bool {
	i := 0
	for i < len(msgs) {
		m := msgs[i]
		if m.Role == agent.RoleTool {
			return false // orphan tool message
		}
		if m.Role != agent.RoleAssistant || len(m.ToolCalls) == 0 {
			i++
			continue
		}

		want := make(map[string]bool, len(m.ToolCalls))
		for _, tc := range m.ToolCalls {
			want[tc.ID] = true
		}
		i++
		for i < len(msgs) && msgs[i].Role == agent.RoleTool {
			tm := msgs[i]
			if !want[tm.ToolCallID] {
				return false // tool message answering an unrequested call
			}
			delete(want, tm.ToolCallID)
			i++
		}
		if len(want) != 0 {
			return false // missing tool result(s)
		}
	}
	return true
}
