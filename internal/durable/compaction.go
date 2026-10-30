package durable

import (
	"context"
	"strings"

	"vino/internal/agent"
)

const (
	// kindCompaction marks an entry that holds a compaction summary. Its
	// Model is a single system message carrying the summary, and its Head
	// points at the last entry the summary covers.
	kindCompaction = "compaction"

	defaultCompactInstructions = "Summarize the conversation above, preserving important facts, decisions, actions taken, and open questions. Be concise but complete."
)

// latestCompaction returns the conversation's most recent compaction entry
// (largest ID), or nil when there is none.
func (h *Harness) latestCompaction(ctx context.Context, conversationID ID) (*EntryRecord, error) {
	page, err := h.session.storage.ScanEntries(ctx, EntryQuery{ConversationID: conversationID, Order: OrderDescending}, 0, nil)
	if err != nil {
		return nil, err
	}
	for i := range page.Items {
		if page.Items[i].Kind == kindCompaction {
			e := page.Items[i]
			return &e, nil
		}
	}
	return nil, nil
}

// liveTail returns the entries the model still sees verbatim — everything
// after the latest compaction's head, excluding compaction entries — together
// with the latest compaction entry itself (nil when none). Without a
// compaction entry the tail is the whole transcript.
func (h *Harness) liveTail(ctx context.Context, conversationID ID) ([]EntryRecord, *EntryRecord, error) {
	comp, err := h.latestCompaction(ctx, conversationID)
	if err != nil {
		return nil, nil, err
	}
	page, err := h.session.storage.ScanEntries(ctx, EntryQuery{ConversationID: conversationID, Order: OrderAscending}, 0, nil)
	if err != nil {
		return nil, nil, err
	}
	tail := make([]EntryRecord, 0, len(page.Items))
	for _, e := range page.Items {
		if e.Kind == kindCompaction {
			continue
		}
		if comp != nil && comp.Head != nil && e.ID <= *comp.Head {
			continue
		}
		tail = append(tail, e)
	}
	return tail, comp, nil
}

// Compact summarizes the conversation so far into a durable compaction entry.
// instructions become the system prompt for the summarization call. Compacting
// an empty (or already fully summarized) conversation is a no-op. The older
// entries are never deleted; they remain in storage and in Entries.
func (c *Conversation) Compact(ctx context.Context, instructions string) error {
	tail, comp, err := c.h.liveTail(ctx, c.id)
	if err != nil {
		return err
	}
	if len(tail) == 0 {
		return nil
	}

	instr := instructions
	if instr == "" {
		instr = defaultCompactInstructions
	}
	msgs := []agent.Message{{Role: agent.RoleSystem, Content: instr}}
	if comp != nil && len(comp.Model) > 0 {
		msgs = append(msgs, agent.Message{Role: agent.RoleSystem, Content: "Previous summary:\n" + comp.Model[0].Content})
	}
	for _, e := range tail {
		msgs = append(msgs, e.Model...)
	}
	// Keep the summarization prompt well-formed: reattach tool results to their
	// assistant tool_calls and fill any missing ones.
	msgs = assembleToolTurns(msgs)

	resp, err := c.h.llm.Chat(ctx, msgs, nil, nil)
	if err != nil {
		return err
	}
	summary := strings.TrimSpace(resp.Content)
	if summary == "" {
		return nil
	}

	head := tail[len(tail)-1].ID
	return c.h.session.Commit(ctx, func(tx *Tx) error {
		id, err := tx.MintID(ctx)
		if err != nil {
			return err
		}
		tx.WriteEntry(&EntryRecord{
			ID:             id,
			ConversationID: c.id,
			Kind:           kindCompaction,
			Head:           &head,
			Model:          []agent.Message{{Role: agent.RoleSystem, Content: summary}},
		})
		return nil
	})
}
