package durable

import (
	"context"

	"vino/internal/agent"
)

// contextMessages derives the model context for a conversation: the system
// prompt, then the fork ancestors' raw history (root-most first, each cut at
// its child's fork point), then the conversation's own compaction summary (if
// any) and live tail. The result satisfies the provider's tool-call schema.
//
// It is the single entry point for "what the model sees": the ancestor walk,
// tail selection, and schema assembly are all internal to this module.
func (h *Harness) contextMessages(ctx context.Context, conversationID ID, systemPrompt string) ([]agent.Message, error) {
	var msgs []agent.Message
	if systemPrompt == "" {
		systemPrompt = h.systemPrompt
	}
	if systemPrompt != "" {
		msgs = append(msgs, agent.Message{Role: agent.RoleSystem, Content: systemPrompt})
	}

	ancestors, err := h.ancestorEntries(ctx, conversationID)
	if err != nil {
		return nil, err
	}
	for _, e := range ancestors {
		msgs = append(msgs, e.Model...)
	}

	tail, comp, err := h.liveTail(ctx, conversationID)
	if err != nil {
		return nil, err
	}
	if comp != nil && len(comp.Model) > 0 {
		msgs = append(msgs, comp.Model[0])
	}
	for _, e := range tail {
		msgs = append(msgs, e.Model...)
	}
	return assembleToolTurns(msgs), nil
}

// ancestorEntries returns the raw (non-compaction) entries from the
// conversation's fork ancestors, in context order (root-most ancestor first),
// each cut at the fork point its child was cut at.
func (h *Harness) ancestorEntries(ctx context.Context, conversationID ID) ([]EntryRecord, error) {
	type link struct {
		convID ID
		cut    ID
	}
	var links []link
	cur := conversationID
	for {
		rec, err := h.session.storage.Conversation(ctx, cur)
		if err != nil {
			return nil, err
		}
		if rec == nil || rec.Parent == nil {
			break
		}
		links = append(links, link{rec.Parent.ConversationID, rec.Parent.At})
		cur = rec.Parent.ConversationID
	}
	var out []EntryRecord
	for i := len(links) - 1; i >= 0; i-- {
		entries, err := h.rawEntriesUpTo(ctx, links[i].convID, &links[i].cut)
		if err != nil {
			return nil, err
		}
		out = append(out, entries...)
	}
	return out, nil
}

// rawEntriesUpTo returns a conversation's entries with ID <= cut (when cut is
// non-nil), excluding compaction entries, in ascending order.
func (h *Harness) rawEntriesUpTo(ctx context.Context, convID ID, cut *ID) ([]EntryRecord, error) {
	q := EntryQuery{ConversationID: convID, Order: OrderAscending}
	if cut != nil {
		q.MaxID = cut
	}
	page, err := h.session.storage.ScanEntries(ctx, q, 0, nil)
	if err != nil {
		return nil, err
	}
	var out []EntryRecord
	for _, e := range page.Items {
		if e.Kind == kindCompaction {
			continue
		}
		out = append(out, e)
	}
	return out, nil
}

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
