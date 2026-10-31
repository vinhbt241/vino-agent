package durable

import (
	"context"
	"errors"

	"vino/internal/agent"
)

// CommitOp is one committed batch and its sequence number.
type CommitOp struct {
	Seq    Seq
	Writes []StorageWrite
}

// View is a conversation's committed snapshot: the records a UI renders plus
// the Seq to subscribe from.
type View struct {
	Conversation ConversationRecord
	Entries      []EntryRecord
	Documents    []DocumentRecord
	Submissions  []SubmissionRecord
	Seq          Seq
}

// View returns the conversation's committed snapshot. The Seq is read before
// the records, so the snapshot is never ahead of its own sequence: a commit
// landing between the Seq read and the records read is re-delivered by Watch,
// never missed.
func (c *Conversation) View(ctx context.Context) (View, error) {
	seq := c.h.session.currentSeq()

	conv, err := c.h.session.storage.Conversation(ctx, c.id)
	if err != nil {
		return View{}, err
	}
	if conv == nil {
		return View{}, ErrConversationNotFound
	}

	entries, err := c.h.session.storage.ScanEntries(ctx, EntryQuery{ConversationID: c.id, Order: OrderAscending}, 0, nil)
	if err != nil {
		return View{}, err
	}
	docs, err := c.h.session.storage.ScanDocuments(ctx, DocumentQuery{ConversationID: &c.id, Order: OrderAscending}, 0, nil)
	if err != nil {
		return View{}, err
	}
	subs, err := c.h.session.storage.ScanSubmissions(ctx, SubmissionQuery{ConversationID: &c.id, Order: OrderAscending}, 0, nil)
	if err != nil {
		return View{}, err
	}

	return View{
		Conversation: *conv,
		Entries:      entries.Items,
		Documents:    docs.Items,
		Submissions:  subs.Items,
		Seq:          seq,
	}, nil
}

// Watch subscribes fn to the conversation's committed changes, catching up
// from after (delivering retained commits with Seq > after synchronously) then
// streaming live commits asynchronously. A non-nil return detaches the
// watcher. The returned cancel is idempotent.
func (c *Conversation) Watch(ctx context.Context, after Seq, fn func(CommitOp) error) (func(), error) {
	if fn == nil {
		return nil, errors.New("durable: nil watch func")
	}
	if conv, err := c.h.session.storage.Conversation(ctx, c.id); err != nil {
		return nil, err
	} else if conv == nil {
		return nil, ErrConversationNotFound
	}
	return c.h.session.Watch(after, func(w StorageWrite) bool { return viewWrite(c.id, w) }, fn), nil
}

// WatchEvents subscribes fn to the conversation's live run events. fn is
// called synchronously on each event; a non-nil return detaches the
// subscriber. The returned cancel is idempotent.
func (c *Conversation) WatchEvents(ctx context.Context, fn agent.EventSink) (func(), error) {
	if fn == nil {
		return nil, errors.New("durable: nil watch func")
	}
	if conv, err := c.h.session.storage.Conversation(ctx, c.id); err != nil {
		return nil, err
	} else if conv == nil {
		return nil, ErrConversationNotFound
	}
	return c.h.subscribeEvents(c.id, fn), nil
}

// viewWrite reports whether a committed write belongs to the conversation's
// view: its record, entries, documents, and submissions — not tasks.
func viewWrite(id ID, w StorageWrite) bool {
	switch w.Type {
	case WriteConversation:
		return w.Value.(*ConversationRecord).ID == id
	case WriteEntry:
		return w.Value.(*EntryRecord).ConversationID == id
	case WriteDocument:
		return w.Value.(*DocumentRecord).ConversationID == id
	case WriteSubmission:
		return w.Value.(*SubmissionRecord).ConversationID == id
	}
	return false
}

// filterWrites returns the writes matching match, in order.
func filterWrites(writes []StorageWrite, match func(StorageWrite) bool) []StorageWrite {
	var out []StorageWrite
	for _, w := range writes {
		if match(w) {
			out = append(out, w)
		}
	}
	return out
}

// cloneWrites deep-clones a commit batch so retained history never shares
// memory with callers.
func cloneWrites(writes []StorageWrite) []StorageWrite {
	out := make([]StorageWrite, len(writes))
	for i, w := range writes {
		out[i] = StorageWrite{Type: w.Type, Value: cloneWriteValue(w)}
	}
	return out
}

func cloneWriteValue(w StorageWrite) any {
	switch w.Type {
	case WriteConversation:
		return clone(w.Value.(*ConversationRecord))
	case WriteEntry:
		return clone(w.Value.(*EntryRecord))
	case WriteTask:
		return clone(w.Value.(*TaskRecord))
	case WriteSubmission:
		return clone(w.Value.(*SubmissionRecord))
	case WriteDocument:
		return clone(w.Value.(*DocumentRecord))
	}
	return nil
}
