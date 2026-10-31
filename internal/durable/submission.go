package durable

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"time"

	"vino/internal/agent"
)

// Submission is an awaitable host submission backed by a SubmissionRecord.
type Submission struct {
	h       *Harness
	rec     *SubmissionRecord
	resumed bool
}

func (s *Submission) ID() ID             { return s.rec.ID }
func (s *Submission) RequestID() string  { return s.rec.RequestID }
func (s *Submission) ConversationID() ID { return s.rec.ConversationID }

// Resumed reports whether this submission pre-existed (its request ID matched
// an already-admitted submission) rather than being newly created.
func (s *Submission) Resumed() bool { return s.resumed }

// Wait drives the submission's conversation to idle and returns the settled
// submission record.
func (s *Submission) Wait(ctx context.Context) (*SubmissionRecord, error) {
	if err := s.h.scheduler.RunConversation(ctx, s.rec.ConversationID); err != nil {
		return nil, err
	}
	// Read the settled record detached from the request context: the run may
	// have been cancelled (ctx done) yet still settled durably.
	return s.h.session.storage.Submission(context.WithoutCancel(ctx), s.rec.ID)
}

// SubmitInput is a user input submission.
type SubmitInput struct {
	Content string
	// Messages is the turn list to append, in order. When non-empty it is
	// used instead of Content (Content is the single-user-turn convenience).
	Messages      []agent.Message
	RequestID     string
	SystemPrompt  string
	MaxIterations int
	ToolTimeout   time.Duration
}

// Conversation is a transcript scope handle.
type Conversation struct {
	h  *Harness
	id ID
}

func (c *Conversation) ID() ID { return c.id }

// Record returns the conversation's record (its fork/owner edges).
func (c *Conversation) Record(ctx context.Context) (*ConversationRecord, error) {
	return c.h.session.storage.Conversation(ctx, c.id)
}

// Fork creates a child conversation cut at entry at: the fork's context is the
// parent's history up to and including at, plus whatever the fork itself
// appends. The parent's entries are referenced, never copied. cfg overrides
// the fork's agent configuration (empty fields inherit the harness defaults).
func (c *Conversation) Fork(ctx context.Context, at *EntryRecord, cfg AgentConfig) (*Conversation, error) {
	if at == nil {
		return nil, errors.New("durable: fork: nil entry")
	}
	if at.ConversationID != c.id {
		return nil, fmt.Errorf("durable: fork: entry %d belongs to conversation %d, not %d", at.ID, at.ConversationID, c.id)
	}
	var child *Conversation
	err := c.h.session.Commit(ctx, func(tx *Tx) error {
		id, err := tx.MintID(ctx)
		if err != nil {
			return err
		}
		tx.WriteConversation(&ConversationRecord{
			ID:     id,
			Parent: &ForkRef{ConversationID: c.id, At: at.ID},
			Agent:  cfg,
		})
		child = &Conversation{h: c.h, id: id}

		// Copy documents per their fork mode: current copies the parent's
		// resolved value as an explicit snapshot; fresh writes nothing (the
		// child resolves to the default on read).
		names := make([]string, 0, len(c.h.docs))
		for name := range c.h.docs {
			names = append(names, name)
		}
		sort.Strings(names)
		for _, name := range names {
			def := c.h.docs[name]
			if resolveFork(def.Fork) == DocumentForkFresh {
				continue
			}
			value := def.Default
			if rec, err := tx.DocumentByName(ctx, c.id, name); err != nil {
				return err
			} else if rec != nil {
				value = rec.Value
			}
			docID, err := tx.MintID(ctx)
			if err != nil {
				return err
			}
			tx.WriteDocument(&DocumentRecord{ID: docID, ConversationID: id, Name: name, Value: value})
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return child, nil
}

// Document returns a document's committed value, or its definition's default
// when the conversation has never written it. The name must be declared.
func (c *Conversation) Document(ctx context.Context, name string) (json.RawMessage, error) {
	def, ok := c.h.documentDef(name)
	if !ok {
		return nil, ErrDocumentUndeclared
	}
	rec, err := c.h.session.storage.DocumentByName(ctx, c.id, name)
	if err != nil {
		return nil, err
	}
	if rec != nil {
		return rec.Value, nil
	}
	return append(json.RawMessage(nil), def.Default...), nil
}

// Configure replaces the conversation's agent configuration.
func (c *Conversation) Configure(ctx context.Context, cfg AgentConfig) error {
	return c.h.session.Commit(ctx, func(tx *Tx) error {
		rec, err := tx.Conversation(ctx, c.id)
		if err != nil {
			return err
		}
		if rec == nil {
			return ErrConversationNotFound
		}
		rec.Agent = cfg
		tx.WriteConversation(rec)
		return nil
	})
}

// Submit admits a user input: a user entry, a submission record, and a
// generation task, all in one atomic commit. A RequestID makes the submission
// exactly-once across crash retries. It is SubmitStream without a live event
// sink.
func (c *Conversation) Submit(ctx context.Context, in SubmitInput) (*Submission, error) {
	return c.SubmitStream(ctx, in, nil)
}

// SubmitStream admits a user input and, while the run executes, emits agent
// events (text deltas, tool calls, tool results, run finished) to sink.
func (c *Conversation) SubmitStream(ctx context.Context, in SubmitInput, sink agent.EventSink) (*Submission, error) {
	if in.RequestID != "" {
		if ex, err := c.h.session.storage.SubmissionByRequest(ctx, in.RequestID); err != nil {
			return nil, err
		} else if ex != nil {
			c.h.registerSink(ex.ID, sink)
			return &Submission{h: c.h, rec: ex, resumed: true}, nil
		}
	}

	var sub *SubmissionRecord
	resumed := false
	err := c.h.session.Commit(ctx, func(tx *Tx) error {
		if in.RequestID != "" {
			if ex, err := tx.SubmissionByRequest(ctx, in.RequestID); err != nil {
				return err
			} else if ex != nil {
				sub = ex
				resumed = true
				return nil
			}
		}
		subID, err := tx.MintID(ctx)
		if err != nil {
			return err
		}
		taskID, err := tx.MintID(ctx)
		if err != nil {
			return err
		}

		msgs := in.Messages
		if len(msgs) == 0 {
			msgs = []agent.Message{{Role: agent.RoleUser, Content: in.Content}}
		}
		var lastEntry *ID
		for _, m := range msgs {
			entryID, err := tx.MintID(ctx)
			if err != nil {
				return err
			}
			kind := kindUser
			if m.Role == agent.RoleAssistant {
				kind = kindAssistant
			}
			tx.WriteEntry(&EntryRecord{
				ID:             entryID,
				ConversationID: c.id,
				Kind:           kind,
				Model:          []agent.Message{m},
			})
			lastEntry = &entryID
		}

		sub = &SubmissionRecord{
			ID:             subID,
			ConversationID: c.id,
			RequestID:      in.RequestID,
			Type:           SubmissionInput,
			Status:         SubmissionPlaced,
			EntryID:        lastEntry,
		}
		tx.WriteSubmission(sub)

		gen := c.h.definition(kindGeneration)
		input := mustJSON(generationInput{
			SubmissionID:  subID,
			SystemPrompt:  in.SystemPrompt,
			MaxIterations: in.MaxIterations,
			ToolTimeoutMs: in.ToolTimeout.Milliseconds(),
		})
		tx.WriteTask(&TaskRecord{
			ID:             taskID,
			ConversationID: c.id,
			Kind:           kindGeneration,
			Version:        gen.Version(),
			Input:          input,
			State:          TaskState{Status: TaskPending, Checkpoint: gen.Initial(input)},
		})
		return nil
	})
	if err != nil {
		return nil, err
	}
	c.h.registerSink(sub.ID, sink)
	return &Submission{h: c.h, rec: sub, resumed: resumed}, nil
}

// Abort marks every foreground task in the conversation aborted, settles any
// open submissions unanswered, and cancels the conversation's in-flight run
// (its tools and model calls observe the cancellation). Settling is idempotent,
// so a marked generation's own settle later is a no-op.
func (c *Conversation) Abort(ctx context.Context) error {
	if err := c.h.session.Commit(ctx, func(tx *Tx) error {
		page, err := tx.ScanTasks(ctx, TaskQuery{ConversationID: &c.id})
		if err != nil {
			return err
		}
		for i := range page.Items {
			t := page.Items[i]
			if t.Background || t.State.Status == TaskTerminal || t.State.Status == TaskCompleting {
				continue
			}
			t.AbortRequested = true
			tx.WriteTask(&t)
		}

		subs, err := tx.ScanSubmissions(ctx, SubmissionQuery{ConversationID: &c.id})
		if err != nil {
			return err
		}
		for i := range subs.Items {
			s := subs.Items[i]
			if s.Status == SubmissionDone || s.Status == SubmissionUnanswered {
				continue
			}
			s.Status = SubmissionUnanswered
			s.Reason = "aborted"
			tx.WriteSubmission(&s)
		}
		return nil
	}); err != nil {
		return err
	}
	c.h.cancelAbort(c.id)
	return nil
}

// Entries returns the conversation's entries in ID order (the raw transcript).
func (c *Conversation) Entries(ctx context.Context) ([]EntryRecord, error) {
	page, err := c.h.session.storage.ScanEntries(ctx, EntryQuery{ConversationID: c.id, Order: OrderAscending}, 0, nil)
	if err != nil {
		return nil, err
	}
	return page.Items, nil
}

// LatestMessage returns the last user or assistant entry with non-empty
// content, in ID order, skipping tool entries and empty assistant turns
// (e.g. a tool-call turn with no prose). It returns nil when there is no such
// entry. The entry's content is not truncated here; that is a presentation
// concern for the API layer.
func (c *Conversation) LatestMessage(ctx context.Context) (*EntryRecord, error) {
	entries, err := c.Entries(ctx)
	if err != nil {
		return nil, err
	}
	for i := len(entries) - 1; i >= 0; i-- {
		e := entries[i]
		if e.Kind != kindUser && e.Kind != kindAssistant {
			continue
		}
		if len(e.Model) > 0 && e.Model[0].Content != "" {
			return &e, nil
		}
	}
	return nil, nil
}
