package durable

import (
	"context"
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

// Wait drives the harness to idle and returns the settled submission record.
func (s *Submission) Wait(ctx context.Context) (*SubmissionRecord, error) {
	if err := s.h.scheduler.Run(ctx); err != nil {
		return nil, err
	}
	return s.h.session.storage.Submission(ctx, s.rec.ID)
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
