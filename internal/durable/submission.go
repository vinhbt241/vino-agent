package durable

import (
	"context"

	"vino/internal/agent"
)

// Submission is an awaitable host submission backed by a SubmissionRecord.
type Submission struct {
	h   *Harness
	rec *SubmissionRecord
}

func (s *Submission) ID() ID            { return s.rec.ID }
func (s *Submission) RequestID() string { return s.rec.RequestID }

// Wait drives the harness to idle and returns the settled submission record.
func (s *Submission) Wait(ctx context.Context) (*SubmissionRecord, error) {
	if err := s.h.scheduler.Run(ctx); err != nil {
		return nil, err
	}
	return s.h.session.storage.Submission(ctx, s.rec.ID)
}

// SubmitInput is a user input submission.
type SubmitInput struct {
	Content   string
	RequestID string
}

// Conversation is a transcript scope handle.
type Conversation struct {
	h  *Harness
	id ID
}

func (c *Conversation) ID() ID { return c.id }

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
			return &Submission{h: c.h, rec: ex}, nil
		}
	}

	var sub *SubmissionRecord
	err := c.h.session.Commit(ctx, func(tx *Tx) error {
		if in.RequestID != "" {
			if ex, err := tx.SubmissionByRequest(ctx, in.RequestID); err != nil {
				return err
			} else if ex != nil {
				sub = ex
				return nil
			}
		}
		entryID, err := tx.MintID(ctx)
		if err != nil {
			return err
		}
		subID, err := tx.MintID(ctx)
		if err != nil {
			return err
		}
		taskID, err := tx.MintID(ctx)
		if err != nil {
			return err
		}

		tx.WriteEntry(&EntryRecord{
			ID:             entryID,
			ConversationID: c.id,
			Kind:           kindUser,
			Model:          []agent.Message{{Role: agent.RoleUser, Content: in.Content}},
		})

		sub = &SubmissionRecord{
			ID:             subID,
			ConversationID: c.id,
			RequestID:      in.RequestID,
			Type:           SubmissionInput,
			Status:         SubmissionPlaced,
			EntryID:        &entryID,
		}
		tx.WriteSubmission(sub)

		gen := c.h.definition(kindGeneration)
		input := mustJSON(generationInput{SubmissionID: subID})
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
	return &Submission{h: c.h, rec: sub}, nil
}

// Entries returns the conversation's entries in ID order (the raw transcript).
func (c *Conversation) Entries(ctx context.Context) ([]EntryRecord, error) {
	page, err := c.h.session.storage.ScanEntries(ctx, EntryQuery{ConversationID: c.id, Order: OrderAscending}, 0, nil)
	if err != nil {
		return nil, err
	}
	return page.Items, nil
}
