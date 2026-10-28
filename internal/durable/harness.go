package durable

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"time"

	"vino/internal/agent"
)

// HarnessOptions wires a harness to the application's model, tools, and any
// additional task definitions.
type HarnessOptions struct {
	LLM          agent.LLM
	Tools        agent.ToolRegistry
	Model        string
	SystemPrompt string
	Definitions  []TaskDefinition
}

// Harness is the host-facing facade over a Session and Scheduler.
type Harness struct {
	session      *Session
	scheduler    *Scheduler
	llm          agent.LLM
	tools        agent.ToolRegistry
	model        string
	systemPrompt string
	defs         []TaskDefinition

	sinkMu sync.Mutex
	sinks  map[ID]agent.EventSink
}

// Open opens a harness over a storage backend, ensures the root conversation
// exists, and reconciles any tasks left running by a previous process.
func Open(ctx context.Context, storage Storage, opts HarnessOptions) (*Harness, error) {
	if opts.LLM == nil {
		return nil, errors.New("durable: nil LLM")
	}
	session := OpenSession(storage)

	defs := append([]TaskDefinition(nil), builtinDefinitions()...)
	defs = append(defs, opts.Definitions...)

	h := &Harness{
		session:      session,
		llm:          opts.LLM,
		tools:        opts.Tools,
		model:        opts.Model,
		systemPrompt: opts.SystemPrompt,
		defs:         defs,
		sinks:        make(map[ID]agent.EventSink),
	}
	h.scheduler = NewScheduler(session, h, defs)

	if err := h.ensureRoot(ctx); err != nil {
		return nil, err
	}
	if err := h.scheduler.Reconcile(ctx); err != nil {
		return nil, err
	}
	return h, nil
}

func (h *Harness) definition(kind string) TaskDefinition {
	for _, d := range h.defs {
		if d.Kind() == kind {
			return d
		}
	}
	return nil
}

func (h *Harness) ensureRoot(ctx context.Context) error {
	if conv, err := h.session.storage.Conversation(ctx, RootConversationID); err != nil {
		return err
	} else if conv != nil {
		return nil
	}
	return h.session.CommitWrites(ctx, []StorageWrite{
		{Type: WriteConversation, Value: &ConversationRecord{ID: RootConversationID}},
	})
}

// Root returns the root conversation, created on first use and the same one
// after every restart.
func (h *Harness) Root() *Conversation {
	return &Conversation{h: h, id: RootConversationID}
}

// Run drives every pending task to completion and returns when idle.
func (h *Harness) Run(ctx context.Context) error {
	return h.scheduler.Run(ctx)
}

// Close closes the harness's storage backend.
func (h *Harness) Close(ctx context.Context) error {
	return h.session.storage.Close(ctx)
}

// Entry reads a committed transcript entry.
func (h *Harness) Entry(ctx context.Context, id ID) (*EntryRecord, error) {
	return h.session.storage.Entry(ctx, id)
}

// Conversation returns a handle for an existing conversation by ID, or
// ErrConversationNotFound if it does not exist.
func (h *Harness) Conversation(ctx context.Context, id ID) (*Conversation, error) {
	conv, err := h.session.storage.Conversation(ctx, id)
	if err != nil {
		return nil, err
	}
	if conv == nil {
		return nil, ErrConversationNotFound
	}
	return &Conversation{h: h, id: id}, nil
}

// ScanConversations pages over conversations matching q.
func (h *Harness) ScanConversations(ctx context.Context, q ConversationQuery, limit int, cursor *Cursor) (*Page[ConversationRecord], error) {
	return h.session.storage.ScanConversations(ctx, q, limit, cursor)
}

// StartInput seeds a fresh conversation with a message history and runs it.
type StartInput struct {
	RequestID     string
	SystemPrompt  string
	Messages      []agent.Message // user/assistant turns, in order
	MaxIterations int             // <= 0 means the default cap
	ToolTimeout   time.Duration   // <= 0 means no timeout
}

// Start creates a fresh conversation, writes the message history as its
// transcript, admits a submission, and streams events to sink while the run
// executes. A RequestID makes it exactly-once across crash retries.
func (h *Harness) Start(ctx context.Context, in StartInput, sink agent.EventSink) (*Submission, error) {
	if in.RequestID != "" {
		if ex, err := h.session.storage.SubmissionByRequest(ctx, in.RequestID); err != nil {
			return nil, err
		} else if ex != nil {
			h.registerSink(ex.ID, sink)
			return &Submission{h: h, rec: ex, resumed: true}, nil
		}
	}

	var sub *SubmissionRecord
	resumed := false
	err := h.session.Commit(ctx, func(tx *Tx) error {
		if in.RequestID != "" {
			if ex, err := tx.SubmissionByRequest(ctx, in.RequestID); err != nil {
				return err
			} else if ex != nil {
				sub = ex
				resumed = true
				return nil
			}
		}

		convID, err := tx.MintID(ctx)
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

		tx.WriteConversation(&ConversationRecord{ID: convID})

		var lastEntry *ID
		for _, m := range in.Messages {
			id, err := tx.MintID(ctx)
			if err != nil {
				return err
			}
			kind := kindUser
			if m.Role == agent.RoleAssistant {
				kind = kindAssistant
			}
			tx.WriteEntry(&EntryRecord{ID: id, ConversationID: convID, Kind: kind, Model: []agent.Message{m}})
			lastEntry = &id
		}

		sub = &SubmissionRecord{
			ID:             subID,
			ConversationID: convID,
			RequestID:      in.RequestID,
			Type:           SubmissionInput,
			Status:         SubmissionPlaced,
			EntryID:        lastEntry,
		}
		tx.WriteSubmission(sub)

		gen := h.definition(kindGeneration)
		raw := mustJSON(generationInput{
			SubmissionID:  subID,
			SystemPrompt:  in.SystemPrompt,
			MaxIterations: in.MaxIterations,
			ToolTimeoutMs: in.ToolTimeout.Milliseconds(),
		})
		tx.WriteTask(&TaskRecord{
			ID:             taskID,
			ConversationID: convID,
			Kind:           kindGeneration,
			Version:        gen.Version(),
			Input:          raw,
			State:          TaskState{Status: TaskPending, Checkpoint: gen.Initial(raw)},
		})
		return nil
	})
	if err != nil {
		return nil, err
	}
	h.registerSink(sub.ID, sink)
	return &Submission{h: h, rec: sub, resumed: resumed}, nil
}

// buildMessages derives the model context for a conversation: the system
// prompt followed by the conversation's entries in ID order.
func (h *Harness) buildMessages(ctx context.Context, conversationID ID, systemPrompt string) ([]agent.Message, error) {
	var msgs []agent.Message
	if systemPrompt == "" {
		systemPrompt = h.systemPrompt
	}
	if systemPrompt != "" {
		msgs = append(msgs, agent.Message{Role: agent.RoleSystem, Content: systemPrompt})
	}
	page, err := h.session.storage.ScanEntries(ctx, EntryQuery{ConversationID: conversationID, Order: OrderAscending}, 0, nil)
	if err != nil {
		return nil, err
	}
	for i := range page.Items {
		msgs = append(msgs, page.Items[i].Model...)
	}
	return msgs, nil
}

// ---- event sinks ----

func (h *Harness) registerSink(submissionID ID, sink agent.EventSink) {
	if sink == nil {
		return
	}
	h.sinkMu.Lock()
	h.sinks[submissionID] = sink
	h.sinkMu.Unlock()
}

func (h *Harness) unregisterSink(submissionID ID) {
	h.sinkMu.Lock()
	delete(h.sinks, submissionID)
	h.sinkMu.Unlock()
}

// emit delivers one run event to the submission's sink, if any. A sink error
// aborts the run.
func (h *Harness) emit(submissionID ID, ev agent.Event) error {
	h.sinkMu.Lock()
	sink := h.sinks[submissionID]
	h.sinkMu.Unlock()
	if sink == nil {
		return nil
	}
	return sink(ev)
}

// onTaskFault settles the submission a faulted generation task was answering,
// so the client sees a terminal state instead of a stuck "placed" submission.
func (h *Harness) onTaskFault(ctx context.Context, task *TaskRecord, msg string) {
	if task.Kind != kindGeneration {
		return
	}
	var in generationInput
	if err := json.Unmarshal(task.Input, &in); err != nil || in.SubmissionID == 0 {
		return
	}
	h.settleUnanswered(ctx, in.SubmissionID, msg)
}

func (h *Harness) settleUnanswered(ctx context.Context, submissionID ID, reason string) {
	err := h.session.Commit(ctx, func(tx *Tx) error {
		sub, err := tx.Submission(ctx, submissionID)
		if err != nil || sub == nil {
			return err
		}
		if sub.Status == SubmissionDone || sub.Status == SubmissionUnanswered {
			return nil // already settled
		}
		updated := *sub
		updated.Status = SubmissionUnanswered
		updated.Reason = reason
		tx.WriteSubmission(&updated)
		return nil
	})
	if err == nil {
		h.unregisterSink(submissionID)
	}
}
