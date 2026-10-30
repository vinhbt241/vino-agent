package durable

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"time"

	"vino/internal/agent"
)

// defaultRestartLimit is the number of consecutive crashes after which a task
// ends itself instead of running again. Values <= 0 on HarnessOptions resolve
// to this default.
const defaultRestartLimit = 5

// HarnessOptions wires a harness to the application's model, tools, and any
// additional task definitions.
type HarnessOptions struct {
	LLM          agent.LLM
	Tools        agent.ToolRegistry
	Model        string
	SystemPrompt string
	Definitions  []TaskDefinition
	// Models maps names to model clients for per-conversation selection via
	// AgentConfig.Model. Unnamed or unknown models fall back to LLM.
	Models map[string]agent.LLM
	// RestartLimit is the number of consecutive crashes after which a task
	// ends itself instead of running again. Values <= 0 resolve to the
	// default (5).
	RestartLimit int
}

// Harness is the host-facing facade over a Session and Scheduler.
type Harness struct {
	session      *Session
	scheduler    *Scheduler
	llm          agent.LLM
	tools        agent.ToolRegistry
	model        string
	systemPrompt string
	models       map[string]agent.LLM
	defs         []TaskDefinition
	restartLimit int

	sinkMu sync.Mutex
	sinks  map[ID]agent.EventSink

	abortMu     sync.Mutex
	abortCancel map[ID]context.CancelFunc
}

// resolveRestartLimit maps HarnessOptions.RestartLimit onto a concrete limit:
// a positive value is honored, anything else (zero, negative) is the default.
func resolveRestartLimit(limit int) int {
	if limit >= 1 {
		return limit
	}
	return defaultRestartLimit
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
		models:       opts.Models,
		defs:         defs,
		restartLimit: resolveRestartLimit(opts.RestartLimit),
		sinks:        make(map[ID]agent.EventSink),
		abortCancel:  make(map[ID]context.CancelFunc),
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

// Run drives every conversation with pending work to completion and returns
// when idle, running different conversations concurrently.
func (h *Harness) Run(ctx context.Context) error {
	return h.scheduler.RunAll(ctx)
}

// registerAbort records the cancel func of a conversation's active run so
// Conversation.Abort can interrupt it. Serialized per conversation by the
// scheduler's conversation lock, so at most one run is registered at a time.
func (h *Harness) registerAbort(conversationID ID, cancel context.CancelFunc) {
	h.abortMu.Lock()
	h.abortCancel[conversationID] = cancel
	h.abortMu.Unlock()
}

func (h *Harness) unregisterAbort(conversationID ID) {
	h.abortMu.Lock()
	delete(h.abortCancel, conversationID)
	h.abortMu.Unlock()
}

func (h *Harness) cancelAbort(conversationID ID) {
	h.abortMu.Lock()
	cancel := h.abortCancel[conversationID]
	h.abortMu.Unlock()
	if cancel != nil {
		cancel()
	}
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
// prompt, then the fork ancestors' raw history (root-most first, each cut at
// its child's fork point), then the conversation's own compaction summary (if
// any) and live tail.
func (h *Harness) buildMessages(ctx context.Context, conversationID ID, systemPrompt string) ([]agent.Message, error) {
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

// llmFor resolves the model client a conversation names, falling back to the
// harness default.
func (h *Harness) llmFor(model string) agent.LLM {
	if model != "" && h.models != nil {
		if m, ok := h.models[model]; ok {
			return m
		}
	}
	return h.llm
}

// toolsFor returns the tool registry for a conversation: the harness registry,
// filtered to the allow-list when the conversation configures one.
func (h *Harness) toolsFor(cfg AgentConfig) agent.ToolRegistry {
	if len(cfg.Tools) == 0 || h.tools == nil {
		return h.tools
	}
	allow := make(map[string]bool, len(cfg.Tools))
	for _, n := range cfg.Tools {
		allow[n] = true
	}
	return toolFilter{base: h.tools, allow: allow}
}

// toolFilter exposes a subset of a tool registry's specs. Call passes through
// to the base registry (only specs the model saw are ever requested).
type toolFilter struct {
	base  agent.ToolRegistry
	allow map[string]bool
}

func (f toolFilter) Specs() []agent.ToolSpec {
	if f.base == nil {
		return nil
	}
	var out []agent.ToolSpec
	for _, s := range f.base.Specs() {
		if f.allow[s.Name] {
			out = append(out, s)
		}
	}
	return out
}

func (f toolFilter) Call(ctx context.Context, name string, args json.RawMessage) (string, error) {
	return f.base.Call(ctx, name, args)
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
// (e.g. the client disconnected) detaches the sink and does not abort the
// run: a durable run must complete and persist a consistent transcript even
// when nobody is watching.
func (h *Harness) emit(submissionID ID, ev agent.Event) {
	h.sinkMu.Lock()
	sink := h.sinks[submissionID]
	h.sinkMu.Unlock()
	if sink == nil {
		return
	}
	if err := sink(ev); err != nil {
		h.unregisterSink(submissionID)
	}
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
