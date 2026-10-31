package durable

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	"vino/internal/agent"
)

// defaultRestartLimit is the number of consecutive crashes after which a task
// ends itself instead of running again. Values <= 0 on HarnessOptions resolve
// to this default.
const defaultRestartLimit = 5

// Extension is a named bundle of system-prompt sections, tools, and tasks.
// Conversations reference extensions by name only, so after a restart they
// pick up whatever the new process installs.
type Extension struct {
	Name         string
	Tools        agent.ToolRegistry
	Tasks        []TaskDefinition
	SystemPrompt []string
	Hooks        Hooks
	WrapTool     map[string]ToolWrapper
}

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
	// Documents declares the named documents every conversation can hold: their
	// default value and fork mode.
	Documents []DocumentDefinition
	// Extensions declares the named extension bundles conversations may
	// activate (by name) to gain tools, tasks, and system-prompt sections.
	Extensions []Extension
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
	docs         map[string]DocumentDefinition
	exts         map[string]Extension
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

	docs := make(map[string]DocumentDefinition, len(opts.Documents))
	for _, d := range opts.Documents {
		if d.Name == "" {
			return nil, errors.New("durable: document definition with empty name")
		}
		if _, exists := docs[d.Name]; exists {
			return nil, fmt.Errorf("durable: duplicate document definition %q", d.Name)
		}
		docs[d.Name] = d
	}

	exts := make(map[string]Extension, len(opts.Extensions))
	for _, e := range opts.Extensions {
		if e.Name == "" {
			return nil, errors.New("durable: extension with empty name")
		}
		if _, exists := exts[e.Name]; exists {
			return nil, fmt.Errorf("durable: duplicate extension %q", e.Name)
		}
		exts[e.Name] = e
	}
	for _, e := range opts.Extensions {
		defs = append(defs, e.Tasks...)
	}
	seenKinds := make(map[string]bool, len(defs))
	for _, d := range defs {
		if seenKinds[d.Kind()] {
			return nil, fmt.Errorf("durable: duplicate task kind %q", d.Kind())
		}
		seenKinds[d.Kind()] = true
	}

	h := &Harness{
		session:      session,
		llm:          opts.LLM,
		tools:        opts.Tools,
		model:        opts.Model,
		systemPrompt: opts.SystemPrompt,
		models:       opts.Models,
		defs:         defs,
		docs:         docs,
		exts:         exts,
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

// documentDef returns the registered definition for a document name.
func (h *Harness) documentDef(name string) (DocumentDefinition, bool) {
	d, ok := h.docs[name]
	return d, ok
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

// activeExtensions returns the extensions a conversation activates, in
// AgentConfig.Extensions order, skipping unknown names.
func (h *Harness) activeExtensions(cfg AgentConfig) []Extension {
	var out []Extension
	for _, name := range cfg.Extensions {
		if e, ok := h.exts[name]; ok {
			out = append(out, e)
		}
	}
	return out
}

// toolsFor returns the merged tool registry for a conversation: the base
// registry (allow-listed when cfg.Tools is set) followed by the active
// extensions' tools in cfg.Extensions order, where a later definition of a
// name shadows an earlier one.
func (h *Harness) toolsFor(cfg AgentConfig) agent.ToolRegistry {
	base := h.tools
	if len(cfg.Tools) > 0 && base != nil {
		allow := make(map[string]bool, len(cfg.Tools))
		for _, n := range cfg.Tools {
			allow[n] = true
		}
		base = toolFilter{base: base, allow: allow}
	}

	exts := h.activeExtensions(cfg)
	var regs []agent.ToolRegistry
	if base != nil {
		regs = append(regs, base)
	}
	for _, e := range exts {
		if e.Tools != nil {
			regs = append(regs, e.Tools)
		}
	}

	var merged agent.ToolRegistry
	switch len(regs) {
	case 0:
		merged = nil
	case 1:
		merged = regs[0]
	default:
		merged = mergeToolRegistries(regs...)
	}
	if merged == nil {
		return nil
	}

	// WrapTool decorates the winner under each name, after shadow resolution.
	// Iterate in reverse activation order so the earliest-activated wrapper
	// ends up outermost (runs first).
	for i := len(exts) - 1; i >= 0; i-- {
		for name, wrap := range exts[i].WrapTool {
			if wrap != nil {
				merged = wrapToolRegistry(merged, name, wrap)
			}
		}
	}
	return merged
}

// mergedTools composes tool registries by name: later registries shadow
// earlier ones. Specs() returns one spec per name (the winning spec) in a
// stable order; Call routes to the winning provider.
type mergedTools struct {
	specs    []agent.ToolSpec
	provider map[string]agent.ToolRegistry
}

// mergeToolRegistries builds a merged registry from registries in priority
// order (highest last).
func mergeToolRegistries(regs ...agent.ToolRegistry) agent.ToolRegistry {
	type item struct {
		spec agent.ToolSpec
		reg  agent.ToolRegistry
	}
	var list []item
	index := map[string]int{}
	for _, reg := range regs {
		if reg == nil {
			continue
		}
		for _, s := range reg.Specs() {
			if i, ok := index[s.Name]; ok {
				list[i] = item{s, reg}
			} else {
				index[s.Name] = len(list)
				list = append(list, item{s, reg})
			}
		}
	}
	m := &mergedTools{specs: make([]agent.ToolSpec, len(list)), provider: make(map[string]agent.ToolRegistry, len(list))}
	for i, it := range list {
		m.specs[i] = it.spec
		m.provider[it.spec.Name] = it.reg
	}
	return m
}

func (m *mergedTools) Specs() []agent.ToolSpec { return m.specs }

func (m *mergedTools) Call(ctx context.Context, name string, args json.RawMessage) (string, error) {
	if r, ok := m.provider[name]; ok && r != nil {
		return r.Call(ctx, name, args)
	}
	return "", fmt.Errorf("durable: tool %q is not registered", name)
}

// wrapToolRegistry decorates a registry's Call for one tool name: calls for
// name route through wrap (whose next reaches the underlying provider); all
// other names pass through unchanged. Specs are unaffected.
func wrapToolRegistry(reg agent.ToolRegistry, name string, wrap ToolWrapper) agent.ToolRegistry {
	return &wrappedTool{base: reg, name: name, wrap: wrap}
}

type wrappedTool struct {
	base agent.ToolRegistry
	name string
	wrap ToolWrapper
}

func (w *wrappedTool) Specs() []agent.ToolSpec { return w.base.Specs() }

func (w *wrappedTool) Call(ctx context.Context, name string, args json.RawMessage) (string, error) {
	if name != w.name {
		return w.base.Call(ctx, name, args)
	}
	next := func(ctx context.Context, n string, a json.RawMessage) (string, error) {
		return w.base.Call(ctx, n, a)
	}
	return w.wrap(ctx, name, args, next)
}

// extensionSections returns the system-prompt sections of a conversation's
// active extensions, in activation order, skipping unknown names.
func (h *Harness) extensionSections(ctx context.Context, conversationID ID) []string {
	conv, err := h.session.storage.Conversation(ctx, conversationID)
	if err != nil || conv == nil {
		return nil
	}
	var out []string
	for _, e := range h.activeExtensions(conv.Agent) {
		out = append(out, e.SystemPrompt...)
	}
	return out
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
