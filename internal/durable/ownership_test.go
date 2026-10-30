package durable

import (
	"context"
	"encoding/json"
	"sync"
	"testing"
	"time"

	"vino/internal/agent"
)

// cascadeRegistry blocks in its "block" tool until the context is cancelled,
// and records every call including ones that would run after a cancel.
type cascadeRegistry struct {
	blockStarted chan struct{}
	blockDone    chan struct{}

	mu    sync.Mutex
	calls []string
}

func (c *cascadeRegistry) Specs() []agent.ToolSpec {
	return []agent.ToolSpec{
		{Name: "block", Replay: agent.ReplaySafe},
		{Name: "search", Replay: agent.ReplaySafe},
	}
}

func (c *cascadeRegistry) Call(ctx context.Context, name string, _ json.RawMessage) (string, error) {
	c.mu.Lock()
	c.calls = append(c.calls, name)
	c.mu.Unlock()
	if name == "block" {
		close(c.blockStarted)
		<-ctx.Done()
		close(c.blockDone)
		return "", ctx.Err()
	}
	return "hit", nil
}

func TestToolTaskRestartLimitFaultsWithoutRerunning(t *testing.T) {
	ctx := context.Background()
	st := NewMemoryStorage()
	write(t, st, ctx, []StorageWrite{
		{Type: WriteConversation, Value: &ConversationRecord{ID: RootConversationID}},
	})
	taskID := mint(t, st, ctx)
	raw := mustJSON(toolInput{CallID: "call1", Name: "search", Arguments: json.RawMessage(`{}`), ReplaySafe: true})
	write(t, st, ctx, []StorageWrite{
		{Type: WriteTask, Value: &TaskRecord{
			ID:             taskID,
			ConversationID: RootConversationID,
			Kind:           kindToolTask,
			Version:        1,
			Input:          raw,
			Restarts:       5,
			State:          TaskState{Status: TaskPending, Checkpoint: toolTaskDef{}.Initial(raw)},
		}},
	})

	tools := &fakeRegistry{results: map[string]string{"search": "hit"}}
	h, err := Open(ctx, st, HarnessOptions{LLM: &fakeLLM{}, Tools: tools})
	if err != nil {
		t.Fatal(err)
	}
	if err := h.Run(ctx); err != nil {
		t.Fatal(err)
	}

	if len(tools.calls) != 0 {
		t.Fatalf("tool calls = %v, want none (a task cut off at the restart limit must not re-run)", tools.calls)
	}
	entries, err := h.Root().Entries(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if got := toolEntryContent(t, entries); got != "Tool search was interrupted 5 times in a row; it is not run again" {
		t.Fatalf("tool entry content = %q", got)
	}
	task, err := st.Task(ctx, taskID)
	if err != nil {
		t.Fatal(err)
	}
	if task.State.Status != TaskTerminal || task.State.Outcome == nil || task.State.Outcome.Status != OutcomeFailed {
		t.Fatalf("task state = %+v, want terminal failed", task.State)
	}
}

func TestAbortRequestedToolSettlesAbortedOnReopen(t *testing.T) {
	ctx := context.Background()
	st := NewMemoryStorage()
	write(t, st, ctx, []StorageWrite{
		{Type: WriteConversation, Value: &ConversationRecord{ID: RootConversationID}},
	})
	taskID := mint(t, st, ctx)
	raw := mustJSON(toolInput{CallID: "call1", Name: "search", Arguments: json.RawMessage(`{}`), ReplaySafe: true})
	write(t, st, ctx, []StorageWrite{
		{Type: WriteTask, Value: &TaskRecord{
			ID:             taskID,
			ConversationID: RootConversationID,
			Kind:           kindToolTask,
			Version:        1,
			Input:          raw,
			AbortRequested: true,
			State:          TaskState{Status: TaskPending, Checkpoint: toolTaskDef{}.Initial(raw)},
		}},
	})

	tools := &fakeRegistry{results: map[string]string{"search": "hit"}}
	h, err := Open(ctx, st, HarnessOptions{LLM: &fakeLLM{}, Tools: tools})
	if err != nil {
		t.Fatal(err)
	}
	if err := h.Run(ctx); err != nil {
		t.Fatal(err)
	}

	if len(tools.calls) != 0 {
		t.Fatalf("tool calls = %v, want none (an aborted task must not re-run its effect on reopen)", tools.calls)
	}
	task, err := st.Task(ctx, taskID)
	if err != nil {
		t.Fatal(err)
	}
	if task.State.Status != TaskTerminal || task.State.Outcome == nil || task.State.Outcome.Status != OutcomeAborted {
		t.Fatalf("task state = %+v, want terminal aborted", task.State)
	}
}

func TestCancelCascadeMarksSiblingTool(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	reg := &cascadeRegistry{blockStarted: make(chan struct{}), blockDone: make(chan struct{})}
	llm := &fakeLLM{steps: []llmStep{{
		resp: agent.Response{ToolCalls: []agent.ToolCall{
			{ID: "c1", Name: "block", Arguments: json.RawMessage(`{}`)},
			{ID: "c2", Name: "search", Arguments: json.RawMessage(`{}`)},
		}},
	}}}
	h, err := Open(ctx, NewMemoryStorage(), HarnessOptions{LLM: llm, Tools: reg})
	if err != nil {
		t.Fatal(err)
	}

	sub, err := h.Root().Submit(ctx, SubmitInput{Content: "go", RequestID: "r1"})
	if err != nil {
		t.Fatal(err)
	}
	waitCh := make(chan *SubmissionRecord, 1)
	errCh := make(chan error, 1)
	go func() {
		rec, err := sub.Wait(ctx)
		if err != nil {
			errCh <- err
			return
		}
		waitCh <- rec
	}()

	select {
	case <-reg.blockStarted:
	case <-time.After(5 * time.Second):
		t.Fatal("block tool never started")
	}

	cancel()

	select {
	case <-reg.blockDone:
	case <-time.After(5 * time.Second):
		t.Fatal("block tool never observed cancellation")
	}

	var rec *SubmissionRecord
	select {
	case rec = <-waitCh:
	case err := <-errCh:
		t.Fatalf("Wait: %v", err)
	case <-time.After(5 * time.Second):
		t.Fatal("Wait never returned")
	}
	if rec.Status != SubmissionUnanswered {
		t.Fatalf("status = %s, want unanswered", rec.Status)
	}

	reg.mu.Lock()
	calls := append([]string(nil), reg.calls...)
	reg.mu.Unlock()
	for _, name := range calls {
		if name == "search" {
			t.Fatalf("sibling tool ran after abort: calls = %v", calls)
		}
	}
}

func TestBackgroundToolSurvivesForegroundAbort(t *testing.T) {
	ctx := context.Background()
	st := NewMemoryStorage()
	write(t, st, ctx, []StorageWrite{
		{Type: WriteConversation, Value: &ConversationRecord{ID: RootConversationID}},
	})
	genID := mint(t, st, ctx)
	fgID := mint(t, st, ctx)
	bgID := mint(t, st, ctx)
	genInput := mustJSON(generationInput{SubmissionID: 0})
	fgInput := mustJSON(toolInput{CallID: "c-fg", Name: "a", Arguments: json.RawMessage(`{}`), ReplaySafe: true})
	bgInput := mustJSON(toolInput{CallID: "c-bg", Name: "b", Arguments: json.RawMessage(`{}`), ReplaySafe: true})
	write(t, st, ctx, []StorageWrite{
		{Type: WriteTask, Value: &TaskRecord{
			ID: genID, ConversationID: RootConversationID, Kind: kindGeneration, Version: 1,
			Input:          genInput,
			AbortRequested: true,
			State:          TaskState{Status: TaskWaiting, Checkpoint: mustJSON(generationCheckpoint{Phase: "tools"}), On: []ID{fgID, bgID}},
		}},
		{Type: WriteTask, Value: &TaskRecord{
			ID: fgID, ConversationID: RootConversationID, Kind: kindToolTask, Version: 1,
			Input: fgInput, OwnerID: &genID, AbortRequested: true,
			State: TaskState{Status: TaskPending, Checkpoint: toolTaskDef{}.Initial(fgInput)},
		}},
		{Type: WriteTask, Value: &TaskRecord{
			ID: bgID, ConversationID: RootConversationID, Kind: kindToolTask, Version: 1,
			Input: bgInput, OwnerID: &genID, Background: true,
			State: TaskState{Status: TaskPending, Checkpoint: toolTaskDef{}.Initial(bgInput)},
		}},
	})

	tools := &fakeRegistry{results: map[string]string{"a": "ra", "b": "rb"}}
	h, err := Open(ctx, st, HarnessOptions{LLM: &fakeLLM{}, Tools: tools})
	if err != nil {
		t.Fatal(err)
	}
	if err := h.Run(ctx); err != nil {
		t.Fatal(err)
	}

	// The background child is not reached by the cascade: it still runs and
	// completes; the foreground child settles aborted without running.
	if len(tools.calls) != 1 || tools.calls[0] != "b" {
		t.Fatalf("tool calls = %v, want [b] (background child runs, foreground child aborts)", tools.calls)
	}
	bg, err := st.Task(ctx, bgID)
	if err != nil {
		t.Fatal(err)
	}
	if bg.State.Status != TaskTerminal || bg.State.Outcome == nil || bg.State.Outcome.Status != OutcomeCompleted {
		t.Fatalf("background task state = %+v, want terminal completed", bg.State)
	}
	fg, err := st.Task(ctx, fgID)
	if err != nil {
		t.Fatal(err)
	}
	if fg.State.Status != TaskTerminal || fg.State.Outcome == nil || fg.State.Outcome.Status != OutcomeAborted {
		t.Fatalf("foreground task state = %+v, want terminal aborted", fg.State)
	}
	gen, err := st.Task(ctx, genID)
	if err != nil {
		t.Fatal(err)
	}
	if gen.State.Status != TaskTerminal || gen.State.Outcome == nil || gen.State.Outcome.Status != OutcomeAborted {
		t.Fatalf("generation state = %+v, want terminal aborted", gen.State)
	}
}

// TestMarkTreeSkipsBackgroundSubtree pins the cascade's reach directly: the
// public seam cannot observe it, because the serial scheduler's global cancel
// reaches a background task before its background-ness matters (deferred to
// the concurrent scheduler). The cascade itself must still not mark background
// subtrees.
func TestMarkTreeSkipsBackgroundSubtree(t *testing.T) {
	ctx := context.Background()
	st := NewMemoryStorage()
	write(t, st, ctx, []StorageWrite{
		{Type: WriteConversation, Value: &ConversationRecord{ID: RootConversationID}},
	})
	g := mint(t, st, ctx)
	a := mint(t, st, ctx)
	b := mint(t, st, ctx)
	c := mint(t, st, ctx)
	self := mint(t, st, ctx)
	write(t, st, ctx, []StorageWrite{
		{Type: WriteTask, Value: &TaskRecord{ID: g, ConversationID: RootConversationID, Kind: kindGeneration, Version: 1, State: TaskState{Status: TaskWaiting}}},
		{Type: WriteTask, Value: &TaskRecord{ID: a, ConversationID: RootConversationID, Kind: kindToolTask, Version: 1, OwnerID: &g, State: TaskState{Status: TaskPending}}},
		{Type: WriteTask, Value: &TaskRecord{ID: b, ConversationID: RootConversationID, Kind: kindToolTask, Version: 1, OwnerID: &g, Background: true, State: TaskState{Status: TaskPending}}},
		{Type: WriteTask, Value: &TaskRecord{ID: c, ConversationID: RootConversationID, Kind: kindToolTask, Version: 1, OwnerID: &b, State: TaskState{Status: TaskPending}}},
		{Type: WriteTask, Value: &TaskRecord{ID: self, ConversationID: RootConversationID, Kind: kindToolTask, Version: 1, OwnerID: &g, State: TaskState{Status: TaskPending}}},
	})

	h, err := Open(ctx, st, HarnessOptions{LLM: &fakeLLM{}})
	if err != nil {
		t.Fatal(err)
	}

	rt := &TaskRuntime{session: h.session, harness: h, task: &TaskRecord{ID: self}, ctx: ctx}
	rt.markTree(g)

	marked := map[ID]bool{}
	for _, w := range rt.writes {
		if w.Type == WriteTask {
			marked[w.Value.(*TaskRecord).ID] = w.Value.(*TaskRecord).AbortRequested
		}
	}
	if !marked[g] || !marked[a] {
		t.Fatalf("foreground tasks not marked: %v", marked)
	}
	if marked[b] || marked[c] || marked[self] {
		t.Fatalf("background subtree or current task was marked: %v", marked)
	}
}

// countingDef is an extension-kind task definition that records how often its
// phase ran.
type countingDef struct {
	steps int
}

func (d *countingDef) Kind() string { return "test.counting" }
func (d *countingDef) Version() int { return 1 }
func (d *countingDef) Initial(input json.RawMessage) json.RawMessage {
	return mustJSON(map[string]string{"phase": "run"})
}
func (d *countingDef) Step(ctx context.Context, checkpoint json.RawMessage, rt *TaskRuntime) (NextState, error) {
	d.steps++
	return NextState{Status: TaskTerminal, Outcome: &Outcome{Status: OutcomeCompleted}}, nil
}

func TestGenerationRestartLimitSettlesUnanswered(t *testing.T) {
	ctx := context.Background()
	st := NewMemoryStorage()
	write(t, st, ctx, []StorageWrite{
		{Type: WriteConversation, Value: &ConversationRecord{ID: RootConversationID}},
	})
	subID := mint(t, st, ctx)
	genID := mint(t, st, ctx)
	genInput := mustJSON(generationInput{SubmissionID: subID})
	write(t, st, ctx, []StorageWrite{
		{Type: WriteSubmission, Value: &SubmissionRecord{ID: subID, ConversationID: RootConversationID, Type: SubmissionInput, Status: SubmissionPlaced}},
		{Type: WriteTask, Value: &TaskRecord{ID: genID, ConversationID: RootConversationID, Kind: kindGeneration, Version: 1,
			Input:    genInput,
			Restarts: 4, // Reconcile bumps this to the default limit of 5
			State:    TaskState{Status: TaskRunning, Checkpoint: mustJSON(generationCheckpoint{Phase: "model"})},
		}},
	})

	llm := &fakeLLM{} // must not be called: the generation ends at the limit
	h, err := Open(ctx, st, HarnessOptions{LLM: llm})
	if err != nil {
		t.Fatal(err)
	}
	if err := h.Run(ctx); err != nil {
		t.Fatal(err)
	}

	if llm.calls != 0 {
		t.Fatalf("llm called %d times, want 0 (cut off at the restart limit)", llm.calls)
	}
	sub, err := st.Submission(ctx, subID)
	if err != nil {
		t.Fatal(err)
	}
	if sub.Status != SubmissionUnanswered {
		t.Fatalf("submission status = %s, want unanswered", sub.Status)
	}
	if sub.Reason != "The generation was interrupted 5 times in a row in its model phase" {
		t.Fatalf("submission reason = %q", sub.Reason)
	}
	gen, err := st.Task(ctx, genID)
	if err != nil {
		t.Fatal(err)
	}
	if gen.State.Status != TaskTerminal || gen.State.Outcome == nil || gen.State.Outcome.Status != OutcomeFailed {
		t.Fatalf("generation state = %+v, want terminal failed", gen.State)
	}
}

func TestExtensionTaskRestartLimitFaults(t *testing.T) {
	ctx := context.Background()
	st := NewMemoryStorage()
	write(t, st, ctx, []StorageWrite{
		{Type: WriteConversation, Value: &ConversationRecord{ID: RootConversationID}},
	})
	taskID := mint(t, st, ctx)
	raw := mustJSON(map[string]string{"phase": "run"})
	write(t, st, ctx, []StorageWrite{
		{Type: WriteTask, Value: &TaskRecord{ID: taskID, ConversationID: RootConversationID, Kind: "test.counting", Version: 1,
			Input:    raw,
			Restarts: 5,
			State:    TaskState{Status: TaskPending, Checkpoint: raw},
		}},
	})

	def := &countingDef{}
	h, err := Open(ctx, st, HarnessOptions{LLM: &fakeLLM{}, Definitions: []TaskDefinition{def}})
	if err != nil {
		t.Fatal(err)
	}
	if err := h.Run(ctx); err != nil {
		t.Fatal(err)
	}

	if def.steps != 0 {
		t.Fatalf("extension task stepped %d times, want 0 (faulted at the limit before running)", def.steps)
	}
	task, err := st.Task(ctx, taskID)
	if err != nil {
		t.Fatal(err)
	}
	if task.State.Status != TaskTerminal || task.State.Outcome == nil || task.State.Outcome.Status != OutcomeFaulted {
		t.Fatalf("task state = %+v, want terminal faulted", task.State)
	}
}

func TestProgressResetsRestartCount(t *testing.T) {
	ctx := context.Background()
	st := NewMemoryStorage()
	write(t, st, ctx, []StorageWrite{
		{Type: WriteConversation, Value: &ConversationRecord{ID: RootConversationID}},
	})
	genID := mint(t, st, ctx)
	genInput := mustJSON(generationInput{SubmissionID: 0})
	write(t, st, ctx, []StorageWrite{
		{Type: WriteTask, Value: &TaskRecord{ID: genID, ConversationID: RootConversationID, Kind: kindGeneration, Version: 1,
			Input:    genInput,
			Restarts: 1, // Reconcile bumps this to 2, below the limit
			State:    TaskState{Status: TaskRunning, Checkpoint: mustJSON(generationCheckpoint{Phase: "model"})},
		}},
	})

	llm := &fakeLLM{steps: []llmStep{
		{resp: agent.Response{ToolCalls: []agent.ToolCall{{ID: "c1", Name: "search", Arguments: json.RawMessage(`{}`)}}}},
		{resp: agent.Response{Content: "done"}},
	}}
	tools := &fakeRegistry{results: map[string]string{"search": "hit"}}
	h, err := Open(ctx, st, HarnessOptions{LLM: llm, Tools: tools})
	if err != nil {
		t.Fatal(err)
	}
	if err := h.Run(ctx); err != nil {
		t.Fatal(err)
	}

	gen, err := st.Task(ctx, genID)
	if err != nil {
		t.Fatal(err)
	}
	if gen.State.Status != TaskTerminal {
		t.Fatalf("generation status = %s, want terminal", gen.State.Status)
	}
	if gen.Restarts != 0 {
		t.Fatalf("generation restarts = %d, want 0 (progress removes the crash count)", gen.Restarts)
	}
}

func TestCustomRestartLimit(t *testing.T) {
	ctx := context.Background()
	st := NewMemoryStorage()
	write(t, st, ctx, []StorageWrite{
		{Type: WriteConversation, Value: &ConversationRecord{ID: RootConversationID}},
	})
	taskID := mint(t, st, ctx)
	raw := mustJSON(toolInput{CallID: "call1", Name: "search", Arguments: json.RawMessage(`{}`), ReplaySafe: true})
	write(t, st, ctx, []StorageWrite{
		{Type: WriteTask, Value: &TaskRecord{ID: taskID, ConversationID: RootConversationID, Kind: kindToolTask, Version: 1,
			Input:    raw,
			Restarts: 2,
			State:    TaskState{Status: TaskPending, Checkpoint: toolTaskDef{}.Initial(raw)},
		}},
	})

	tools := &fakeRegistry{results: map[string]string{"search": "hit"}}
	h, err := Open(ctx, st, HarnessOptions{LLM: &fakeLLM{}, Tools: tools, RestartLimit: 2})
	if err != nil {
		t.Fatal(err)
	}
	if err := h.Run(ctx); err != nil {
		t.Fatal(err)
	}

	if len(tools.calls) != 0 {
		t.Fatalf("tool calls = %v, want none (custom limit 2, cut off at 2 restarts)", tools.calls)
	}
	entries, err := h.Root().Entries(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if got := toolEntryContent(t, entries); got != "Tool search was interrupted 2 times in a row; it is not run again" {
		t.Fatalf("tool entry content = %q", got)
	}
}
