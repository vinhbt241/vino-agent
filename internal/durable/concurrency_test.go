package durable

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"testing"
	"time"

	"vino/internal/agent"
)

// fgBgRegistry blocks in its "fg" tool until the context is cancelled, and
// completes its "bg" tool without looking at the context.
type fgBgRegistry struct {
	fgStarted chan struct{}
	fgDone    chan struct{}

	mu    sync.Mutex
	calls []string
}

func (r *fgBgRegistry) Specs() []agent.ToolSpec {
	return []agent.ToolSpec{{Name: "fg", Replay: agent.ReplaySafe}, {Name: "bg", Replay: agent.ReplaySafe}}
}

func (r *fgBgRegistry) Call(ctx context.Context, name string, _ json.RawMessage) (string, error) {
	r.mu.Lock()
	r.calls = append(r.calls, name)
	r.mu.Unlock()
	if name == "fg" {
		close(r.fgStarted)
		<-ctx.Done()
		close(r.fgDone)
		return "", ctx.Err()
	}
	return "bg-result", nil
}

// twoBlockRegistry blocks in both its tools until release is closed, signalling
// each call's tool name on started.
type twoBlockRegistry struct {
	started chan string
	release chan struct{}
}

func (r *twoBlockRegistry) Specs() []agent.ToolSpec {
	return []agent.ToolSpec{{Name: "a", Replay: agent.ReplaySafe}, {Name: "b", Replay: agent.ReplaySafe}}
}

func (r *twoBlockRegistry) Call(_ context.Context, name string, _ json.RawMessage) (string, error) {
	r.started <- name
	<-r.release
	return name + "-result", nil
}

func TestConversationsRunConcurrently(t *testing.T) {
	ctx := context.Background()
	llmA := &fakeLLM{steps: []llmStep{
		{resp: agent.Response{ToolCalls: []agent.ToolCall{{ID: "ca", Name: "a", Arguments: json.RawMessage(`{}`)}}}},
		{resp: agent.Response{Content: "A done"}},
	}}
	llmB := &fakeLLM{steps: []llmStep{
		{resp: agent.Response{ToolCalls: []agent.ToolCall{{ID: "cb", Name: "b", Arguments: json.RawMessage(`{}`)}}}},
		{resp: agent.Response{Content: "B done"}},
	}}
	reg := &twoBlockRegistry{started: make(chan string, 2), release: make(chan struct{})}
	h, err := Open(ctx, NewMemoryStorage(), HarnessOptions{
		LLM:    &fakeLLM{},
		Models: map[string]agent.LLM{"A": llmA, "B": llmB},
		Tools:  reg,
	})
	if err != nil {
		t.Fatal(err)
	}

	start := func(model, content string) (*Submission, error) {
		sub, err := h.Start(ctx, StartInput{Messages: []agent.Message{{Role: agent.RoleUser, Content: content}}}, nil)
		if err != nil {
			return nil, err
		}
		conv, err := h.Conversation(ctx, sub.ConversationID())
		if err != nil {
			return nil, err
		}
		if err := conv.Configure(ctx, AgentConfig{Model: model}); err != nil {
			return nil, err
		}
		return sub, nil
	}

	subA, err := start("A", "a")
	if err != nil {
		t.Fatal(err)
	}
	subB, err := start("B", "b")
	if err != nil {
		t.Fatal(err)
	}

	wait := func(sub *Submission) chan error {
		ch := make(chan error, 1)
		go func() {
			rec, err := sub.Wait(ctx)
			if err != nil {
				ch <- err
				return
			}
			if rec.Status != SubmissionDone {
				ch <- fmt.Errorf("status = %s, want done", rec.Status)
				return
			}
			ch <- nil
		}()
		return ch
	}

	doneA := wait(subA)
	doneB := wait(subB)

	// Both tools must start before either is released: the two conversations
	// run concurrently rather than serializing behind one another.
	seen := map[string]bool{}
	for i := 0; i < 2; i++ {
		select {
		case name := <-reg.started:
			seen[name] = true
		case <-time.After(5 * time.Second):
			t.Fatalf("only %v started before release; conversations are serializing", seen)
		}
	}
	if !seen["a"] || !seen["b"] {
		t.Fatalf("started = %v, want both a and b", seen)
	}

	close(reg.release)

	for i, ch := range []chan error{doneA, doneB} {
		select {
		case err := <-ch:
			if err != nil {
				t.Fatalf("run %d: %v", i+1, err)
			}
		case <-time.After(5 * time.Second):
			t.Fatalf("run %d timed out", i+1)
		}
	}
}

func TestBackgroundToolSurvivesForegroundCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	st := NewMemoryStorage()
	write(t, st, ctx, []StorageWrite{
		{Type: WriteConversation, Value: &ConversationRecord{ID: RootConversationID}},
	})
	fgID := mint(t, st, ctx)
	bgID := mint(t, st, ctx)
	fgInput := mustJSON(toolInput{CallID: "c-fg", Name: "fg", Arguments: json.RawMessage(`{}`), ReplaySafe: true})
	bgInput := mustJSON(toolInput{CallID: "c-bg", Name: "bg", Arguments: json.RawMessage(`{}`), ReplaySafe: true})
	write(t, st, ctx, []StorageWrite{
		{Type: WriteTask, Value: &TaskRecord{ID: fgID, ConversationID: RootConversationID, Kind: kindToolTask, Version: 1,
			Input: fgInput, State: TaskState{Status: TaskPending, Checkpoint: toolTaskDef{}.Initial(fgInput)}}},
		{Type: WriteTask, Value: &TaskRecord{ID: bgID, ConversationID: RootConversationID, Kind: kindToolTask, Version: 1,
			Input: bgInput, Background: true, State: TaskState{Status: TaskPending, Checkpoint: toolTaskDef{}.Initial(bgInput)}}},
	})

	reg := &fgBgRegistry{fgStarted: make(chan struct{}), fgDone: make(chan struct{})}
	h, err := Open(ctx, st, HarnessOptions{LLM: &fakeLLM{}, Tools: reg})
	if err != nil {
		t.Fatal(err)
	}

	runDone := make(chan error, 1)
	go func() { runDone <- h.Run(ctx) }()

	select {
	case <-reg.fgStarted:
	case <-time.After(5 * time.Second):
		t.Fatal("foreground tool never started")
	}

	cancel()

	select {
	case <-reg.fgDone:
	case <-time.After(5 * time.Second):
		t.Fatal("foreground tool never observed cancellation")
	}

	select {
	case err := <-runDone:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("run never returned")
	}

	reg.mu.Lock()
	calls := append([]string(nil), reg.calls...)
	reg.mu.Unlock()
	foundBg := false
	for _, name := range calls {
		if name == "bg" {
			foundBg = true
		}
	}
	if !foundBg {
		t.Fatalf("background tool never ran: calls = %v", calls)
	}

	readCtx := context.Background()
	bg, err := st.Task(readCtx, bgID)
	if err != nil {
		t.Fatal(err)
	}
	if bg.State.Status != TaskTerminal || bg.State.Outcome == nil || bg.State.Outcome.Status != OutcomeCompleted {
		t.Fatalf("background task state = %+v, want terminal completed (detached ctx, not killed by cancel)", bg.State)
	}
	fg, err := st.Task(readCtx, fgID)
	if err != nil {
		t.Fatal(err)
	}
	if fg.State.Status != TaskTerminal {
		t.Fatalf("foreground task status = %s, want terminal", fg.State.Status)
	}

	// The foreground tool records the cancel; the background tool keeps its
	// real result, proving it ran under a detached ctx.
	entries, err := h.Root().Entries(readCtx)
	if err != nil {
		t.Fatal(err)
	}
	var fgContent, bgContent string
	for _, e := range entries {
		if e.Kind != kindTool || len(e.Model) == 0 {
			continue
		}
		switch e.Model[0].Name {
		case "fg":
			fgContent = e.Model[0].Content
		case "bg":
			bgContent = e.Model[0].Content
		}
	}
	if fgContent != abortedToolResult {
		t.Fatalf("foreground tool result = %q, want %q", fgContent, abortedToolResult)
	}
	if bgContent != "bg-result" {
		t.Fatalf("background tool result = %q, want %q", bgContent, "bg-result")
	}
}

func TestConversationAbortSettlesAndCancels(t *testing.T) {
	ctx := context.Background()
	started := make(chan struct{})
	released := make(chan struct{})
	reg := &cancelAwareRegistry{started: started, released: released}
	llm := &fakeLLM{steps: []llmStep{
		{resp: agent.Response{ToolCalls: []agent.ToolCall{{ID: "c1", Name: "block", Arguments: json.RawMessage(`{}`)}}}},
	}}
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
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("tool never started")
	}

	conv, err := h.Conversation(ctx, sub.ConversationID())
	if err != nil {
		t.Fatal(err)
	}
	if err := conv.Abort(ctx); err != nil {
		t.Fatal(err)
	}

	select {
	case <-released:
	case <-time.After(5 * time.Second):
		t.Fatal("tool never observed the abort cancellation")
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
	if rec.Reason != "aborted" {
		t.Fatalf("reason = %q, want %q", rec.Reason, "aborted")
	}
}

func TestAbortWithNoInFlightRunSettles(t *testing.T) {
	ctx := context.Background()
	llm := &fakeLLM{} // must not be called: the run is aborted before it starts
	h, err := Open(ctx, NewMemoryStorage(), HarnessOptions{LLM: llm})
	if err != nil {
		t.Fatal(err)
	}

	sub, err := h.Root().Submit(ctx, SubmitInput{Content: "go", RequestID: "r1"})
	if err != nil {
		t.Fatal(err)
	}

	conv, err := h.Conversation(ctx, sub.ConversationID())
	if err != nil {
		t.Fatal(err)
	}
	if err := conv.Abort(ctx); err != nil {
		t.Fatal(err)
	}

	rec, err := sub.Wait(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if rec.Status != SubmissionUnanswered {
		t.Fatalf("status = %s, want unanswered", rec.Status)
	}
	if rec.Reason != "aborted" {
		t.Fatalf("reason = %q, want %q", rec.Reason, "aborted")
	}
	if llm.calls != 0 {
		t.Fatalf("llm called %d times, want 0 (aborted before running)", llm.calls)
	}
}

// cancelIsolationRegistry runs tool "a" until its ctx is cancelled, and tool
// "b" until either its ctx is cancelled or releaseB is closed.
type cancelIsolationRegistry struct {
	startedA  chan struct{}
	releasedA chan struct{}
	startedB  chan struct{}
	releasedB chan struct{}
	releaseB  chan struct{}
}

func (r *cancelIsolationRegistry) Specs() []agent.ToolSpec {
	return []agent.ToolSpec{{Name: "a", Replay: agent.ReplaySafe}, {Name: "b", Replay: agent.ReplaySafe}}
}

func (r *cancelIsolationRegistry) Call(ctx context.Context, name string, _ json.RawMessage) (string, error) {
	if name == "a" {
		close(r.startedA)
		<-ctx.Done()
		close(r.releasedA)
		return "", ctx.Err()
	}
	close(r.startedB)
	select {
	case <-ctx.Done():
		close(r.releasedB)
		return "", ctx.Err()
	case <-r.releaseB:
		return "b-result", nil
	}
}

func TestCancelOneConversationDoesNotAffectAnother(t *testing.T) {
	ctxA, cancelA := context.WithCancel(context.Background())
	ctxB := context.Background()
	reg := &cancelIsolationRegistry{
		startedA:  make(chan struct{}),
		releasedA: make(chan struct{}),
		startedB:  make(chan struct{}),
		releasedB: make(chan struct{}),
		releaseB:  make(chan struct{}),
	}
	llmA := &fakeLLM{steps: []llmStep{
		{resp: agent.Response{ToolCalls: []agent.ToolCall{{ID: "ca", Name: "a", Arguments: json.RawMessage(`{}`)}}}},
		{resp: agent.Response{Content: "A done"}},
	}}
	llmB := &fakeLLM{steps: []llmStep{
		{resp: agent.Response{ToolCalls: []agent.ToolCall{{ID: "cb", Name: "b", Arguments: json.RawMessage(`{}`)}}}},
		{resp: agent.Response{Content: "B done"}},
	}}
	h, err := Open(context.Background(), NewMemoryStorage(), HarnessOptions{
		LLM:    &fakeLLM{},
		Models: map[string]agent.LLM{"A": llmA, "B": llmB},
		Tools:  reg,
	})
	if err != nil {
		t.Fatal(err)
	}

	start := func(model, content string) (*Submission, error) {
		sub, err := h.Start(context.Background(), StartInput{Messages: []agent.Message{{Role: agent.RoleUser, Content: content}}}, nil)
		if err != nil {
			return nil, err
		}
		conv, err := h.Conversation(context.Background(), sub.ConversationID())
		if err != nil {
			return nil, err
		}
		if err := conv.Configure(context.Background(), AgentConfig{Model: model}); err != nil {
			return nil, err
		}
		return sub, nil
	}

	subA, err := start("A", "a")
	if err != nil {
		t.Fatal(err)
	}
	subB, err := start("B", "b")
	if err != nil {
		t.Fatal(err)
	}

	type waitResult struct {
		rec *SubmissionRecord
		err error
	}
	wait := func(sub *Submission, runCtx context.Context) chan waitResult {
		ch := make(chan waitResult, 1)
		go func() {
			rec, err := sub.Wait(runCtx)
			ch <- waitResult{rec, err}
		}()
		return ch
	}

	doneA := wait(subA, ctxA)
	doneB := wait(subB, ctxB)

	select {
	case <-reg.startedA:
	case <-time.After(5 * time.Second):
		t.Fatal("conversation A tool never started")
	}
	select {
	case <-reg.startedB:
	case <-time.After(5 * time.Second):
		t.Fatal("conversation B tool never started (runs are serializing)")
	}

	cancelA()

	select {
	case <-reg.releasedA:
	case <-time.After(5 * time.Second):
		t.Fatal("conversation A tool never observed its own cancellation")
	}

	var ra waitResult
	select {
	case ra = <-doneA:
	case <-time.After(5 * time.Second):
		t.Fatal("conversation A Wait never returned")
	}
	if ra.err != nil {
		t.Fatalf("conversation A Wait: %v", ra.err)
	}
	if ra.rec.Status != SubmissionUnanswered {
		t.Fatalf("conversation A status = %s, want unanswered", ra.rec.Status)
	}

	// B must be untouched by A's cancellation: its tool is still running.
	select {
	case <-reg.releasedB:
		t.Fatal("conversation B observed A's cancellation")
	case <-time.After(50 * time.Millisecond):
	}

	close(reg.releaseB)

	var rb waitResult
	select {
	case rb = <-doneB:
	case <-time.After(5 * time.Second):
		t.Fatal("conversation B Wait never returned")
	}
	if rb.err != nil {
		t.Fatalf("conversation B Wait: %v", rb.err)
	}
	if rb.rec.Status != SubmissionDone {
		t.Fatalf("conversation B status = %s, want done (A's cancel must not reach B)", rb.rec.Status)
	}
}
