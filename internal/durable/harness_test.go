package durable

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	"vino/internal/agent"
)

// ---- fakes ----

type llmStep struct {
	deltas []string
	resp   agent.Response
	err    error
}

type fakeLLM struct {
	steps    []llmStep
	calls    int
	messages [][]agent.Message
}

func (f *fakeLLM) Chat(_ context.Context, msgs []agent.Message, _ []agent.ToolSpec, onDelta func(string) error) (agent.Response, error) {
	f.calls++
	f.messages = append(f.messages, append([]agent.Message(nil), msgs...))
	if len(f.steps) == 0 {
		return agent.Response{}, fmt.Errorf("fakeLLM: no more scripted steps")
	}
	s := f.steps[0]
	f.steps = f.steps[1:]
	if s.err != nil {
		return agent.Response{}, s.err
	}
	for _, d := range s.deltas {
		if err := onDelta(d); err != nil {
			return agent.Response{}, err
		}
	}
	return s.resp, nil
}

type fakeRegistry struct {
	specs   []agent.ToolSpec
	results map[string]string
	calls   []string
}

// blockingLLM signals when its Chat is invoked and blocks until the context
// is cancelled, mirroring an in-flight model request during a cancel.
type blockingLLM struct {
	started chan struct{}
}

func (b *blockingLLM) Chat(ctx context.Context, _ []agent.Message, _ []agent.ToolSpec, _ func(string) error) (agent.Response, error) {
	close(b.started)
	<-ctx.Done()
	return agent.Response{}, ctx.Err()
}

// streamingBlockingLLM streams scripted deltas then blocks until the context
// is cancelled, mirroring a real streaming model cut off mid-answer.
type streamingBlockingLLM struct {
	deltas     []string
	started    chan struct{}
	deltasSent chan struct{}
}

func (s *streamingBlockingLLM) Chat(ctx context.Context, _ []agent.Message, _ []agent.ToolSpec, onDelta func(string) error) (agent.Response, error) {
	close(s.started)
	for _, d := range s.deltas {
		if err := onDelta(d); err != nil {
			return agent.Response{}, err
		}
	}
	close(s.deltasSent)
	<-ctx.Done()
	return agent.Response{}, ctx.Err()
}

func (f *fakeRegistry) Specs() []agent.ToolSpec { return f.specs }
func (f *fakeRegistry) Call(_ context.Context, name string, _ json.RawMessage) (string, error) {
	f.calls = append(f.calls, name)
	return f.results[name], nil
}

// blockingRegistry signals when its tool starts and blocks until released,
// used to hold a run in flight while another submission is admitted.
type blockingRegistry struct {
	started chan struct{}
	release chan struct{}
}

func (b *blockingRegistry) Specs() []agent.ToolSpec {
	return []agent.ToolSpec{{Name: "block", Replay: agent.ReplaySafe}}
}

func (b *blockingRegistry) Call(_ context.Context, name string, _ json.RawMessage) (string, error) {
	b.started <- struct{}{}
	<-b.release
	return "released", nil
}

// cancelAwareRegistry blocks in its tool until the context is cancelled, then
// reports the cancellation — like a real tool (e.g. bash sleep) killed by a
// request cancellation.
type cancelAwareRegistry struct {
	started  chan struct{}
	released chan struct{}
}

func (c *cancelAwareRegistry) Specs() []agent.ToolSpec {
	return []agent.ToolSpec{{Name: "block", Replay: agent.ReplaySafe}}
}

func (c *cancelAwareRegistry) Call(ctx context.Context, name string, _ json.RawMessage) (string, error) {
	close(c.started)
	<-ctx.Done()
	close(c.released)
	return "", ctx.Err()
}

// ---- helpers ----

func write(t *testing.T, st Storage, ctx context.Context, writes []StorageWrite) {
	t.Helper()
	if _, err := st.Commit(ctx, writes); err != nil {
		t.Fatal(err)
	}
}

func mint(t *testing.T, st Storage, ctx context.Context) ID {
	t.Helper()
	id, err := st.MintID(ctx)
	if err != nil {
		t.Fatal(err)
	}
	return id
}

func toolEntryContent(t *testing.T, entries []EntryRecord) string {
	t.Helper()
	for _, e := range entries {
		if e.Kind == kindTool && len(e.Model) > 0 {
			return e.Model[0].Content
		}
	}
	t.Fatal("no tool entry found")
	return ""
}

// seedCrashedTool writes a transcript that crashed mid-tool-call: a running
// tool task with a generation task waiting on it.
func seedCrashedTool(t *testing.T, st Storage, replaySafe bool) {
	t.Helper()
	ctx := context.Background()
	write(t, st, ctx, []StorageWrite{
		{Type: WriteConversation, Value: &ConversationRecord{ID: RootConversationID}},
	})
	userID := mint(t, st, ctx)
	assistantID := mint(t, st, ctx)
	genID := mint(t, st, ctx)
	toolID := mint(t, st, ctx)

	toolInputRaw := mustJSON(toolInput{CallID: "call1", Name: "search", Arguments: json.RawMessage(`{}`), ReplaySafe: replaySafe})
	write(t, st, ctx, []StorageWrite{
		{Type: WriteEntry, Value: &EntryRecord{ID: userID, ConversationID: RootConversationID, Kind: kindUser,
			Model: []agent.Message{{Role: agent.RoleUser, Content: "find x"}}}},
		{Type: WriteEntry, Value: &EntryRecord{ID: assistantID, ConversationID: RootConversationID, Kind: kindAssistant,
			Model: []agent.Message{{Role: agent.RoleAssistant, ToolCalls: []agent.ToolCall{{ID: "call1", Name: "search", Arguments: json.RawMessage(`{}`)}}}}}},
		{Type: WriteTask, Value: &TaskRecord{ID: genID, ConversationID: RootConversationID, Kind: kindGeneration, Version: 1,
			Input: mustJSON(generationInput{SubmissionID: 0}),
			State: TaskState{Status: TaskWaiting, Checkpoint: mustJSON(generationCheckpoint{Phase: "tools"}), On: []ID{toolID}}}},
		{Type: WriteTask, Value: &TaskRecord{ID: toolID, ConversationID: RootConversationID, Kind: kindToolTask, Version: 1,
			Input: toolInputRaw,
			State: TaskState{Status: TaskRunning, Checkpoint: toolTaskDef{}.Initial(toolInputRaw)}}},
	})
}

// ---- tests ----

func TestSubmitRunsToAnswer(t *testing.T) {
	ctx := context.Background()
	llm := &fakeLLM{steps: []llmStep{{resp: agent.Response{Content: "hello there"}}}}
	h, err := Open(ctx, NewMemoryStorage(), HarnessOptions{LLM: llm, SystemPrompt: "sys"})
	if err != nil {
		t.Fatal(err)
	}
	sub, err := h.Root().Submit(ctx, SubmitInput{Content: "hi", RequestID: "r1"})
	if err != nil {
		t.Fatal(err)
	}
	rec, err := sub.Wait(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if rec.Status != SubmissionDone {
		t.Fatalf("submission status = %s, want done", rec.Status)
	}
	if rec.AnswerID == nil {
		t.Fatal("submission has no answer entry")
	}
	entries, err := h.Root().Entries(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 2 || entries[0].Kind != kindUser || entries[1].Kind != kindAssistant {
		t.Fatalf("entries = %+v, want [user assistant]", entries)
	}
	if entries[1].Model[0].Content != "hello there" {
		t.Fatalf("answer content = %q", entries[1].Model[0].Content)
	}
}

func TestSubmitWithToolCall(t *testing.T) {
	ctx := context.Background()
	llm := &fakeLLM{steps: []llmStep{
		{resp: agent.Response{ToolCalls: []agent.ToolCall{{ID: "call1", Name: "search", Arguments: json.RawMessage(`{"q":"x"}`)}}}},
		{resp: agent.Response{Content: "found it"}},
	}}
	tools := &fakeRegistry{
		specs:   []agent.ToolSpec{{Name: "search", Replay: agent.ReplaySafe}},
		results: map[string]string{"search": "hit"},
	}
	h, err := Open(ctx, NewMemoryStorage(), HarnessOptions{LLM: llm, Tools: tools})
	if err != nil {
		t.Fatal(err)
	}
	sub, err := h.Root().Submit(ctx, SubmitInput{Content: "find x", RequestID: "r1"})
	if err != nil {
		t.Fatal(err)
	}
	rec, err := sub.Wait(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if rec.Status != SubmissionDone {
		t.Fatalf("submission status = %s", rec.Status)
	}
	if len(tools.calls) != 1 || tools.calls[0] != "search" {
		t.Fatalf("tool calls = %v", tools.calls)
	}
	entries, _ := h.Root().Entries(ctx)
	if len(entries) != 4 {
		t.Fatalf("entries = %d, want 4 (user, assistant, tool, assistant)", len(entries))
	}
	if entries[2].Kind != kindTool || entries[2].Model[0].Content != "hit" {
		t.Fatalf("tool entry = %+v", entries[2])
	}
	if entries[3].Model[0].Content != "found it" {
		t.Fatalf("final answer = %q", entries[3].Model[0].Content)
	}
}

func TestExactlyOnceRequestID(t *testing.T) {
	ctx := context.Background()
	llm := &fakeLLM{steps: []llmStep{
		{resp: agent.Response{Content: "first"}},
		{resp: agent.Response{Content: "second"}},
	}}
	h, err := Open(ctx, NewMemoryStorage(), HarnessOptions{LLM: llm})
	if err != nil {
		t.Fatal(err)
	}
	root := h.Root()
	s1, err := root.Submit(ctx, SubmitInput{Content: "a", RequestID: "job-1"})
	if err != nil {
		t.Fatal(err)
	}
	s2, err := root.Submit(ctx, SubmitInput{Content: "b", RequestID: "job-1"})
	if err != nil {
		t.Fatal(err)
	}
	if s1.ID() != s2.ID() {
		t.Fatalf("requestId dedup failed: %d != %d", s1.ID(), s2.ID())
	}
	rec, err := s1.Wait(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if rec.Status != SubmissionDone {
		t.Fatalf("status = %s", rec.Status)
	}
	entries, _ := root.Entries(ctx)
	if len(entries) != 2 {
		t.Fatalf("entries = %d, want 2 (dedup must not duplicate the user turn)", len(entries))
	}
	if llm.calls != 1 {
		t.Fatalf("llm called %d times, want 1 (dedup must not start a second run)", llm.calls)
	}
}

func TestRecoverySafeToolReruns(t *testing.T) {
	ctx := context.Background()
	st := NewMemoryStorage()
	seedCrashedTool(t, st, true)

	llm := &fakeLLM{steps: []llmStep{{resp: agent.Response{Content: "done"}}}}
	tools := &fakeRegistry{results: map[string]string{"search": "hit"}}

	h, err := Open(ctx, st, HarnessOptions{LLM: llm, Tools: tools})
	if err != nil {
		t.Fatal(err)
	}
	// Reconcile already flipped the running tool task to pending with a
	// restart counted.
	if err := h.Run(ctx); err != nil {
		t.Fatal(err)
	}
	if len(tools.calls) != 1 || tools.calls[0] != "search" {
		t.Fatalf("tool calls = %v, want [search] (safe tool re-runs)", tools.calls)
	}
	entries, _ := h.Root().Entries(ctx)
	if got := toolEntryContent(t, entries); got != "hit" {
		t.Fatalf("tool entry content = %q, want hit", got)
	}
}

func TestRecoveryUnsafeToolReportsInterrupted(t *testing.T) {
	ctx := context.Background()
	st := NewMemoryStorage()
	seedCrashedTool(t, st, false)

	llm := &fakeLLM{steps: []llmStep{{resp: agent.Response{Content: "done"}}}}
	tools := &fakeRegistry{results: map[string]string{"search": "hit"}}

	h, err := Open(ctx, st, HarnessOptions{LLM: llm, Tools: tools})
	if err != nil {
		t.Fatal(err)
	}
	if err := h.Run(ctx); err != nil {
		t.Fatal(err)
	}
	if len(tools.calls) != 0 {
		t.Fatalf("tool calls = %v, want none (unsafe tool must not re-run)", tools.calls)
	}
	entries, _ := h.Root().Entries(ctx)
	if got := toolEntryContent(t, entries); !strings.Contains(got, "not safe to re-run") {
		t.Fatalf("tool entry content = %q, want interruption notice", got)
	}
}

func TestReconcileMarksRunningPending(t *testing.T) {
	ctx := context.Background()
	st := NewMemoryStorage()
	seedCrashedTool(t, st, true)

	_, err := Open(ctx, st, HarnessOptions{LLM: &fakeLLM{}})
	if err != nil {
		t.Fatal(err)
	}
	// The running tool task must now be pending with one restart.
	page, err := st.ScanTasks(ctx, TaskQuery{Kind: kindToolTask}, 0, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(page.Items) != 1 {
		t.Fatalf("tool tasks = %d", len(page.Items))
	}
	task := page.Items[0]
	if task.State.Status != TaskPending {
		t.Fatalf("status = %s, want pending", task.State.Status)
	}
	if task.Restarts != 1 {
		t.Fatalf("restarts = %d, want 1", task.Restarts)
	}
}

func TestStartStreamsEvents(t *testing.T) {
	ctx := context.Background()
	llm := &fakeLLM{steps: []llmStep{
		{deltas: []string{"go"}, resp: agent.Response{ToolCalls: []agent.ToolCall{{ID: "c1", Name: "search", Arguments: json.RawMessage(`{}`)}}}},
		{resp: agent.Response{Content: "done"}},
	}}
	tools := &fakeRegistry{
		specs:   []agent.ToolSpec{{Name: "search", Replay: agent.ReplaySafe}},
		results: map[string]string{"search": "hit"},
	}
	h, err := Open(ctx, NewMemoryStorage(), HarnessOptions{LLM: llm, Tools: tools})
	if err != nil {
		t.Fatal(err)
	}

	var events []agent.Event
	sub, err := h.Start(ctx, StartInput{
		RequestID:    "r1",
		SystemPrompt: "sys",
		Messages:     []agent.Message{{Role: agent.RoleUser, Content: "find"}},
	}, func(ev agent.Event) error {
		events = append(events, ev)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	rec, err := sub.Wait(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if rec.Status != SubmissionDone {
		t.Fatalf("status = %s, want done", rec.Status)
	}

	want := []agent.EventType{agent.EventTextDelta, agent.EventToolCall, agent.EventToolResult, agent.EventRunFinished}
	if len(events) != len(want) {
		t.Fatalf("events = %d, want %d: %#v", len(events), len(want), events)
	}
	for i, w := range want {
		if events[i].Type != w {
			t.Fatalf("events[%d].Type = %s, want %s", i, events[i].Type, w)
		}
	}
}

func TestStartExactlyOnceAcrossConversations(t *testing.T) {
	ctx := context.Background()
	llm := &fakeLLM{steps: []llmStep{{resp: agent.Response{Content: "a"}}, {resp: agent.Response{Content: "b"}}}}
	h, err := Open(ctx, NewMemoryStorage(), HarnessOptions{LLM: llm})
	if err != nil {
		t.Fatal(err)
	}
	s1, err := h.Start(ctx, StartInput{RequestID: "job", Messages: []agent.Message{{Role: agent.RoleUser, Content: "one"}}}, nil)
	if err != nil {
		t.Fatal(err)
	}
	// A second request with the same request ID must resolve to the first run.
	s2, err := h.Start(ctx, StartInput{RequestID: "job", Messages: []agent.Message{{Role: agent.RoleUser, Content: "two"}}}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if s1.ID() != s2.ID() {
		t.Fatalf("requestId dedup failed across conversations: %d != %d", s1.ID(), s2.ID())
	}
	rec, err := s1.Wait(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if rec.Status != SubmissionDone {
		t.Fatalf("status = %s", rec.Status)
	}
	if llm.calls != 1 {
		t.Fatalf("llm calls = %d, want 1", llm.calls)
	}
}

func TestSinkDisconnectDoesNotAbortRun(t *testing.T) {
	ctx := context.Background()
	llm := &fakeLLM{steps: []llmStep{
		{resp: agent.Response{ToolCalls: []agent.ToolCall{{ID: "c1", Name: "search", Arguments: json.RawMessage(`{}`)}}}},
		{resp: agent.Response{Content: "done"}},
	}}
	tools := &fakeRegistry{
		specs:   []agent.ToolSpec{{Name: "search", Replay: agent.ReplaySafe}},
		results: map[string]string{"search": "hit"},
	}
	h, err := Open(ctx, NewMemoryStorage(), HarnessOptions{LLM: llm, Tools: tools})
	if err != nil {
		t.Fatal(err)
	}

	// Simulate the client disconnecting right as the tool result is emitted.
	// The run must keep going and persist a consistent transcript.
	sink := func(ev agent.Event) error {
		if ev.Type == agent.EventToolResult {
			return errors.New("client disconnected")
		}
		return nil
	}
	sub, err := h.Root().SubmitStream(ctx, SubmitInput{Content: "find x", RequestID: "r1"}, sink)
	if err != nil {
		t.Fatal(err)
	}
	rec, err := sub.Wait(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if rec.Status != SubmissionDone {
		t.Fatalf("status = %s, want done (sink failure must not abort the run)", rec.Status)
	}

	entries, err := h.Root().Entries(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 4 {
		t.Fatalf("entries = %d, want 4 (user, assistant, tool, assistant)", len(entries))
	}
	if entries[2].Kind != kindTool || entries[2].Model[0].Content != "hit" {
		t.Fatalf("tool entry = %+v, want a committed tool result", entries[2])
	}
	if entries[3].Model[0].Content != "done" {
		t.Fatalf("final answer = %q, want done", entries[3].Model[0].Content)
	}
}

func TestConcurrentSubmitQueuesBehindInFlightRun(t *testing.T) {
	ctx := context.Background()
	started := make(chan struct{})
	release := make(chan struct{})
	tools := &blockingRegistry{started: started, release: release}
	llm := &fakeLLM{steps: []llmStep{
		{resp: agent.Response{ToolCalls: []agent.ToolCall{{ID: "c1", Name: "block", Arguments: json.RawMessage(`{}`)}}}},
		{resp: agent.Response{Content: "first done"}},
		{resp: agent.Response{Content: "second done"}},
	}}
	h, err := Open(ctx, NewMemoryStorage(), HarnessOptions{LLM: llm, Tools: tools})
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

	sub1, err := h.Root().Submit(ctx, SubmitInput{Content: "first", RequestID: "r1"})
	if err != nil {
		t.Fatal(err)
	}
	firstDone := wait(sub1)

	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("first run never reached its tool")
	}

	sub2, err := h.Root().Submit(ctx, SubmitInput{Content: "second", RequestID: "r2"})
	if err != nil {
		t.Fatal(err)
	}
	secondDone := wait(sub2)

	// The second run must not settle while the first is still executing.
	select {
	case <-secondDone:
		t.Fatal("second submission completed while the first run was still in flight")
	case <-time.After(50 * time.Millisecond):
	}

	close(release)

	for i, ch := range []chan error{firstDone, secondDone} {
		select {
		case err := <-ch:
			if err != nil {
				t.Fatalf("run %d: %v", i+1, err)
			}
		case <-time.After(5 * time.Second):
			t.Fatalf("run %d timed out", i+1)
		}
	}

	entries, err := h.Root().Entries(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 6 {
		t.Fatalf("entries = %d, want 6 (user, assistant, tool, assistant, user, assistant)", len(entries))
	}
}

func TestCancelAbortsToolAndSettlesRun(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	started := make(chan struct{})
	released := make(chan struct{})
	tools := &cancelAwareRegistry{started: started, released: released}
	llm := &fakeLLM{steps: []llmStep{
		{resp: agent.Response{ToolCalls: []agent.ToolCall{{ID: "c1", Name: "block", Arguments: json.RawMessage(`{}`)}}}},
		{resp: agent.Response{Content: "after cancel"}},
	}}
	h, err := Open(ctx, NewMemoryStorage(), HarnessOptions{LLM: llm, Tools: tools})
	if err != nil {
		t.Fatal(err)
	}

	sub, err := h.Root().Submit(ctx, SubmitInput{Content: "do it", RequestID: "r1"})
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

	cancel()

	select {
	case <-released:
	case <-time.After(5 * time.Second):
		t.Fatal("tool never observed cancellation")
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
		t.Fatalf("status = %s, want unanswered (cancelled)", rec.Status)
	}
	if rec.Reason != "run cancelled by client" {
		t.Fatalf("reason = %q, want %q", rec.Reason, "run cancelled by client")
	}

	ctx2 := context.Background()
	entries, err := h.Root().Entries(ctx2)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 3 {
		t.Fatalf("entries = %d, want 3 (user, assistant, tool)", len(entries))
	}
	if entries[2].Kind != kindTool || entries[2].Model[0].Content != abortedToolResult {
		t.Fatalf("tool entry = %+v, want cancelled tool result", entries[2])
	}

	// A subsequent prompt must build a valid context from the aborted run.
	sub2, err := h.Root().Submit(ctx2, SubmitInput{Content: "now what", RequestID: "r2"})
	if err != nil {
		t.Fatal(err)
	}
	rec2, err := sub2.Wait(ctx2)
	if err != nil {
		t.Fatal(err)
	}
	if rec2.Status != SubmissionDone {
		t.Fatalf("second status = %s, want done", rec2.Status)
	}
	if llm.calls != 2 {
		t.Fatalf("llm calls = %d, want 2", llm.calls)
	}
}

func TestCancelDuringModelCallAborts(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	llm := &blockingLLM{started: make(chan struct{})}
	h, err := Open(ctx, NewMemoryStorage(), HarnessOptions{LLM: llm})
	if err != nil {
		t.Fatal(err)
	}

	sub, err := h.Root().Submit(ctx, SubmitInput{Content: "hi", RequestID: "r1"})
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
	case <-llm.started:
	case <-time.After(5 * time.Second):
		t.Fatal("model call never started")
	}

	cancel()

	var rec *SubmissionRecord
	select {
	case rec = <-waitCh:
	case err := <-errCh:
		t.Fatalf("Wait: %v", err)
	case <-time.After(5 * time.Second):
		t.Fatal("Wait never returned")
	}

	if rec.Status != SubmissionUnanswered {
		t.Fatalf("status = %s, want unanswered (cancelled)", rec.Status)
	}

	// No assistant entry was committed, so the transcript stays valid and the
	// conversation can be resumed.
	entries, err := h.Root().Entries(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 || entries[0].Kind != kindUser {
		t.Fatalf("entries = %+v, want a single user entry", entries)
	}
}

func TestCancelMidStreamPersistsAbortedPartial(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	llm := &streamingBlockingLLM{
		deltas:     []string{"par", "tial"},
		started:    make(chan struct{}),
		deltasSent: make(chan struct{}),
	}
	h, err := Open(ctx, NewMemoryStorage(), HarnessOptions{LLM: llm})
	if err != nil {
		t.Fatal(err)
	}

	var events []agent.Event
	sub, err := h.Root().SubmitStream(ctx, SubmitInput{Content: "hi", RequestID: "r1"}, func(ev agent.Event) error {
		events = append(events, ev)
		return nil
	})
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
	case <-llm.started:
	case <-time.After(5 * time.Second):
		t.Fatal("model call never started")
	}
	select {
	case <-llm.deltasSent:
	case <-time.After(5 * time.Second):
		t.Fatal("deltas never streamed")
	}

	cancel()

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

	entries, err := h.Root().Entries(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 2 {
		t.Fatalf("entries = %d, want 2 (user, assistant)", len(entries))
	}
	if entries[0].Kind != kindUser {
		t.Fatalf("entries[0].Kind = %s, want user", entries[0].Kind)
	}
	a := entries[1]
	if a.Kind != kindAssistant {
		t.Fatalf("entries[1].Kind = %s, want assistant", a.Kind)
	}
	if len(a.Model) != 1 || a.Model[0].Role != agent.RoleAssistant || a.Model[0].Content != "partial" {
		t.Fatalf("entries[1].Model = %+v, want assistant partial", a.Model)
	}
	var data assistantEntryData
	if err := json.Unmarshal(a.Data, &data); err != nil || !data.Aborted {
		t.Fatalf("entries[1].Data = %s, want aborted marker", a.Data)
	}

	var sawDeltas bool
	for _, ev := range events {
		if ev.Type == agent.EventTextDelta {
			sawDeltas = true
		}
	}
	if !sawDeltas {
		t.Fatal("sink never saw a text delta")
	}
}

func TestAbortedPartialFeedsNextRunContext(t *testing.T) {
	ctx1, cancel := context.WithCancel(context.Background())
	st := NewMemoryStorage()
	llm := &streamingBlockingLLM{
		deltas:     []string{"half answer"},
		started:    make(chan struct{}),
		deltasSent: make(chan struct{}),
	}
	h, err := Open(ctx1, st, HarnessOptions{LLM: llm})
	if err != nil {
		t.Fatal(err)
	}
	sub, err := h.Root().Submit(ctx1, SubmitInput{Content: "first", RequestID: "r1"})
	if err != nil {
		t.Fatal(err)
	}
	waitCh := make(chan *SubmissionRecord, 1)
	errCh := make(chan error, 1)
	go func() {
		rec, err := sub.Wait(ctx1)
		if err != nil {
			errCh <- err
			return
		}
		waitCh <- rec
	}()
	<-llm.started
	<-llm.deltasSent
	cancel()

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

	llm2 := &fakeLLM{steps: []llmStep{{resp: agent.Response{Content: "ok"}}}}
	h2, err := Open(context.Background(), st, HarnessOptions{LLM: llm2})
	if err != nil {
		t.Fatal(err)
	}
	sub2, err := h2.Root().Submit(context.Background(), SubmitInput{Content: "second", RequestID: "r2"})
	if err != nil {
		t.Fatal(err)
	}
	rec2, err := sub2.Wait(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if rec2.Status != SubmissionDone {
		t.Fatalf("second status = %s, want done", rec2.Status)
	}
	if llm2.calls != 1 {
		t.Fatalf("llm2 calls = %d, want 1", llm2.calls)
	}

	var foundAborted, foundUser bool
	for _, m := range llm2.messages[0] {
		switch {
		case m.Role == agent.RoleAssistant && m.Content == "half answer":
			foundAborted = true
		case m.Role == agent.RoleUser && m.Content == "second":
			foundUser = true
		}
	}
	if !foundAborted {
		t.Fatalf("context missing aborted partial: %+v", llm2.messages[0])
	}
	if !foundUser {
		t.Fatalf("context missing new user turn: %+v", llm2.messages[0])
	}
}

func TestSanitizeToolCalls(t *testing.T) {
	toolCall := agent.ToolCall{ID: "c1", Name: "search", Arguments: json.RawMessage(`{}`)}
	cases := []struct {
		name string
		in   []agent.Message
		want []agent.Message
	}{
		{
			name: "well formed unchanged",
			in: []agent.Message{
				{Role: agent.RoleUser, Content: "find"},
				{Role: agent.RoleAssistant, ToolCalls: []agent.ToolCall{toolCall}},
				{Role: agent.RoleTool, ToolCallID: "c1", Name: "search", Content: "hit"},
				{Role: agent.RoleAssistant, Content: "done"},
			},
			want: []agent.Message{
				{Role: agent.RoleUser, Content: "find"},
				{Role: agent.RoleAssistant, ToolCalls: []agent.ToolCall{toolCall}},
				{Role: agent.RoleTool, ToolCallID: "c1", Name: "search", Content: "hit"},
				{Role: agent.RoleAssistant, Content: "done"},
			},
		},
		{
			name: "dangling tool call gets placeholder",
			in: []agent.Message{
				{Role: agent.RoleUser, Content: "find"},
				{Role: agent.RoleAssistant, ToolCalls: []agent.ToolCall{toolCall}},
				{Role: agent.RoleUser, Content: "what are you doing"},
			},
			want: []agent.Message{
				{Role: agent.RoleUser, Content: "find"},
				{Role: agent.RoleAssistant, ToolCalls: []agent.ToolCall{toolCall}},
				{Role: agent.RoleTool, ToolCallID: "c1", Name: "search", Content: cancelledToolResult},
				{Role: agent.RoleUser, Content: "what are you doing"},
			},
		},
		{
			name: "orphan tool message dropped",
			in: []agent.Message{
				{Role: agent.RoleAssistant, ToolCalls: []agent.ToolCall{toolCall}},
				{Role: agent.RoleTool, ToolCallID: "other", Name: "search", Content: "stale"},
				{Role: agent.RoleUser, Content: "next"},
			},
			want: []agent.Message{
				{Role: agent.RoleAssistant, ToolCalls: []agent.ToolCall{toolCall}},
				{Role: agent.RoleTool, ToolCallID: "c1", Name: "search", Content: cancelledToolResult},
				{Role: agent.RoleUser, Content: "next"},
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := sanitizeToolCalls(tc.in); !reflect.DeepEqual(got, tc.want) {
				t.Fatalf("sanitizeToolCalls() = %#v, want %#v", got, tc.want)
			}
		})
	}
}
