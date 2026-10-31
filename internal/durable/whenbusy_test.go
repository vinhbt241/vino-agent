package durable

import (
	"context"
	"errors"
	"sync"
	"testing"

	"vino/internal/agent"
)

// steerLLM blocks its first Chat until the context is cancelled (mirroring an
// in-flight model request a steer must interrupt), then returns a scripted
// response on the second call. It records every message list it is given.
type steerLLM struct {
	started chan struct{}
	resp    agent.Response

	mu      sync.Mutex
	calls   int
	allMsgs [][]agent.Message
}

func (s *steerLLM) Chat(ctx context.Context, msgs []agent.Message, _ []agent.ToolSpec, _ func(string) error) (agent.Response, error) {
	s.mu.Lock()
	s.calls++
	n := s.calls
	s.allMsgs = append(s.allMsgs, append([]agent.Message(nil), msgs...))
	s.mu.Unlock()

	if n == 1 {
		close(s.started)
		<-ctx.Done()
		return agent.Response{}, ctx.Err()
	}
	return s.resp, nil
}

func (s *steerLLM) callCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.calls
}

func TestRejectWhenBusy(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	llm := &blockingLLM{started: make(chan struct{})}
	h, err := Open(ctx, NewMemoryStorage(), HarnessOptions{LLM: llm})
	if err != nil {
		t.Fatal(err)
	}
	sub, err := h.Start(ctx, StartInput{Messages: []agent.Message{{Role: agent.RoleUser, Content: "first"}}}, nil)
	if err != nil {
		t.Fatal(err)
	}
	go func() { _, _ = sub.Wait(ctx) }()
	<-llm.started // the run is now mid-model: busy

	c, err := h.Conversation(ctx, sub.ConversationID())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.Submit(ctx, SubmitInput{Content: "rejected", WhenBusy: WhenBusyReject}); !errors.Is(err, ErrConversationBusy) {
		t.Fatalf("Submit(reject) err = %v, want ErrConversationBusy", err)
	}

	// Nothing committed: the transcript still holds only the original entry.
	entries, err := c.Entries(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 || entries[0].Model[0].Content != "first" {
		t.Fatalf("entries = %#v, want only the original \"first\" entry", entries)
	}
}

func TestRejectWhenIdleSubmits(t *testing.T) {
	ctx := context.Background()
	llm := &fakeLLM{steps: []llmStep{{resp: agent.Response{Content: "done"}}}}
	h, err := Open(ctx, NewMemoryStorage(), HarnessOptions{LLM: llm})
	if err != nil {
		t.Fatal(err)
	}
	sub, err := h.Start(ctx, StartInput{Messages: []agent.Message{{Role: agent.RoleUser, Content: "first"}}}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := sub.Wait(ctx); err != nil {
		t.Fatal(err)
	}

	c, err := h.Conversation(ctx, sub.ConversationID())
	if err != nil {
		t.Fatal(err)
	}
	sub2, err := c.Submit(ctx, SubmitInput{Content: "second", WhenBusy: WhenBusyReject})
	if err != nil {
		t.Fatalf("Submit(reject) on idle conversation err = %v, want nil", err)
	}
	if sub2.ID() == sub.ID() {
		t.Fatalf("idle reject should create a new submission, got original %d", sub2.ID())
	}
}

func TestSteerMergesIntoRun(t *testing.T) {
	ctx := context.Background()
	llm := &steerLLM{started: make(chan struct{}), resp: agent.Response{Content: "final answer"}}
	h, err := Open(ctx, NewMemoryStorage(), HarnessOptions{LLM: llm})
	if err != nil {
		t.Fatal(err)
	}
	sub1, err := h.Start(ctx, StartInput{Messages: []agent.Message{{Role: agent.RoleUser, Content: "first"}}}, nil)
	if err != nil {
		t.Fatal(err)
	}

	runDone := make(chan error, 1)
	go func() {
		_, err := sub1.Wait(ctx)
		runDone <- err
	}()
	<-llm.started // mid-model: steer can interrupt

	c, err := h.Conversation(ctx, sub1.ConversationID())
	if err != nil {
		t.Fatal(err)
	}
	sub2, err := c.Submit(ctx, SubmitInput{Content: "steered", WhenBusy: WhenBusySteer})
	if err != nil {
		t.Fatalf("Submit(steer) err = %v, want nil", err)
	}
	if !sub2.Resumed() {
		t.Fatal("steered submission should be resumed")
	}
	if sub2.ID() != sub1.ID() {
		t.Fatalf("steered submission id = %d, want original %d", sub2.ID(), sub1.ID())
	}

	if err := <-runDone; err != nil {
		t.Fatalf("run settle err = %v, want nil", err)
	}

	// The run settled done (not unanswered): the steer did not abort it.
	rec, err := h.session.storage.Submission(ctx, sub1.ID())
	if err != nil {
		t.Fatal(err)
	}
	if rec.Status != SubmissionDone {
		t.Fatalf("submission status = %q, want done", rec.Status)
	}

	entries, err := c.Entries(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 3 {
		t.Fatalf("entries = %d, want 3 (first, steered, answer)", len(entries))
	}
	if entries[1].Model[0].Content != "steered" {
		t.Fatalf("entries[1] = %q, want steered", entries[1].Model[0].Content)
	}
	if entries[2].Model[0].Content != "final answer" {
		t.Fatalf("entries[2] = %q, want final answer", entries[2].Model[0].Content)
	}

	// The steered message reached the re-run model request.
	if n := llm.callCount(); n != 2 {
		t.Fatalf("Chat calls = %d, want 2 (cancelled + rerun)", n)
	}
	llm.mu.Lock()
	second := llm.allMsgs[1]
	llm.mu.Unlock()
	found := false
	for _, m := range second {
		if m.Role == agent.RoleUser && m.Content == "steered" {
			found = true
		}
	}
	if !found {
		t.Fatalf("second Chat messages = %#v, want the steered user message", second)
	}
}

func TestDefaultQueuesSecondRun(t *testing.T) {
	ctx := context.Background()
	h, err := Open(ctx, NewMemoryStorage(), HarnessOptions{LLM: &fakeLLM{}})
	if err != nil {
		t.Fatal(err)
	}
	sub1, err := h.Start(ctx, StartInput{Messages: []agent.Message{{Role: agent.RoleUser, Content: "a"}}}, nil)
	if err != nil {
		t.Fatal(err)
	}
	c, err := h.Conversation(ctx, sub1.ConversationID())
	if err != nil {
		t.Fatal(err)
	}
	sub2, err := c.Submit(ctx, SubmitInput{Content: "b"})
	if err != nil {
		t.Fatal(err)
	}
	if sub2.ID() == sub1.ID() {
		t.Fatal("default submit should queue a new submission, not resume the original")
	}

	convID := sub1.ConversationID()
	page, err := h.session.storage.ScanTasks(ctx, TaskQuery{ConversationID: &convID, Kind: kindGeneration}, 0, nil)
	if err != nil {
		t.Fatal(err)
	}
	nonTerminal := 0
	for _, task := range page.Items {
		if task.State.Status != TaskTerminal && task.State.Status != TaskCompleting {
			nonTerminal++
		}
	}
	if nonTerminal != 2 {
		t.Fatalf("non-terminal generation tasks = %d, want 2 (queued)", nonTerminal)
	}
}
