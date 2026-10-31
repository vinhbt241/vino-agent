package durable

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"vino/internal/agent"
)

// commitSession routes writes through the session mutation line (rather than
// storage.Commit directly) so the session's lastSeq and retained commit log
// advance, which View and Watch read.
func commitSession(t *testing.T, h *Harness, ctx context.Context, writes []StorageWrite) {
	t.Helper()
	if err := h.session.CommitWrites(ctx, writes); err != nil {
		t.Fatal(err)
	}
}

func TestViewReturnsSnapshot(t *testing.T) {
	ctx := context.Background()
	h, err := Open(ctx, NewMemoryStorage(), HarnessOptions{LLM: &fakeLLM{}})
	if err != nil {
		t.Fatal(err)
	}
	convID := mint(t, h.session.storage, ctx)
	entryID := mint(t, h.session.storage, ctx)
	docID := mint(t, h.session.storage, ctx)
	subID := mint(t, h.session.storage, ctx)
	commitSession(t, h, ctx, []StorageWrite{
		{Type: WriteConversation, Value: &ConversationRecord{ID: RootConversationID}},
		{Type: WriteConversation, Value: &ConversationRecord{ID: convID}},
		{Type: WriteEntry, Value: &EntryRecord{ID: entryID, ConversationID: convID, Kind: kindUser, Model: []agent.Message{{Role: agent.RoleUser, Content: "hi"}}}},
		{Type: WriteDocument, Value: &DocumentRecord{ID: docID, ConversationID: convID, Name: "notes", Value: json.RawMessage(`"v"`)}},
		{Type: WriteSubmission, Value: &SubmissionRecord{ID: subID, ConversationID: convID, Type: SubmissionInput, Status: SubmissionDone}},
	})

	c, err := h.Conversation(ctx, convID)
	if err != nil {
		t.Fatal(err)
	}
	v, err := c.View(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if v.Conversation.ID != convID {
		t.Fatalf("view conversation = %d, want %d", v.Conversation.ID, convID)
	}
	if len(v.Entries) != 1 || v.Entries[0].ID != entryID {
		t.Fatalf("view entries = %v, want [%d]", v.Entries, entryID)
	}
	if len(v.Documents) != 1 || string(v.Documents[0].Value) != `"v"` {
		t.Fatalf("view documents = %v, want one with value \"v\"", v.Documents)
	}
	if len(v.Submissions) != 1 || v.Submissions[0].ID != subID {
		t.Fatalf("view submissions = %v, want [%d]", v.Submissions, subID)
	}
	if v.Seq == 0 {
		t.Fatal("view Seq = 0, want the commit sequence of the seeded commit")
	}
}

func TestWatchCatchesUpFromSeq(t *testing.T) {
	ctx := context.Background()
	h, err := Open(ctx, NewMemoryStorage(), HarnessOptions{LLM: &fakeLLM{}})
	if err != nil {
		t.Fatal(err)
	}
	convID := mint(t, h.session.storage, ctx)
	entryA := mint(t, h.session.storage, ctx)
	entryB := mint(t, h.session.storage, ctx)
	commitSession(t, h, ctx, []StorageWrite{
		{Type: WriteConversation, Value: &ConversationRecord{ID: RootConversationID}},
		{Type: WriteConversation, Value: &ConversationRecord{ID: convID}},
		{Type: WriteEntry, Value: &EntryRecord{ID: entryA, ConversationID: convID, Kind: kindUser, Model: []agent.Message{{Role: agent.RoleUser, Content: "a"}}}},
	})

	c, err := h.Conversation(ctx, convID)
	if err != nil {
		t.Fatal(err)
	}
	v, err := c.View(ctx)
	if err != nil {
		t.Fatal(err)
	}

	commitSession(t, h, ctx, []StorageWrite{
		{Type: WriteEntry, Value: &EntryRecord{ID: entryB, ConversationID: convID, Kind: kindUser, Model: []agent.Message{{Role: agent.RoleUser, Content: "b"}}}},
	})

	ops := make(chan CommitOp, 8)
	cancel, err := c.Watch(ctx, v.Seq, func(op CommitOp) error {
		ops <- op
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	defer cancel()

	select {
	case op := <-ops:
		if len(op.Writes) != 1 {
			t.Fatalf("caught up writes = %d, want 1 (entry B only)", len(op.Writes))
		}
		w := op.Writes[0]
		if w.Type != WriteEntry || w.Value.(*EntryRecord).ID != entryB {
			t.Fatalf("caught up write = %v, want entry %d", w, entryB)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for watch catch-up")
	}

	select {
	case op := <-ops:
		t.Fatalf("unexpected extra op %v (entry A must not be re-delivered)", op)
	case <-time.After(50 * time.Millisecond):
	}
}

func TestWatchStreamsLiveCommits(t *testing.T) {
	ctx := context.Background()
	h, err := Open(ctx, NewMemoryStorage(), HarnessOptions{LLM: &fakeLLM{}})
	if err != nil {
		t.Fatal(err)
	}
	convID := mint(t, h.session.storage, ctx)
	commitSession(t, h, ctx, []StorageWrite{
		{Type: WriteConversation, Value: &ConversationRecord{ID: RootConversationID}},
		{Type: WriteConversation, Value: &ConversationRecord{ID: convID}},
	})

	c, err := h.Conversation(ctx, convID)
	if err != nil {
		t.Fatal(err)
	}
	v, err := c.View(ctx)
	if err != nil {
		t.Fatal(err)
	}

	ops := make(chan CommitOp, 8)
	cancel, err := c.Watch(ctx, v.Seq, func(op CommitOp) error {
		ops <- op
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	defer cancel()

	entryB := mint(t, h.session.storage, ctx)
	commitSession(t, h, ctx, []StorageWrite{
		{Type: WriteEntry, Value: &EntryRecord{ID: entryB, ConversationID: convID, Kind: kindUser, Model: []agent.Message{{Role: agent.RoleUser, Content: "b"}}}},
	})

	select {
	case op := <-ops:
		if len(op.Writes) != 1 || op.Writes[0].Value.(*EntryRecord).ID != entryB {
			t.Fatalf("live op = %v, want entry %d", op, entryB)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for live commit")
	}
}

func TestWatchFiltersToConversation(t *testing.T) {
	ctx := context.Background()
	h, err := Open(ctx, NewMemoryStorage(), HarnessOptions{LLM: &fakeLLM{}})
	if err != nil {
		t.Fatal(err)
	}
	convA := mint(t, h.session.storage, ctx)
	convB := mint(t, h.session.storage, ctx)
	commitSession(t, h, ctx, []StorageWrite{
		{Type: WriteConversation, Value: &ConversationRecord{ID: RootConversationID}},
		{Type: WriteConversation, Value: &ConversationRecord{ID: convA}},
		{Type: WriteConversation, Value: &ConversationRecord{ID: convB}},
	})

	cA, err := h.Conversation(ctx, convA)
	if err != nil {
		t.Fatal(err)
	}
	vA, err := cA.View(ctx)
	if err != nil {
		t.Fatal(err)
	}

	ops := make(chan CommitOp, 8)
	cancel, err := cA.Watch(ctx, vA.Seq, func(op CommitOp) error {
		ops <- op
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	defer cancel()

	entryA := mint(t, h.session.storage, ctx)
	entryB := mint(t, h.session.storage, ctx)
	commitSession(t, h, ctx, []StorageWrite{
		{Type: WriteEntry, Value: &EntryRecord{ID: entryB, ConversationID: convB, Kind: kindUser, Model: []agent.Message{{Role: agent.RoleUser, Content: "b"}}}},
		{Type: WriteEntry, Value: &EntryRecord{ID: entryA, ConversationID: convA, Kind: kindUser, Model: []agent.Message{{Role: agent.RoleUser, Content: "a"}}}},
	})

	select {
	case op := <-ops:
		if len(op.Writes) != 1 || op.Writes[0].Value.(*EntryRecord).ID != entryA {
			t.Fatalf("op = %v, want only entry %d (entry B filtered out)", op, entryA)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for conversation A's commit")
	}

	select {
	case op := <-ops:
		t.Fatalf("unexpected extra op %v (B's entry must not be delivered)", op)
	case <-time.After(50 * time.Millisecond):
	}
}

func TestWatchExcludesTasks(t *testing.T) {
	ctx := context.Background()
	h, err := Open(ctx, NewMemoryStorage(), HarnessOptions{LLM: &fakeLLM{}})
	if err != nil {
		t.Fatal(err)
	}
	convID := mint(t, h.session.storage, ctx)
	commitSession(t, h, ctx, []StorageWrite{
		{Type: WriteConversation, Value: &ConversationRecord{ID: RootConversationID}},
		{Type: WriteConversation, Value: &ConversationRecord{ID: convID}},
	})

	c, err := h.Conversation(ctx, convID)
	if err != nil {
		t.Fatal(err)
	}
	v, err := c.View(ctx)
	if err != nil {
		t.Fatal(err)
	}

	ops := make(chan CommitOp, 8)
	cancel, err := c.Watch(ctx, v.Seq, func(op CommitOp) error {
		ops <- op
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	defer cancel()

	taskID := mint(t, h.session.storage, ctx)
	entryID := mint(t, h.session.storage, ctx)
	commitSession(t, h, ctx, []StorageWrite{
		{Type: WriteTask, Value: &TaskRecord{ID: taskID, ConversationID: convID, Kind: "test.kind", Version: 1, Input: json.RawMessage("{}"), State: TaskState{Status: TaskPending, Checkpoint: json.RawMessage("0")}}},
		{Type: WriteEntry, Value: &EntryRecord{ID: entryID, ConversationID: convID, Kind: kindUser, Model: []agent.Message{{Role: agent.RoleUser, Content: "hi"}}}},
	})

	select {
	case op := <-ops:
		if len(op.Writes) != 1 || op.Writes[0].Type != WriteEntry || op.Writes[0].Value.(*EntryRecord).ID != entryID {
			t.Fatalf("op = %v, want only entry %d (task filtered out)", op, entryID)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for the entry commit")
	}

	select {
	case op := <-ops:
		t.Fatalf("unexpected extra op %v (task must not be delivered)", op)
	case <-time.After(50 * time.Millisecond):
	}
}

func TestWatchDetachesOnError(t *testing.T) {
	ctx := context.Background()
	h, err := Open(ctx, NewMemoryStorage(), HarnessOptions{LLM: &fakeLLM{}})
	if err != nil {
		t.Fatal(err)
	}
	convID := mint(t, h.session.storage, ctx)
	commitSession(t, h, ctx, []StorageWrite{
		{Type: WriteConversation, Value: &ConversationRecord{ID: RootConversationID}},
		{Type: WriteConversation, Value: &ConversationRecord{ID: convID}},
	})
	c, err := h.Conversation(ctx, convID)
	if err != nil {
		t.Fatal(err)
	}
	v, err := c.View(ctx)
	if err != nil {
		t.Fatal(err)
	}

	called := make(chan struct{}, 4)
	cancel, err := c.Watch(ctx, v.Seq, func(op CommitOp) error {
		called <- struct{}{}
		return errors.New("boom")
	})
	if err != nil {
		t.Fatal(err)
	}
	defer cancel()

	entryB := mint(t, h.session.storage, ctx)
	commitSession(t, h, ctx, []StorageWrite{
		{Type: WriteEntry, Value: &EntryRecord{ID: entryB, ConversationID: convID, Kind: kindUser, Model: []agent.Message{{Role: agent.RoleUser, Content: "b"}}}},
	})
	select {
	case <-called:
	case <-time.After(2 * time.Second):
		t.Fatal("watcher was not called")
	}

	// Wait until the watcher detaches (fn returned an error).
	deadline := time.Now().Add(2 * time.Second)
	for {
		h.session.mu.Lock()
		n := len(h.session.watchers)
		h.session.mu.Unlock()
		if n == 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("watcher did not detach after error")
		}
		time.Sleep(time.Millisecond)
	}

	entryC := mint(t, h.session.storage, ctx)
	commitSession(t, h, ctx, []StorageWrite{
		{Type: WriteEntry, Value: &EntryRecord{ID: entryC, ConversationID: convID, Kind: kindUser, Model: []agent.Message{{Role: agent.RoleUser, Content: "c"}}}},
	})

	select {
	case <-called:
		t.Fatal("watcher delivered after error")
	case <-time.After(50 * time.Millisecond):
	}
}

func TestWatchCancelStopsDelivery(t *testing.T) {
	ctx := context.Background()
	h, err := Open(ctx, NewMemoryStorage(), HarnessOptions{LLM: &fakeLLM{}})
	if err != nil {
		t.Fatal(err)
	}
	convID := mint(t, h.session.storage, ctx)
	commitSession(t, h, ctx, []StorageWrite{
		{Type: WriteConversation, Value: &ConversationRecord{ID: RootConversationID}},
		{Type: WriteConversation, Value: &ConversationRecord{ID: convID}},
	})
	c, err := h.Conversation(ctx, convID)
	if err != nil {
		t.Fatal(err)
	}
	v, err := c.View(ctx)
	if err != nil {
		t.Fatal(err)
	}

	got := make(chan CommitOp, 8)
	cancel, err := c.Watch(ctx, v.Seq, func(op CommitOp) error {
		got <- op
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	cancel()

	entryB := mint(t, h.session.storage, ctx)
	commitSession(t, h, ctx, []StorageWrite{
		{Type: WriteEntry, Value: &EntryRecord{ID: entryB, ConversationID: convID, Kind: kindUser, Model: []agent.Message{{Role: agent.RoleUser, Content: "b"}}}},
	})

	select {
	case op := <-got:
		t.Fatalf("delivered after cancel: %v", op)
	case <-time.After(50 * time.Millisecond):
	}
}

func TestWatchEventsFansOut(t *testing.T) {
	ctx := context.Background()
	llm := &fakeLLM{steps: []llmStep{{resp: agent.Response{Content: "hi"}}}}
	h, err := Open(ctx, NewMemoryStorage(), HarnessOptions{LLM: llm})
	if err != nil {
		t.Fatal(err)
	}
	root := h.Root()

	ev1 := make(chan agent.Event, 8)
	ev2 := make(chan agent.Event, 8)
	cancel1, err := root.WatchEvents(ctx, func(ev agent.Event) error {
		ev1 <- ev
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	defer cancel1()
	cancel2, err := root.WatchEvents(ctx, func(ev agent.Event) error {
		ev2 <- ev
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	defer cancel2()

	sub, err := root.Submit(ctx, SubmitInput{Content: "hi"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := sub.Wait(ctx); err != nil {
		t.Fatal(err)
	}

	for i, ch := range []chan agent.Event{ev1, ev2} {
		select {
		case e := <-ch:
			if e.Type != agent.EventRunFinished {
				t.Fatalf("subscriber %d first event = %v, want run_finished", i+1, e.Type)
			}
		case <-time.After(2 * time.Second):
			t.Fatalf("subscriber %d did not receive run_finished", i+1)
		}
	}
}

func TestWatchEventsDetachesOnError(t *testing.T) {
	ctx := context.Background()
	base := &fakeRegistry{specs: []agent.ToolSpec{{Name: "read"}}, results: map[string]string{"read": "r"}}
	llm := &fakeLLM{steps: []llmStep{
		{resp: agent.Response{ToolCalls: []agent.ToolCall{{ID: "c1", Name: "read", Arguments: json.RawMessage("{}")}}}},
		{resp: agent.Response{Content: "done"}},
	}}
	h, err := Open(ctx, NewMemoryStorage(), HarnessOptions{LLM: llm, Tools: base})
	if err != nil {
		t.Fatal(err)
	}
	root := h.Root()

	var bad, good []agent.Event
	cancelBad, err := root.WatchEvents(ctx, func(ev agent.Event) error {
		bad = append(bad, ev)
		return errors.New("boom")
	})
	if err != nil {
		t.Fatal(err)
	}
	defer cancelBad()
	cancelGood, err := root.WatchEvents(ctx, func(ev agent.Event) error {
		good = append(good, ev)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	defer cancelGood()

	sub, err := root.Submit(ctx, SubmitInput{Content: "hi"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := sub.Wait(ctx); err != nil {
		t.Fatal(err)
	}

	if len(good) != 3 {
		t.Fatalf("good subscriber events = %d, want 3", len(good))
	}
	if len(bad) != 1 || bad[0].Type != agent.EventToolCall {
		t.Fatalf("bad subscriber events = %d, want exactly the first tool_call_started then detach", len(bad))
	}
}

func TestWatchEventsCancelStopsDelivery(t *testing.T) {
	ctx := context.Background()
	llm := &fakeLLM{steps: []llmStep{{resp: agent.Response{Content: "hi"}}}}
	h, err := Open(ctx, NewMemoryStorage(), HarnessOptions{LLM: llm})
	if err != nil {
		t.Fatal(err)
	}
	root := h.Root()

	var got []agent.Event
	cancel, err := root.WatchEvents(ctx, func(ev agent.Event) error {
		got = append(got, ev)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	cancel()

	sub, err := root.Submit(ctx, SubmitInput{Content: "hi"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := sub.Wait(ctx); err != nil {
		t.Fatal(err)
	}

	if len(got) != 0 {
		t.Fatalf("received events after cancel: %v", got)
	}
}

func TestSubmitSinkAndWatchEventsCoexist(t *testing.T) {
	ctx := context.Background()
	llm := &fakeLLM{steps: []llmStep{{resp: agent.Response{Content: "hi"}}}}
	h, err := Open(ctx, NewMemoryStorage(), HarnessOptions{LLM: llm})
	if err != nil {
		t.Fatal(err)
	}
	root := h.Root()

	// Two WatchEvents subscribers plus a SubmitStream sink exercise the two
	// subscriber namespaces: the harness-local counter and the storage-minted
	// submission ID must not collide.
	var w1, w2, sink []agent.Event
	cancelW1, err := root.WatchEvents(ctx, func(ev agent.Event) error {
		w1 = append(w1, ev)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	defer cancelW1()
	cancelW2, err := root.WatchEvents(ctx, func(ev agent.Event) error {
		w2 = append(w2, ev)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	defer cancelW2()

	sub, err := root.SubmitStream(ctx, SubmitInput{Content: "hi"}, func(ev agent.Event) error {
		sink = append(sink, ev)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := sub.Wait(ctx); err != nil {
		t.Fatal(err)
	}

	for name, evs := range map[string][]agent.Event{"watch1": w1, "watch2": w2, "sink": sink} {
		if len(evs) == 0 || evs[len(evs)-1].Type != agent.EventRunFinished {
			t.Fatalf("%s received %d events, want run_finished last", name, len(evs))
		}
	}
}

func TestWatchCatchesUpClonesDeliveredWrites(t *testing.T) {
	ctx := context.Background()
	h, err := Open(ctx, NewMemoryStorage(), HarnessOptions{LLM: &fakeLLM{}})
	if err != nil {
		t.Fatal(err)
	}
	convID := mint(t, h.session.storage, ctx)
	commitSession(t, h, ctx, []StorageWrite{
		{Type: WriteConversation, Value: &ConversationRecord{ID: RootConversationID}},
		{Type: WriteConversation, Value: &ConversationRecord{ID: convID}},
	})
	c, err := h.Conversation(ctx, convID)
	if err != nil {
		t.Fatal(err)
	}
	v, err := c.View(ctx)
	if err != nil {
		t.Fatal(err)
	}

	entryID := mint(t, h.session.storage, ctx)
	commitSession(t, h, ctx, []StorageWrite{
		{Type: WriteEntry, Value: &EntryRecord{ID: entryID, ConversationID: convID, Kind: kindUser, Model: []agent.Message{{Role: agent.RoleUser, Content: "original"}}}},
	})

	// The first watcher mutates the delivered entry in place.
	cancel1, err := c.Watch(ctx, v.Seq, func(op CommitOp) error {
		for _, w := range op.Writes {
			if w.Type == WriteEntry {
				w.Value.(*EntryRecord).Model[0].Content = "mutated"
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	cancel1()

	// A second watcher replaying the same seq must see the original value,
	// not the mutation the first watcher made.
	got := make(chan CommitOp, 1)
	cancel2, err := c.Watch(ctx, v.Seq, func(op CommitOp) error {
		got <- op
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	defer cancel2()

	select {
	case op := <-got:
		if len(op.Writes) != 1 || op.Writes[0].Value.(*EntryRecord).Model[0].Content != "original" {
			t.Fatalf("replayed op = %v, want original content (retained history was mutated)", op)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for replay")
	}
}

func TestWatchLiveClonesDeliveredWrites(t *testing.T) {
	ctx := context.Background()
	h, err := Open(ctx, NewMemoryStorage(), HarnessOptions{LLM: &fakeLLM{}})
	if err != nil {
		t.Fatal(err)
	}
	convID := mint(t, h.session.storage, ctx)
	commitSession(t, h, ctx, []StorageWrite{
		{Type: WriteConversation, Value: &ConversationRecord{ID: RootConversationID}},
		{Type: WriteConversation, Value: &ConversationRecord{ID: convID}},
	})
	c, err := h.Conversation(ctx, convID)
	if err != nil {
		t.Fatal(err)
	}
	v, err := c.View(ctx)
	if err != nil {
		t.Fatal(err)
	}

	mutated := make(chan struct{})
	seen := make(chan string, 1)

	cancel1, err := c.Watch(ctx, v.Seq, func(op CommitOp) error {
		for _, w := range op.Writes {
			if w.Type == WriteEntry {
				w.Value.(*EntryRecord).Model[0].Content = "mutated"
			}
		}
		close(mutated)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	defer cancel1()

	cancel2, err := c.Watch(ctx, v.Seq, func(op CommitOp) error {
		select {
		case <-mutated:
		case <-time.After(2 * time.Second):
			seen <- "timeout"
			return nil
		}
		for _, w := range op.Writes {
			if w.Type == WriteEntry {
				seen <- w.Value.(*EntryRecord).Model[0].Content
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	defer cancel2()

	entryID := mint(t, h.session.storage, ctx)
	commitSession(t, h, ctx, []StorageWrite{
		{Type: WriteEntry, Value: &EntryRecord{ID: entryID, ConversationID: convID, Kind: kindUser, Model: []agent.Message{{Role: agent.RoleUser, Content: "original"}}}},
	})

	select {
	case got := <-seen:
		if got != "original" {
			t.Fatalf("second watcher saw %q, want original (live delivery shares memory)", got)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for the second watcher")
	}
}

func TestWatchSeqExpired(t *testing.T) {
	ctx := context.Background()
	h, err := Open(ctx, NewMemoryStorage(), HarnessOptions{LLM: &fakeLLM{}})
	if err != nil {
		t.Fatal(err)
	}
	h.session.retainCommits = 1

	convID := mint(t, h.session.storage, ctx)
	commitSession(t, h, ctx, []StorageWrite{
		{Type: WriteConversation, Value: &ConversationRecord{ID: RootConversationID}},
		{Type: WriteConversation, Value: &ConversationRecord{ID: convID}},
	})
	c, err := h.Conversation(ctx, convID)
	if err != nil {
		t.Fatal(err)
	}
	v, err := c.View(ctx)
	if err != nil {
		t.Fatal(err)
	}

	entryID := mint(t, h.session.storage, ctx)
	commitSession(t, h, ctx, []StorageWrite{
		{Type: WriteEntry, Value: &EntryRecord{ID: entryID, ConversationID: convID, Kind: kindUser, Model: []agent.Message{{Role: agent.RoleUser, Content: "hi"}}}},
	})

	// The commit v.Seq reflects has been evicted; watching after it expires.
	cancel, err := c.Watch(ctx, v.Seq, func(op CommitOp) error { return nil })
	if !errors.Is(err, ErrWatchSeqExpired) {
		t.Fatalf("Watch(after=%d) err = %v, want ErrWatchSeqExpired", v.Seq, err)
	}
	if cancel != nil {
		t.Fatalf("Watch(after=%d) returned non-nil cancel, want nil", v.Seq)
	}
}

func TestWatchWithinWindowStillCatchesUp(t *testing.T) {
	ctx := context.Background()
	h, err := Open(ctx, NewMemoryStorage(), HarnessOptions{LLM: &fakeLLM{}})
	if err != nil {
		t.Fatal(err)
	}
	h.session.retainCommits = 2

	convID := mint(t, h.session.storage, ctx)
	commitSession(t, h, ctx, []StorageWrite{
		{Type: WriteConversation, Value: &ConversationRecord{ID: RootConversationID}},
		{Type: WriteConversation, Value: &ConversationRecord{ID: convID}},
	})
	c, err := h.Conversation(ctx, convID)
	if err != nil {
		t.Fatal(err)
	}
	entryA := mint(t, h.session.storage, ctx)
	commitSession(t, h, ctx, []StorageWrite{
		{Type: WriteEntry, Value: &EntryRecord{ID: entryA, ConversationID: convID, Kind: kindUser, Model: []agent.Message{{Role: agent.RoleUser, Content: "a"}}}},
	})
	v, err := c.View(ctx)
	if err != nil {
		t.Fatal(err)
	}

	entryB := mint(t, h.session.storage, ctx)
	commitSession(t, h, ctx, []StorageWrite{
		{Type: WriteEntry, Value: &EntryRecord{ID: entryB, ConversationID: convID, Kind: kindUser, Model: []agent.Message{{Role: agent.RoleUser, Content: "b"}}}},
	})

	// The window still holds v.Seq; watching after it delivers only entryB.
	ops := make(chan CommitOp, 8)
	cancel, err := c.Watch(ctx, v.Seq, func(op CommitOp) error {
		ops <- op
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	defer cancel()

	select {
	case op := <-ops:
		if len(op.Writes) != 1 || op.Writes[0].Value.(*EntryRecord).ID != entryB {
			t.Fatalf("caught up op = %v, want entry %d only", op, entryB)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for catch-up within window")
	}

	select {
	case op := <-ops:
		t.Fatalf("unexpected extra op %v", op)
	case <-time.After(50 * time.Millisecond):
	}
}

func TestWatchEventsNilFnErrors(t *testing.T) {
	ctx := context.Background()
	h, err := Open(ctx, NewMemoryStorage(), HarnessOptions{LLM: &fakeLLM{}})
	if err != nil {
		t.Fatal(err)
	}
	root := h.Root()
	cancel, err := root.WatchEvents(ctx, nil)
	if err == nil {
		t.Fatal("WatchEvents(nil) returned nil error, want error")
	}
	if cancel != nil {
		t.Fatal("WatchEvents(nil) returned non-nil cancel, want nil")
	}
}
