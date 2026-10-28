package durable

import (
	"context"
	"encoding/json"
	"path/filepath"
	"testing"

	"vino/internal/agent"
)

// runConformance exercises the Storage contract over any backend. newStore
// returns a fresh (or reopened) backend; reopen returns a backend opened over
// the same durable state (identity for memory).
func runConformance(t *testing.T, newStore func(*testing.T) Storage, reopen func(*testing.T, Storage) Storage) {
	ctx := context.Background()
	store := newStore(t)

	t.Run("conversation entry task submission", func(t *testing.T) {
		convID, _ := store.MintID(ctx)
		entryID, _ := store.MintID(ctx)
		taskID, _ := store.MintID(ctx)
		subID, _ := store.MintID(ctx)

		_, err := store.Commit(ctx, []StorageWrite{
			{Type: WriteConversation, Value: &ConversationRecord{ID: convID}},
			{Type: WriteEntry, Value: &EntryRecord{ID: entryID, ConversationID: convID, Kind: "user",
				Model: []agent.Message{{Role: agent.RoleUser, Content: "hello"}}}},
			{Type: WriteTask, Value: &TaskRecord{ID: taskID, ConversationID: convID, Kind: "k", Version: 1,
				State: TaskState{Status: TaskPending, Checkpoint: json.RawMessage(`{"phase":"run"}`)}}},
			{Type: WriteSubmission, Value: &SubmissionRecord{ID: subID, ConversationID: convID,
				RequestID: "r1", Type: SubmissionInput, Status: SubmissionPlaced}},
		})
		if err != nil {
			t.Fatalf("commit: %v", err)
		}

		conv, _ := store.Conversation(ctx, convID)
		if conv == nil || conv.ID != convID {
			t.Fatalf("conversation = %+v, want id %d", conv, convID)
		}
		entry, _ := store.Entry(ctx, entryID)
		if entry == nil || entry.Kind != "user" || len(entry.Model) != 1 || entry.Model[0].Content != "hello" {
			t.Fatalf("entry = %+v", entry)
		}
		task, _ := store.Task(ctx, taskID)
		if task == nil || task.State.Status != TaskPending {
			t.Fatalf("task = %+v", task)
		}
		sub, _ := store.Submission(ctx, subID)
		if sub == nil || sub.Status != SubmissionPlaced {
			t.Fatalf("submission = %+v", sub)
		}
	})

	t.Run("atomic on invalid batch", func(t *testing.T) {
		entryID, _ := store.MintID(ctx)
		if _, err := store.Commit(ctx, []StorageWrite{
			{Type: WriteEntry, Value: &EntryRecord{ID: entryID, ConversationID: 1, Kind: "user"}},
		}); err != nil {
			t.Fatalf("seed: %v", err)
		}
		// Duplicate entry in the same batch as a valid write: nothing applies.
		bad := entryID
		otherID, _ := store.MintID(ctx)
		if _, err := store.Commit(ctx, []StorageWrite{
			{Type: WriteEntry, Value: &EntryRecord{ID: bad, ConversationID: 1, Kind: "user"}},
			{Type: WriteEntry, Value: &EntryRecord{ID: otherID, ConversationID: 1, Kind: "user"}},
		}); err == nil {
			t.Fatal("expected duplicate-entry commit to fail")
		}
		if e, _ := store.Entry(ctx, otherID); e != nil {
			t.Fatalf("valid write in failed batch was applied: %+v", e)
		}
	})

	t.Run("task replacement and terminal immutability", func(t *testing.T) {
		taskID, _ := store.MintID(ctx)
		if _, err := store.Commit(ctx, []StorageWrite{
			{Type: WriteTask, Value: &TaskRecord{ID: taskID, ConversationID: 1, Kind: "k", Version: 1,
				State: TaskState{Status: TaskPending, Checkpoint: json.RawMessage(`{"n":1}`)}}},
		}); err != nil {
			t.Fatalf("seed: %v", err)
		}
		if _, err := store.Commit(ctx, []StorageWrite{
			{Type: WriteTask, Value: &TaskRecord{ID: taskID, ConversationID: 1, Kind: "k", Version: 1,
				State: TaskState{Status: TaskRunning, Checkpoint: json.RawMessage(`{"n":2}`)}}},
		}); err != nil {
			t.Fatalf("replace running: %v", err)
		}
		task, _ := store.Task(ctx, taskID)
		if task.State.Status != TaskRunning {
			t.Fatalf("task = %+v", task)
		}
		if _, err := store.Commit(ctx, []StorageWrite{
			{Type: WriteTask, Value: &TaskRecord{ID: taskID, ConversationID: 1, Kind: "k", Version: 1,
				State: TaskState{Status: TaskTerminal, Outcome: &Outcome{Status: OutcomeCompleted}}}},
		}); err != nil {
			t.Fatalf("terminal: %v", err)
		}
		if _, err := store.Commit(ctx, []StorageWrite{
			{Type: WriteTask, Value: &TaskRecord{ID: taskID, ConversationID: 1, Kind: "k", Version: 1,
				State: TaskState{Status: TaskPending}}},
		}); err == nil {
			t.Fatal("expected terminal task replacement to fail")
		}
	})

	t.Run("scans order and pagination", func(t *testing.T) {
		convID, _ := store.MintID(ctx)
		if _, err := store.Commit(ctx, []StorageWrite{
			{Type: WriteConversation, Value: &ConversationRecord{ID: convID}},
		}); err != nil {
			t.Fatalf("seed conversation: %v", err)
		}
		ids := make([]ID, 3)
		for i := range ids {
			ids[i], _ = store.MintID(ctx)
		}
		if _, err := store.Commit(ctx, []StorageWrite{
			{Type: WriteEntry, Value: &EntryRecord{ID: ids[0], ConversationID: convID, Kind: "user"}},
			{Type: WriteEntry, Value: &EntryRecord{ID: ids[1], ConversationID: convID, Kind: "assistant"}},
			{Type: WriteEntry, Value: &EntryRecord{ID: ids[2], ConversationID: convID, Kind: "user"}},
		}); err != nil {
			t.Fatalf("seed entries: %v", err)
		}

		page1, _ := store.ScanEntries(ctx, EntryQuery{ConversationID: convID, Order: OrderAscending}, 2, nil)
		if len(page1.Items) != 2 || page1.Items[0].ID != ids[0] || page1.Items[1].ID != ids[1] {
			t.Fatalf("asc page1 = %+v", page1.Items)
		}
		if page1.Next == nil {
			t.Fatal("expected more pages")
		}
		page2, _ := store.ScanEntries(ctx, EntryQuery{ConversationID: convID, Order: OrderAscending}, 2, page1.Next)
		if len(page2.Items) != 1 || page2.Items[0].ID != ids[2] || page2.Next != nil {
			t.Fatalf("asc page2 = %+v next=%v", page2.Items, page2.Next)
		}

		desc1, _ := store.ScanEntries(ctx, EntryQuery{ConversationID: convID, Order: OrderDescending}, 2, nil)
		if len(desc1.Items) != 2 || desc1.Items[0].ID != ids[2] || desc1.Items[1].ID != ids[1] {
			t.Fatalf("desc page1 = %+v", desc1.Items)
		}
		desc2, _ := store.ScanEntries(ctx, EntryQuery{ConversationID: convID, Order: OrderDescending}, 2, desc1.Next)
		if len(desc2.Items) != 1 || desc2.Items[0].ID != ids[0] {
			t.Fatalf("desc page2 = %+v", desc2.Items)
		}
	})

	t.Run("submissionByRequest dedup", func(t *testing.T) {
		convID, _ := store.MintID(ctx)
		if _, err := store.Commit(ctx, []StorageWrite{
			{Type: WriteConversation, Value: &ConversationRecord{ID: convID}},
			{Type: WriteSubmission, Value: &SubmissionRecord{ID: mustID(t, store, ctx), ConversationID: convID,
				RequestID: "job-1", Type: SubmissionInput, Status: SubmissionPlaced}},
		}); err != nil {
			t.Fatalf("seed: %v", err)
		}
		sub, _ := store.SubmissionByRequest(ctx, "job-1")
		if sub == nil || sub.Status != SubmissionPlaced {
			t.Fatalf("submissionByRequest = %+v", sub)
		}
		if missing, _ := store.SubmissionByRequest(ctx, "nope"); missing != nil {
			t.Fatalf("unexpected hit: %+v", missing)
		}
	})

	t.Run("clone on read", func(t *testing.T) {
		entryID, _ := store.MintID(ctx)
		if _, err := store.Commit(ctx, []StorageWrite{
			{Type: WriteEntry, Value: &EntryRecord{ID: entryID, ConversationID: 1, Kind: "user",
				Model: []agent.Message{{Role: agent.RoleUser, Content: "original"}}}},
		}); err != nil {
			t.Fatalf("seed: %v", err)
		}
		e, _ := store.Entry(ctx, entryID)
		e.Model[0].Content = "mutated"
		again, _ := store.Entry(ctx, entryID)
		if again.Model[0].Content != "original" {
			t.Fatalf("read returned aliased storage: %q", again.Model[0].Content)
		}
	})

	store = reopen(t, store)

	t.Run("state survives reopen and mint continues", func(t *testing.T) {
		before, _ := store.MintID(ctx)
		id, _ := store.MintID(ctx)
		if id <= before {
			t.Fatalf("mint not increasing after reopen: %d then %d", before, id)
		}
		if _, err := store.Commit(ctx, []StorageWrite{
			{Type: WriteEntry, Value: &EntryRecord{ID: id, ConversationID: 1, Kind: "user"}},
		}); err != nil {
			t.Fatalf("post-reopen commit: %v", err)
		}
	})
}

func TestConformanceMemory(t *testing.T) {
	runConformance(t,
		func(t *testing.T) Storage { return NewMemoryStorage() },
		func(t *testing.T, cur Storage) Storage { return cur },
	)
}

func TestConformanceJSONL(t *testing.T) {
	path := filepath.Join(t.TempDir(), "agent.jsonl")
	runConformance(t,
		func(t *testing.T) Storage {
			s, err := OpenJSONL(path, false)
			if err != nil {
				t.Fatal(err)
			}
			return s
		},
		func(t *testing.T, cur Storage) Storage {
			if err := cur.Close(context.Background()); err != nil {
				t.Fatal(err)
			}
			s, err := OpenJSONL(path, false)
			if err != nil {
				t.Fatal(err)
			}
			return s
		},
	)
}

func mustID(t *testing.T, s Storage, ctx context.Context) ID {
	t.Helper()
	id, err := s.MintID(ctx)
	if err != nil {
		t.Fatal(err)
	}
	return id
}
