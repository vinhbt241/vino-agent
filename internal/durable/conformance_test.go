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
	var reopenDocID ID

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

	t.Run("document write replace scan clone", func(t *testing.T) {
		convID, _ := store.MintID(ctx)
		docID, _ := store.MintID(ctx)
		if _, err := store.Commit(ctx, []StorageWrite{
			{Type: WriteConversation, Value: &ConversationRecord{ID: convID}},
			{Type: WriteDocument, Value: &DocumentRecord{ID: docID, ConversationID: convID, Name: "plan", Value: json.RawMessage(`{"n":1}`)}},
		}); err != nil {
			t.Fatalf("seed document: %v", err)
		}
		reopenDocID = docID

		d, _ := store.Document(ctx, docID)
		if d == nil || d.Name != "plan" || string(d.Value) != `{"n":1}` {
			t.Fatalf("document = %+v", d)
		}
		bn, _ := store.DocumentByName(ctx, convID, "plan")
		if bn == nil || bn.ID != docID {
			t.Fatalf("documentByName = %+v", bn)
		}

		// Replace the same ID in place.
		if _, err := store.Commit(ctx, []StorageWrite{
			{Type: WriteDocument, Value: &DocumentRecord{ID: docID, ConversationID: convID, Name: "plan", Value: json.RawMessage(`{"n":2}`)}},
		}); err != nil {
			t.Fatalf("replace document: %v", err)
		}
		d, _ = store.Document(ctx, docID)
		if string(d.Value) != `{"n":2}` {
			t.Fatalf("document value = %s, want {\"n\":2}", d.Value)
		}

		// Name is unique per conversation: a second ID with the same name fails.
		otherID, _ := store.MintID(ctx)
		if _, err := store.Commit(ctx, []StorageWrite{
			{Type: WriteDocument, Value: &DocumentRecord{ID: otherID, ConversationID: convID, Name: "plan", Value: json.RawMessage(`{}`)}},
		}); err == nil {
			t.Fatal("expected duplicate document name to fail")
		}

		// ...and so does the same name under two IDs within one batch.
		idA, _ := store.MintID(ctx)
		idB, _ := store.MintID(ctx)
		if _, err := store.Commit(ctx, []StorageWrite{
			{Type: WriteDocument, Value: &DocumentRecord{ID: idA, ConversationID: convID, Name: "notes", Value: json.RawMessage(`1`)}},
			{Type: WriteDocument, Value: &DocumentRecord{ID: idB, ConversationID: convID, Name: "notes", Value: json.RawMessage(`2`)}},
		}); err == nil {
			t.Fatal("expected same-name document batch to fail")
		}

		// Reads clone: mutating the returned value must not reach storage.
		d, _ = store.Document(ctx, docID)
		d.Value = json.RawMessage(`{"mutated":true}`)
		again, _ := store.Document(ctx, docID)
		if string(again.Value) != `{"n":2}` {
			t.Fatalf("read returned aliased document value: %s", again.Value)
		}
	})

	t.Run("document scan filters and paginates", func(t *testing.T) {
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
			{Type: WriteDocument, Value: &DocumentRecord{ID: ids[0], ConversationID: convID, Name: "a", Value: json.RawMessage(`1`)}},
			{Type: WriteDocument, Value: &DocumentRecord{ID: ids[1], ConversationID: convID, Name: "b", Value: json.RawMessage(`2`)}},
			{Type: WriteDocument, Value: &DocumentRecord{ID: ids[2], ConversationID: convID, Name: "c", Value: json.RawMessage(`3`)}},
		}); err != nil {
			t.Fatalf("seed documents: %v", err)
		}

		page1, _ := store.ScanDocuments(ctx, DocumentQuery{ConversationID: &convID, Order: OrderAscending}, 2, nil)
		if len(page1.Items) != 2 || page1.Items[0].Name != "a" || page1.Items[1].Name != "b" || page1.Next == nil {
			t.Fatalf("asc page1 = %+v next=%v", page1.Items, page1.Next)
		}
		page2, _ := store.ScanDocuments(ctx, DocumentQuery{ConversationID: &convID, Order: OrderAscending}, 2, page1.Next)
		if len(page2.Items) != 1 || page2.Items[0].Name != "c" || page2.Next != nil {
			t.Fatalf("asc page2 = %+v next=%v", page2.Items, page2.Next)
		}

		named, _ := store.ScanDocuments(ctx, DocumentQuery{Name: "b", Order: OrderAscending}, 0, nil)
		if len(named.Items) != 1 || named.Items[0].Name != "b" {
			t.Fatalf("name-filtered scan = %+v", named.Items)
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
		if d, _ := store.Document(ctx, reopenDocID); d == nil || string(d.Value) != `{"n":2}` {
			t.Fatalf("document did not survive reopen: %+v", d)
		}
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

func TestConformanceSQLite(t *testing.T) {
	path := filepath.Join(t.TempDir(), "agent.db")
	runConformance(t,
		func(t *testing.T) Storage {
			s, err := OpenSQLite(path)
			if err != nil {
				t.Fatal(err)
			}
			return s
		},
		func(t *testing.T, cur Storage) Storage {
			if err := cur.Close(context.Background()); err != nil {
				t.Fatal(err)
			}
			s, err := OpenSQLite(path)
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
