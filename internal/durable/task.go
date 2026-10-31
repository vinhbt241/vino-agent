package durable

import (
	"context"
	"encoding/json"
	"errors"
)

// NextState is the durable state a task step commits: a replacement
// checkpoint, a wait on child tasks, or a terminal outcome. Settle carries a
// submission settlement the scheduler applies atomically with the commit.
type NextState struct {
	Status     TaskStatus
	Checkpoint json.RawMessage
	On         []ID
	Outcome    *Outcome
	Settle     *SettleIntent
}

// TaskDefinition describes a durable state machine. Checkpoints are opaque
// JSON owned by the definition; Step runs one phase toward the next state.
type TaskDefinition interface {
	Kind() string
	Version() int
	// Initial returns the checkpoint a freshly created task starts from.
	Initial(input json.RawMessage) json.RawMessage
	// Step runs one phase. It may accumulate writes (entries, child tasks)
	// through rt; those writes commit atomically with the returned state.
	Step(ctx context.Context, checkpoint json.RawMessage, rt *TaskRuntime) (NextState, error)
}

// TaskRuntime is the capability a step uses to make durable progress. It is
// valid for the duration of one Step call.
type TaskRuntime struct {
	session *Session
	harness *Harness
	task    *TaskRecord
	// ctx is the durable context for storage operations. It is detached from
	// the request lifetime so a client cancel can abort execution (the Step's
	// own ctx) without preventing the run from persisting its settled state.
	ctx    context.Context
	writes []StorageWrite
	// docs tracks documents staged this step by name, so a second SetDocument
	// for the same name replaces the staged value instead of minting a second
	// ID and tripping name-uniqueness validation.
	docs map[string]*DocumentRecord
	// memos holds memos staged this step by key (first-writer-wins against the
	// committed rt.task.Memos); it merges into the step's checkpoint commit.
	memos map[string]json.RawMessage
}

func (rt *TaskRuntime) TaskID() ID             { return rt.task.ID }
func (rt *TaskRuntime) ConversationID() ID     { return rt.task.ConversationID }
func (rt *TaskRuntime) Input() json.RawMessage { return rt.task.Input }
func (rt *TaskRuntime) Restarts() int          { return rt.task.Restarts }
func (rt *TaskRuntime) Harness() *Harness      { return rt.harness }
func (rt *TaskRuntime) Ctx() context.Context   { return rt.ctx }
func (rt *TaskRuntime) OwnerID() *ID           { return rt.task.OwnerID }
func (rt *TaskRuntime) AbortRequested() bool   { return rt.task.AbortRequested }

func (rt *TaskRuntime) MintID() (ID, error) {
	return rt.session.storage.MintID(rt.ctx)
}

func (rt *TaskRuntime) Entry(id ID) (*EntryRecord, error) {
	return rt.session.storage.Entry(rt.ctx, id)
}

func (rt *TaskRuntime) Conversation(id ID) (*ConversationRecord, error) {
	return rt.session.storage.Conversation(rt.ctx, id)
}

func (rt *TaskRuntime) Task(id ID) (*TaskRecord, error) {
	return rt.session.storage.Task(rt.ctx, id)
}

func (rt *TaskRuntime) Submission(id ID) (*SubmissionRecord, error) {
	return rt.session.storage.Submission(rt.ctx, id)
}

func (rt *TaskRuntime) ScanEntries(q EntryQuery) (*Page[EntryRecord], error) {
	return rt.session.storage.ScanEntries(rt.ctx, q, 0, nil)
}

// Document returns a document's committed value, or its definition's default
// when this conversation has never written it. The name must be declared.
func (rt *TaskRuntime) Document(name string) (json.RawMessage, error) {
	def, ok := rt.harness.documentDef(name)
	if !ok {
		return nil, ErrDocumentUndeclared
	}
	rec, err := rt.session.storage.DocumentByName(rt.ctx, rt.task.ConversationID, name)
	if err != nil {
		return nil, err
	}
	if rec != nil {
		return rec.Value, nil
	}
	return append(json.RawMessage(nil), def.Default...), nil
}

// SetDocument stages a full-value replacement of a named document for the
// step's atomic commit. The name must be declared; the existing record (if
// any) is replaced in place, otherwise a new record is minted. A later
// SetDocument for the same name in the same step replaces the staged value
// (last write wins). The staged value is not visible to Document until the
// step's commit lands.
func (rt *TaskRuntime) SetDocument(name string, value json.RawMessage) error {
	if _, ok := rt.harness.documentDef(name); !ok {
		return ErrDocumentUndeclared
	}
	if rec, ok := rt.docs[name]; ok {
		rec.Value = value
		return nil
	}
	rec, err := rt.session.storage.DocumentByName(rt.ctx, rt.task.ConversationID, name)
	if err != nil {
		return err
	}
	if rec == nil {
		id, err := rt.MintID()
		if err != nil {
			return err
		}
		rec = &DocumentRecord{ID: id, ConversationID: rt.task.ConversationID, Name: name}
	}
	rec.Value = value
	rt.docs[name] = rec
	rt.AppendDocument(rec)
	return nil
}

// Memo stages a small first-writer-wins value in the task envelope, committed
// atomically with the step's checkpoint. It returns true only when this call
// staged the first value for key (absent from committed Memos and from this
// step's staged memos), so a task or hook that re-runs after a crash can
// branch on it instead of re-asking. The error return is reserved (nil today).
func (rt *TaskRuntime) Memo(key string, value json.RawMessage) (bool, error) {
	if key == "" {
		return false, errors.New("durable: empty memo key")
	}
	if _, ok := rt.task.Memos[key]; ok {
		return false, nil
	}
	if _, ok := rt.memos[key]; ok {
		return false, nil
	}
	rt.memos[key] = value
	return true, nil
}

// MemoValue returns a memo's committed value, or nil when absent. Memos
// staged this step are not visible until the commit lands.
func (rt *TaskRuntime) MemoValue(key string) json.RawMessage {
	return rt.task.Memos[key]
}

// AppendDocument stages a document write for the step's atomic commit.
func (rt *TaskRuntime) AppendDocument(rec *DocumentRecord) {
	rt.writes = append(rt.writes, StorageWrite{Type: WriteDocument, Value: rec})
}

// AppendEntry stages an entry write for the step's atomic commit.
func (rt *TaskRuntime) AppendEntry(rec *EntryRecord) {
	rt.writes = append(rt.writes, StorageWrite{Type: WriteEntry, Value: rec})
}

// AppendTask stages a child task creation for the step's atomic commit.
func (rt *TaskRuntime) AppendTask(rec *TaskRecord) {
	rt.writes = append(rt.writes, StorageWrite{Type: WriteTask, Value: rec})
}

// AppendSubmission stages a submission update for the step's atomic commit.
func (rt *TaskRuntime) AppendSubmission(rec *SubmissionRecord) {
	rt.writes = append(rt.writes, StorageWrite{Type: WriteSubmission, Value: rec})
}

// markAbortCascade stages abort marks for the whole foreground ownership tree
// the current task belongs to: it walks up OwnerID to the topmost foreground
// owner, then walks down marking every live, non-background descendant. The
// marks commit atomically with the step's own outcome, so a crash between the
// abort and a descendant's settle does not re-run that descendant's effect.
func (rt *TaskRuntime) markAbortCascade() {
	root := rt.task.ID
	for {
		t, err := rt.Task(root)
		if err != nil || t == nil || t.OwnerID == nil {
			break
		}
		parent, err := rt.Task(*t.OwnerID)
		if err != nil || parent == nil || parent.Background {
			break
		}
		root = *t.OwnerID
	}
	rt.markTree(root)
}

// markTree marks a task and its live foreground descendants as aborted,
// skipping background subtrees (which ordinary abort leaves alone) and tasks
// already marked or terminal. The current task handles its own settle, so it
// is skipped.
func (rt *TaskRuntime) markTree(id ID) {
	t, err := rt.Task(id)
	if err != nil || t == nil {
		return
	}
	if t.State.Status == TaskTerminal || t.State.Status == TaskCompleting {
		return
	}
	if t.Background {
		return // background tasks and everything below them are not reached
	}
	if id != rt.task.ID && !t.AbortRequested {
		t.AbortRequested = true
		rt.AppendTask(t)
	}
	page, err := rt.session.storage.ScanTasks(rt.ctx, TaskQuery{OwnerID: &id}, 0, nil)
	if err != nil {
		return
	}
	for _, child := range page.Items {
		rt.markTree(child.ID)
	}
}

func mustJSON(v any) json.RawMessage {
	b, err := json.Marshal(v)
	if err != nil {
		panic(err)
	}
	return b
}
