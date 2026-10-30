package durable

import (
	"context"
	"encoding/json"
)

// NextState is the durable state a task step commits: a replacement
// checkpoint, a wait on child tasks, or a terminal outcome.
type NextState struct {
	Status     TaskStatus
	Checkpoint json.RawMessage
	On         []ID
	Outcome    *Outcome
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
