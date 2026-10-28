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
	writes  []StorageWrite
}

func (rt *TaskRuntime) TaskID() ID             { return rt.task.ID }
func (rt *TaskRuntime) ConversationID() ID     { return rt.task.ConversationID }
func (rt *TaskRuntime) Input() json.RawMessage { return rt.task.Input }
func (rt *TaskRuntime) Restarts() int          { return rt.task.Restarts }
func (rt *TaskRuntime) Harness() *Harness      { return rt.harness }

func (rt *TaskRuntime) MintID(ctx context.Context) (ID, error) {
	return rt.session.storage.MintID(ctx)
}

func (rt *TaskRuntime) Entry(ctx context.Context, id ID) (*EntryRecord, error) {
	return rt.session.storage.Entry(ctx, id)
}

func (rt *TaskRuntime) Task(ctx context.Context, id ID) (*TaskRecord, error) {
	return rt.session.storage.Task(ctx, id)
}

func (rt *TaskRuntime) Submission(ctx context.Context, id ID) (*SubmissionRecord, error) {
	return rt.session.storage.Submission(ctx, id)
}

func (rt *TaskRuntime) ScanEntries(ctx context.Context, q EntryQuery) (*Page[EntryRecord], error) {
	return rt.session.storage.ScanEntries(ctx, q, 0, nil)
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

func mustJSON(v any) json.RawMessage {
	b, err := json.Marshal(v)
	if err != nil {
		panic(err)
	}
	return b
}
