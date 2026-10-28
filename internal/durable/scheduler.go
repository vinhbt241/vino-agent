package durable

import (
	"bytes"
	"context"
	"encoding/json"
)

// Scheduler reserves pending tasks, invokes their definitions off the mutation
// line, commits each step's durable progress, and reconciles running tasks
// back to pending on open. Slice 1 runs tasks serially.
type Scheduler struct {
	session *Session
	harness *Harness
	defs    map[string]TaskDefinition
}

// NewScheduler builds a scheduler over a session for the given definitions.
func NewScheduler(session *Session, harness *Harness, defs []TaskDefinition) *Scheduler {
	m := make(map[string]TaskDefinition, len(defs))
	for _, d := range defs {
		m[d.Kind()] = d
	}
	return &Scheduler{session: session, harness: harness, defs: m}
}

// Reconcile marks every surviving running task as pending and counts the
// crash. Called once on open before any task runs.
func (sch *Scheduler) Reconcile(ctx context.Context) error {
	return sch.session.Commit(ctx, func(tx *Tx) error {
		page, err := tx.ScanTasks(ctx, TaskQuery{Status: TaskRunning})
		if err != nil {
			return err
		}
		for i := range page.Items {
			t := page.Items[i]
			t.State.Status = TaskPending
			t.Restarts++
			tx.WriteTask(&t)
		}
		return nil
	})
}

// Run drives every pending task to a terminal or waiting state, resuming
// waiting tasks whose child tasks have finished. It returns when the harness
// is idle.
func (sch *Scheduler) Run(ctx context.Context) error {
	for {
		if err := sch.resumeWaiting(ctx); err != nil {
			return err
		}
		task, err := sch.nextPending(ctx)
		if err != nil {
			return err
		}
		if task == nil {
			return nil
		}
		if err := sch.runOne(ctx, task); err != nil {
			return err
		}
	}
}

func (sch *Scheduler) nextPending(ctx context.Context) (*TaskRecord, error) {
	page, err := sch.session.storage.ScanTasks(ctx, TaskQuery{Status: TaskPending, Order: OrderAscending}, 1, nil)
	if err != nil {
		return nil, err
	}
	if len(page.Items) == 0 {
		return nil, nil
	}
	t := page.Items[0]
	return &t, nil
}

// resumeWaiting flips a waiting task back to pending once every task it waits
// on is terminal.
func (sch *Scheduler) resumeWaiting(ctx context.Context) error {
	return sch.session.Commit(ctx, func(tx *Tx) error {
		page, err := tx.ScanTasks(ctx, TaskQuery{Status: TaskWaiting})
		if err != nil {
			return err
		}
		for i := range page.Items {
			t := page.Items[i]
			done, err := allTerminal(ctx, tx, t.State.On)
			if err != nil {
				return err
			}
			if done {
				t.State.Status = TaskPending
				tx.WriteTask(&t)
			}
		}
		return nil
	})
}

func allTerminal(ctx context.Context, tx *Tx, ids []ID) (bool, error) {
	for _, id := range ids {
		t, err := tx.Task(ctx, id)
		if err != nil {
			return false, err
		}
		if t == nil || (t.State.Status != TaskTerminal && t.State.Status != TaskCompleting) {
			return false, nil
		}
	}
	return true, nil
}

// runOne reserves a pending task, then repeatedly steps it until it reaches a
// terminal or waiting state.
func (sch *Scheduler) runOne(ctx context.Context, task *TaskRecord) error {
	def := sch.defs[task.Kind]
	if def == nil {
		return sch.fault(ctx, task, "missing task definition "+task.Kind)
	}

	// Reserve: pending -> running, checkpoint preserved. This is the durable
	// intent that a crash re-invokes.
	if err := sch.commitState(ctx, task, TaskRunning, task.State.Checkpoint, nil, nil); err != nil {
		return err
	}

	for {
		cur, err := sch.session.storage.Task(ctx, task.ID)
		if err != nil {
			return err
		}
		if cur == nil || cur.State.Status == TaskTerminal || cur.State.Status == TaskCompleting {
			return nil
		}

		rt := &TaskRuntime{session: sch.session, harness: sch.harness, task: cur}
		next, err := def.Step(ctx, cur.State.Checkpoint, rt)
		if err != nil {
			return sch.fault(ctx, cur, err.Error())
		}

		writes := append([]StorageWrite(nil), rt.writes...)
		if err := sch.commitState(ctx, cur, next.Status, next.Checkpoint, next.On, next.Outcome, writes...); err != nil {
			return err
		}

		switch next.Status {
		case TaskTerminal, TaskCompleting, TaskWaiting:
			return nil
		case TaskRunning, TaskPending:
			if noProgress(cur.State.Checkpoint, next.Checkpoint, rt.writes) {
				return sch.fault(ctx, cur, "task step made no durable progress")
			}
			continue
		default:
			return sch.fault(ctx, cur, "unknown next status "+string(next.Status))
		}
	}
}

func noProgress(oldCheckpoint, newCheckpoint []byte, writes []StorageWrite) bool {
	return len(writes) == 0 && bytes.Equal(oldCheckpoint, newCheckpoint)
}

// commitState replaces a task's state atomically with any accumulated writes.
func (sch *Scheduler) commitState(ctx context.Context, cur *TaskRecord, status TaskStatus, checkpoint json.RawMessage, on []ID, outcome *Outcome, writes ...StorageWrite) error {
	updated := *cur
	updated.State = TaskState{Status: status, Checkpoint: checkpoint, On: on, Outcome: outcome}
	writes = append(writes, StorageWrite{Type: WriteTask, Value: &updated})
	return sch.session.CommitWrites(ctx, writes)
}

// fault writes a terminal faulted outcome, preserving the task record, then
// lets the harness settle any submission the task was answering.
func (sch *Scheduler) fault(ctx context.Context, cur *TaskRecord, msg string) error {
	updated := *cur
	updated.State = TaskState{
		Status:  TaskTerminal,
		Outcome: &Outcome{Status: OutcomeFaulted, Error: &OutcomeError{Message: msg}},
	}
	if err := sch.session.CommitWrites(ctx, []StorageWrite{{Type: WriteTask, Value: &updated}}); err != nil {
		return err
	}
	sch.harness.onTaskFault(ctx, cur, msg)
	return nil
}
