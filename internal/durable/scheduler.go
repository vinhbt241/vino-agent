package durable

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"sync"
)

// Scheduler reserves pending tasks, invokes their definitions off the mutation
// line, commits each step's durable progress, and reconciles running tasks
// back to pending on open. It schedules per conversation: runs in different
// conversations proceed concurrently, runs in the same conversation serialize.
type Scheduler struct {
	session *Session
	harness *Harness
	defs    map[string]TaskDefinition

	convMu    sync.Mutex
	convLocks map[ID]*sync.Mutex
}

// NewScheduler builds a scheduler over a session for the given definitions.
func NewScheduler(session *Session, harness *Harness, defs []TaskDefinition) *Scheduler {
	m := make(map[string]TaskDefinition, len(defs))
	for _, d := range defs {
		m[d.Kind()] = d
	}
	return &Scheduler{session: session, harness: harness, defs: m, convLocks: make(map[ID]*sync.Mutex)}
}

// lockFor returns the mutex serializing a conversation's runs, creating it on
// first use.
func (sch *Scheduler) lockFor(id ID) *sync.Mutex {
	sch.convMu.Lock()
	defer sch.convMu.Unlock()
	if l, ok := sch.convLocks[id]; ok {
		return l
	}
	l := &sync.Mutex{}
	sch.convLocks[id] = l
	return l
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

// RunConversation drives one conversation's tasks to idle, serializing with
// other runs of the same conversation and running concurrently with other
// conversations. Execution ctx stays cancellable so a client cancel (or
// Conversation.Abort) aborts tools and model calls; background tasks run under
// a detached ctx so a foreground cancel leaves them alone. The storage ctx is
// detached so the scheduler still commits settled state after the request is
// gone.
func (sch *Scheduler) RunConversation(ctx context.Context, conversationID ID) error {
	lock := sch.lockFor(conversationID)
	lock.Lock()
	defer lock.Unlock()

	execCtx, cancel := context.WithCancel(ctx)
	sch.harness.registerAbort(conversationID, cancel)
	defer func() {
		cancel()
		sch.harness.unregisterAbort(conversationID)
	}()

	durableCtx := context.WithoutCancel(ctx)
	for {
		if err := sch.resumeWaiting(durableCtx, conversationID); err != nil {
			return err
		}
		task, err := sch.nextPending(durableCtx, conversationID)
		if err != nil {
			return err
		}
		if task == nil {
			return nil
		}
		taskCtx := execCtx
		if task.Background {
			taskCtx = context.WithoutCancel(ctx)
		}
		if err := sch.runOne(taskCtx, durableCtx, task); err != nil {
			return err
		}
	}
}

// RunAll drives every conversation with work to idle, running different
// conversations concurrently and waiting for all of them.
func (sch *Scheduler) RunAll(ctx context.Context) error {
	page, err := sch.session.storage.ScanTasks(ctx, TaskQuery{}, 0, nil)
	if err != nil {
		return err
	}
	convs := make(map[ID]bool)
	for _, t := range page.Items {
		if t.State.Status != TaskTerminal {
			convs[t.ConversationID] = true
		}
	}

	var wg sync.WaitGroup
	var mu sync.Mutex
	var firstErr error
	for id := range convs {
		wg.Add(1)
		go func(id ID) {
			defer wg.Done()
			if err := sch.RunConversation(ctx, id); err != nil {
				mu.Lock()
				if firstErr == nil {
					firstErr = err
				}
				mu.Unlock()
			}
		}(id)
	}
	wg.Wait()
	return firstErr
}

func (sch *Scheduler) nextPending(ctx context.Context, conversationID ID) (*TaskRecord, error) {
	page, err := sch.session.storage.ScanTasks(ctx, TaskQuery{ConversationID: &conversationID, Status: TaskPending, Order: OrderAscending}, 1, nil)
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
func (sch *Scheduler) resumeWaiting(ctx context.Context, conversationID ID) error {
	return sch.session.Commit(ctx, func(tx *Tx) error {
		page, err := tx.ScanTasks(ctx, TaskQuery{Status: TaskWaiting, ConversationID: &conversationID})
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
// terminal or waiting state. execCtx aborts execution on cancel; durableCtx
// persists progress.
func (sch *Scheduler) runOne(execCtx, durableCtx context.Context, task *TaskRecord) error {
	def := sch.defs[task.Kind]
	if def == nil {
		return sch.fault(durableCtx, task, "missing task definition "+task.Kind)
	}

	// A task cut off restartLimit times ends at reservation: built-in kinds
	// end themselves in their phase (nicer result), any other kind is faulted
	// here before its phase runs.
	if !sch.limitsRestarts(task.Kind) && task.Restarts >= sch.harness.restartLimit {
		return sch.fault(durableCtx, task, fmt.Sprintf("Task %d (%s) was cut off %d times without progress", task.ID, task.Kind, task.Restarts))
	}

	// Reserve: pending -> running, checkpoint preserved. This is the durable
	// intent that a crash re-invokes.
	if err := sch.commitState(durableCtx, task, TaskRunning, task.State.Checkpoint, nil, nil, nil, nil); err != nil {
		return err
	}

	for {
		cur, err := sch.session.storage.Task(durableCtx, task.ID)
		if err != nil {
			return err
		}
		if cur == nil || cur.State.Status == TaskTerminal || cur.State.Status == TaskCompleting {
			return nil
		}

		rt := &TaskRuntime{session: sch.session, harness: sch.harness, task: cur, ctx: durableCtx, docs: make(map[string]*DocumentRecord), memos: make(map[string]json.RawMessage)}
		next, err := def.Step(execCtx, cur.State.Checkpoint, rt)
		if err != nil {
			return sch.fault(durableCtx, cur, err.Error())
		}

		writes := append([]StorageWrite(nil), rt.writes...)
		if err := sch.commitState(durableCtx, cur, next.Status, next.Checkpoint, next.On, next.Outcome, rt.memos, next.Settle, writes...); err != nil {
			return err
		}

		switch next.Status {
		case TaskTerminal, TaskCompleting, TaskWaiting:
			return nil
		case TaskRunning, TaskPending:
			if noProgress(cur.State.Checkpoint, next.Checkpoint, rt.writes) {
				return sch.fault(durableCtx, cur, "task step made no durable progress")
			}
			continue
		default:
			return sch.fault(durableCtx, cur, "unknown next status "+string(next.Status))
		}
	}
}

func noProgress(oldCheckpoint, newCheckpoint []byte, writes []StorageWrite) bool {
	return len(writes) == 0 && bytes.Equal(oldCheckpoint, newCheckpoint)
}

// commitState replaces a task's state atomically with any accumulated writes
// and staged memos.
func (sch *Scheduler) commitState(ctx context.Context, cur *TaskRecord, status TaskStatus, checkpoint json.RawMessage, on []ID, outcome *Outcome, memos map[string]json.RawMessage, settle *SettleIntent, writes ...StorageWrite) error {
	updated := *cur
	updated.State = TaskState{Status: status, Checkpoint: checkpoint, On: on, Outcome: outcome}
	// Real progress (a new checkpoint, a wait, or an outcome) removes the
	// crash count, so the limit is "in a row"; the reservation commit
	// (pending -> running, same checkpoint) is not progress.
	if isProgress(cur, status, checkpoint) {
		updated.Restarts = 0
	}
	if len(memos) > 0 {
		nm := make(map[string]json.RawMessage, len(updated.Memos)+len(memos))
		for k, v := range updated.Memos {
			nm[k] = v
		}
		for k, v := range memos {
			nm[k] = v
		}
		updated.Memos = nm
	}
	all := append(append([]StorageWrite(nil), writes...), StorageWrite{Type: WriteTask, Value: &updated})
	return sch.session.Commit(ctx, func(tx *Tx) error {
		for _, w := range all {
			tx.write(w)
		}
		if settle != nil {
			return sch.harness.settlement.ApplyDone(ctx, tx, settle.SubmissionID, settle.AnswerID)
		}
		return nil
	})
}

// isProgress reports whether a committed state counts as durable progress: a
// changed checkpoint, or a transition to waiting/completing/terminal.
func isProgress(cur *TaskRecord, status TaskStatus, checkpoint json.RawMessage) bool {
	if !bytes.Equal(cur.State.Checkpoint, checkpoint) {
		return true
	}
	switch status {
	case TaskWaiting, TaskCompleting, TaskTerminal:
		return true
	}
	return false
}

// limitsRestarts reports whether a task kind ends itself once cut off
// restartLimit times (the built-in tasks); the scheduler faults any other kind
// at reservation instead.
func (sch *Scheduler) limitsRestarts(kind string) bool {
	switch kind {
	case kindGeneration, kindToolTask:
		return true
	}
	return false
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
