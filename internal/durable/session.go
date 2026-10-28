package durable

import (
	"context"
	"errors"
	"sync"
)

var (
	// ErrTaskNotFound is returned when a runtime operation targets a task
	// that does not exist.
	ErrTaskNotFound = errors.New("durable: task not found")
	// ErrTaskTerminal is returned when a runtime operation targets a task
	// that is already terminal.
	ErrTaskTerminal = errors.New("durable: task is terminal")
	// ErrConversationNotFound is returned when a runtime operation targets a
	// conversation that does not exist.
	ErrConversationNotFound = errors.New("durable: conversation not found")
)

// Session owns the mutation line: one writer at a time, atomic commits across
// all records. A storage error is fatal to the session; the host reopens the
// storage to recover from the last consistent commit.
type Session struct {
	storage Storage
	mu      sync.Mutex
}

// OpenSession opens a session over a storage backend.
func OpenSession(storage Storage) *Session {
	return &Session{storage: storage}
}

// Storage returns the backend the session writes through.
func (s *Session) Storage() Storage { return s.storage }

// Commit serializes fn on the mutation line and commits its accumulated writes
// atomically. Reads inside fn see committed state only.
func (s *Session) Commit(ctx context.Context, fn func(tx *Tx) error) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	tx := &Tx{session: s}
	if err := fn(tx); err != nil {
		return err
	}
	return s.commitLocked(ctx, tx.writes)
}

// CommitWrites commits a pre-built batch atomically on the mutation line.
func (s *Session) CommitWrites(ctx context.Context, writes []StorageWrite) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.commitLocked(ctx, writes)
}

func (s *Session) commitLocked(ctx context.Context, writes []StorageWrite) error {
	if len(writes) == 0 {
		return nil
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	_, err := s.storage.Commit(ctx, writes)
	return err
}

// Tx accumulates writes for one commit and reads committed state.
type Tx struct {
	session *Session
	writes  []StorageWrite
}

func (tx *Tx) MintID(ctx context.Context) (ID, error) {
	return tx.session.storage.MintID(ctx)
}

func (tx *Tx) Conversation(ctx context.Context, id ID) (*ConversationRecord, error) {
	return tx.session.storage.Conversation(ctx, id)
}

func (tx *Tx) Entry(ctx context.Context, id ID) (*EntryRecord, error) {
	return tx.session.storage.Entry(ctx, id)
}

func (tx *Tx) Task(ctx context.Context, id ID) (*TaskRecord, error) {
	return tx.session.storage.Task(ctx, id)
}

func (tx *Tx) Submission(ctx context.Context, id ID) (*SubmissionRecord, error) {
	return tx.session.storage.Submission(ctx, id)
}

func (tx *Tx) SubmissionByRequest(ctx context.Context, requestID string) (*SubmissionRecord, error) {
	return tx.session.storage.SubmissionByRequest(ctx, requestID)
}

// ScanTasks reads all tasks matching q (limit 0 means no bound).
func (tx *Tx) ScanTasks(ctx context.Context, q TaskQuery) (*Page[TaskRecord], error) {
	return tx.session.storage.ScanTasks(ctx, q, 0, nil)
}

// ScanEntries reads all entries matching q (limit 0 means no bound).
func (tx *Tx) ScanEntries(ctx context.Context, q EntryQuery) (*Page[EntryRecord], error) {
	return tx.session.storage.ScanEntries(ctx, q, 0, nil)
}

// ScanConversations reads all conversations matching q (limit 0 means no bound).
func (tx *Tx) ScanConversations(ctx context.Context, q ConversationQuery) (*Page[ConversationRecord], error) {
	return tx.session.storage.ScanConversations(ctx, q, 0, nil)
}

func (tx *Tx) WriteConversation(rec *ConversationRecord) {
	tx.writes = append(tx.writes, StorageWrite{Type: WriteConversation, Value: rec})
}

func (tx *Tx) WriteEntry(rec *EntryRecord) {
	tx.writes = append(tx.writes, StorageWrite{Type: WriteEntry, Value: rec})
}

func (tx *Tx) WriteTask(rec *TaskRecord) {
	tx.writes = append(tx.writes, StorageWrite{Type: WriteTask, Value: rec})
}

func (tx *Tx) WriteSubmission(rec *SubmissionRecord) {
	tx.writes = append(tx.writes, StorageWrite{Type: WriteSubmission, Value: rec})
}
