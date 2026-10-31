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
	// ErrDocumentUndeclared is returned when a document name has no
	// DocumentDefinition in the harness options.
	ErrDocumentUndeclared = errors.New("durable: document is not declared")
)

// Session owns the mutation line: one writer at a time, atomic commits across
// all records. A storage error is fatal to the session; the host reopens the
// storage to recover from the last consistent commit.
type Session struct {
	storage Storage
	mu      sync.Mutex
	lastSeq Seq
	// commits retains every commit since open (ascending Seq) for Watch
	// catch-up; the SQLite/working-set slice bounds this.
	commits []CommitOp
	// watchers are active commit watchers, drained asynchronously so a slow
	// watcher never stalls the mutation line.
	watchers []*commitWatcher
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
	seq, err := s.storage.Commit(ctx, writes)
	if err != nil {
		return err
	}
	s.lastSeq = seq
	op := CommitOp{Seq: seq, Writes: cloneWrites(writes)}
	s.commits = append(s.commits, op)
	s.publishLocked(op)
	return nil
}

// currentSeq returns the sequence of the most recent commit, under the
// mutation lock so a snapshot can pair it with committed reads.
func (s *Session) currentSeq() Seq {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.lastSeq
}

// watchBuffer is each commit watcher's buffered capacity; a watcher that
// cannot keep up is detached rather than stalling commits.
const watchBuffer = 64

// commitWatcher is one subscribed watcher: it receives commit ops matching a
// predicate, delivered asynchronously.
type commitWatcher struct {
	match func(StorageWrite) bool
	ch    chan CommitOp
	done  chan struct{}
	once  sync.Once
}

// Watch subscribes fn to commits after seq whose writes match. A non-nil fn
// return detaches the watcher. Catch-up (retained commits > seq) is delivered
// synchronously to the caller; live commits are delivered asynchronously. fn
// is never called under the mutation lock, so a slow watcher never stalls
// commits. Registration and catch-up selection happen under the mutation lock,
// so a commit can never slip between a snapshot and its subscription. The
// returned cancel is idempotent.
func (s *Session) Watch(after Seq, match func(StorageWrite) bool, fn func(CommitOp) error) func() {
	if match == nil {
		match = func(StorageWrite) bool { return true }
	}
	w := &commitWatcher{match: match, ch: make(chan CommitOp, watchBuffer), done: make(chan struct{})}

	s.mu.Lock()
	s.watchers = append(s.watchers, w)
	var backlog []CommitOp
	for _, c := range s.commits {
		if c.Seq <= after {
			continue
		}
		op := CommitOp{Seq: c.Seq, Writes: filterWrites(c.Writes, match)}
		if len(op.Writes) > 0 {
			backlog = append(backlog, op)
		}
	}
	s.mu.Unlock()

	// Deliver catch-up synchronously to the caller (the client). This never
	// holds the mutation lock, so a slow catch-up cannot stall commits.
	for _, op := range backlog {
		if err := fn(op); err != nil {
			s.mu.Lock()
			s.detachLocked(w)
			s.mu.Unlock()
			return func() {}
		}
	}

	// Live delivery drains the buffered channel asynchronously.
	go func() {
		for {
			select {
			case <-w.done:
				return
			case op := <-w.ch:
				if err := fn(op); err != nil {
					s.mu.Lock()
					s.detachLocked(w)
					s.mu.Unlock()
					return
				}
			}
		}
	}()

	return func() {
		s.mu.Lock()
		s.detachLocked(w)
		s.mu.Unlock()
	}
}

// publishLocked delivers one commit op to every watcher it matches, detaching
// any whose buffer is full. Caller holds s.mu.
func (s *Session) publishLocked(op CommitOp) {
	for i := 0; i < len(s.watchers); i++ {
		w := s.watchers[i]
		filtered := CommitOp{Seq: op.Seq, Writes: filterWrites(op.Writes, w.match)}
		if len(filtered.Writes) == 0 {
			continue
		}
		if !s.enqueueLocked(w, filtered) {
			s.detachLocked(w)
			i-- // the slice shrank; re-check the same index
		}
	}
}

// enqueueLocked sends op to the watcher's buffer without blocking. It reports
// false when the buffer is full (the watcher is too slow).
func (s *Session) enqueueLocked(w *commitWatcher, op CommitOp) bool {
	select {
	case w.ch <- op:
		return true
	default:
		return false
	}
}

// detachLocked removes the watcher and closes its done channel. Caller holds
// s.mu. Idempotent.
func (s *Session) detachLocked(w *commitWatcher) {
	for i := range s.watchers {
		if s.watchers[i] == w {
			s.watchers = append(s.watchers[:i], s.watchers[i+1:]...)
			break
		}
	}
	w.once.Do(func() { close(w.done) })
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

func (tx *Tx) Document(ctx context.Context, id ID) (*DocumentRecord, error) {
	return tx.session.storage.Document(ctx, id)
}

func (tx *Tx) DocumentByName(ctx context.Context, conversationID ID, name string) (*DocumentRecord, error) {
	return tx.session.storage.DocumentByName(ctx, conversationID, name)
}

// ScanDocuments reads all documents matching q (limit 0 means no bound).
func (tx *Tx) ScanDocuments(ctx context.Context, q DocumentQuery) (*Page[DocumentRecord], error) {
	return tx.session.storage.ScanDocuments(ctx, q, 0, nil)
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

// ScanSubmissions reads all submissions matching q (limit 0 means no bound).
func (tx *Tx) ScanSubmissions(ctx context.Context, q SubmissionQuery) (*Page[SubmissionRecord], error) {
	return tx.session.storage.ScanSubmissions(ctx, q, 0, nil)
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

func (tx *Tx) WriteDocument(rec *DocumentRecord) {
	tx.writes = append(tx.writes, StorageWrite{Type: WriteDocument, Value: rec})
}
