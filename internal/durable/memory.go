package durable

import "context"

// MemoryStorage is the reference Storage backend. It holds records in memory,
// clones every write and read, and is the semantics every other backend is
// tested against by the conformance suite.
type MemoryStorage struct {
	ix      *recordIndex
	nextID  ID
	nextSeq Seq
}

// NewMemoryStorage returns an empty memory backend. The root conversation ID
// (1) is reserved for the harness; minting starts above it.
func NewMemoryStorage() *MemoryStorage {
	return &MemoryStorage{ix: newRecordIndex(), nextID: 2}
}

func (m *MemoryStorage) Commit(ctx context.Context, writes []StorageWrite) (Seq, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	if err := m.ix.apply(writes); err != nil {
		return 0, err
	}
	m.nextSeq++
	return m.nextSeq, nil
}

func (m *MemoryStorage) MintID(ctx context.Context) (ID, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	id := m.nextID
	m.nextID++
	return id, nil
}

func (m *MemoryStorage) Conversation(ctx context.Context, id ID) (*ConversationRecord, error) {
	return m.ix.conversation(id), ctx.Err()
}

func (m *MemoryStorage) ScanConversations(ctx context.Context, q ConversationQuery, limit int, cursor *Cursor) (*Page[ConversationRecord], error) {
	return m.ix.scanConversations(q, limit, cursor), ctx.Err()
}

func (m *MemoryStorage) Entry(ctx context.Context, id ID) (*EntryRecord, error) {
	return m.ix.entry(id), ctx.Err()
}

func (m *MemoryStorage) ScanEntries(ctx context.Context, q EntryQuery, limit int, cursor *Cursor) (*Page[EntryRecord], error) {
	return m.ix.scanEntries(q, limit, cursor), ctx.Err()
}

func (m *MemoryStorage) Task(ctx context.Context, id ID) (*TaskRecord, error) {
	return m.ix.task(id), ctx.Err()
}

func (m *MemoryStorage) ScanTasks(ctx context.Context, q TaskQuery, limit int, cursor *Cursor) (*Page[TaskRecord], error) {
	return m.ix.scanTasks(q, limit, cursor), ctx.Err()
}

func (m *MemoryStorage) Submission(ctx context.Context, id ID) (*SubmissionRecord, error) {
	return m.ix.submission(id), ctx.Err()
}

func (m *MemoryStorage) ScanSubmissions(ctx context.Context, q SubmissionQuery, limit int, cursor *Cursor) (*Page[SubmissionRecord], error) {
	return m.ix.scanSubmissions(q, limit, cursor), ctx.Err()
}

func (m *MemoryStorage) SubmissionByRequest(ctx context.Context, requestID string) (*SubmissionRecord, error) {
	return m.ix.submissionByRequest(requestID), ctx.Err()
}

func (m *MemoryStorage) Close(ctx context.Context) error { return ctx.Err() }
