package durable

import "context"

// StorageWriteType discriminates a StorageWrite.
type StorageWriteType string

const (
	WriteConversation StorageWriteType = "conversation"
	WriteEntry        StorageWriteType = "entry"
	WriteTask         StorageWriteType = "task"
	WriteSubmission   StorageWriteType = "submission"
)

// StorageWrite is one record write in a commit batch. Value is a pointer to
// the record type matching Type.
type StorageWrite struct {
	Type  StorageWriteType
	Value any
}

// ScanOrder is the direction of an ID-ordered scan.
type ScanOrder string

const (
	OrderAscending  ScanOrder = "ascending"
	OrderDescending ScanOrder = "descending"
)

// Cursor continues an ID-ordered scan. It is a JSON-marshalable last-seen ID;
// nil starts at the beginning (or end, for a descending scan).
type Cursor struct {
	LastID ID `json:"lastId"`
}

// Page is one scan result page. Next is nil when the scan is exhausted.
type Page[T any] struct {
	Items []T
	Next  *Cursor
}

// ConversationQuery filters a conversation scan.
type ConversationQuery struct {
	OwnerConversationID *ID
	OwnerTaskID         *ID
	// ExcludeRoot drops the harness's singleton root conversation from the
	// scan. It is an internal bookkeeping conversation, not a user one.
	ExcludeRoot bool
	Order       ScanOrder
}

// EntryQuery filters an entry scan. MinID/MaxID are inclusive.
type EntryQuery struct {
	ConversationID ID
	MinID          *ID
	MaxID          *ID
	Order          ScanOrder
}

// TaskQuery filters a task scan.
type TaskQuery struct {
	ConversationID *ID
	Kind           string
	Status         TaskStatus
	Background     *bool
	Order          ScanOrder
}

// SubmissionQuery filters a submission scan.
type SubmissionQuery struct {
	ConversationID *ID
	Status         SubmissionStatus
	Order          ScanOrder
}

// Storage persists records atomically per commit and mints IDs from one global
// namespace. Implementations are trusted to receive semantically valid writes;
// the Session serializes commits. A successful Commit means the batch is
// durable and published; a failed Commit publishes nothing.
type Storage interface {
	// Commit persists the batch atomically and returns its sequence.
	Commit(ctx context.Context, writes []StorageWrite) (Seq, error)
	// MintID allocates from the one global numeric namespace, above every ID
	// stored, also across reopen. IDs order as creation order.
	MintID(ctx context.Context) (ID, error)

	Conversation(ctx context.Context, id ID) (*ConversationRecord, error)
	ScanConversations(ctx context.Context, q ConversationQuery, limit int, cursor *Cursor) (*Page[ConversationRecord], error)

	Entry(ctx context.Context, id ID) (*EntryRecord, error)
	ScanEntries(ctx context.Context, q EntryQuery, limit int, cursor *Cursor) (*Page[EntryRecord], error)

	Task(ctx context.Context, id ID) (*TaskRecord, error)
	ScanTasks(ctx context.Context, q TaskQuery, limit int, cursor *Cursor) (*Page[TaskRecord], error)

	Submission(ctx context.Context, id ID) (*SubmissionRecord, error)
	ScanSubmissions(ctx context.Context, q SubmissionQuery, limit int, cursor *Cursor) (*Page[SubmissionRecord], error)
	// SubmissionByRequest returns the submission deduplicated by request ID,
	// server-wide, so a crash retry finds the original regardless of which
	// conversation it was admitted into.
	SubmissionByRequest(ctx context.Context, requestID string) (*SubmissionRecord, error)

	Close(ctx context.Context) error
}
