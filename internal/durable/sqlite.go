package durable

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"

	_ "modernc.org/sqlite"
)

// SQLiteStorage persists records to a SQLite database. Reads are SQL
// statements; there is no application-level record index — SQLite's page
// cache is the working set.
type SQLiteStorage struct {
	db *sql.DB
}

// migration is one versioned schema step. Statements are single SQL
// statements (no ';' separators), applied inside one transaction.
type migration struct {
	version int
	sql     []string
}

// migrations is the ordered schema history. New schemas append here; existing
// databases are upgraded in place by running every migration above their
// current user_version.
var migrations = []migration{
	{version: 1, sql: schemaV1},
}

var schemaV1 = []string{
	`CREATE TABLE meta (
		id       INTEGER PRIMARY KEY CHECK (id = 1),
		next_id  INTEGER NOT NULL,
		last_seq INTEGER NOT NULL
	)`,
	`INSERT INTO meta (id, next_id, last_seq) VALUES (1, 2, 0)`,
	`CREATE TABLE conversations (
		id                    INTEGER PRIMARY KEY CHECK (id != 0),
		owner_conversation_id INTEGER,
		owner_task_id         INTEGER,
		body                  TEXT NOT NULL
	)`,
	`CREATE TABLE entries (
		id              INTEGER PRIMARY KEY CHECK (id != 0),
		conversation_id INTEGER NOT NULL,
		kind            TEXT NOT NULL,
		body            TEXT NOT NULL
	)`,
	`CREATE INDEX entries_by_conv ON entries (conversation_id, id)`,
	`CREATE TABLE tasks (
		id              INTEGER PRIMARY KEY CHECK (id != 0),
		conversation_id INTEGER NOT NULL,
		owner_id        INTEGER,
		kind            TEXT NOT NULL,
		status          TEXT NOT NULL,
		background      INTEGER NOT NULL DEFAULT 0,
		body            TEXT NOT NULL
	)`,
	`CREATE INDEX tasks_by_conv ON tasks (conversation_id, id)`,
	`CREATE INDEX tasks_by_status ON tasks (status, id)`,
	`CREATE TABLE submissions (
		id              INTEGER PRIMARY KEY CHECK (id != 0),
		conversation_id INTEGER NOT NULL,
		request_id      TEXT NOT NULL DEFAULT '',
		status          TEXT NOT NULL,
		body            TEXT NOT NULL
	)`,
	`CREATE INDEX submissions_by_conv ON submissions (conversation_id, id)`,
	`CREATE INDEX submissions_by_request ON submissions (request_id)`,
	`CREATE TABLE documents (
		id              INTEGER PRIMARY KEY CHECK (id != 0),
		conversation_id INTEGER NOT NULL CHECK (conversation_id != 0),
		name            TEXT NOT NULL CHECK (name != ''),
		body            TEXT NOT NULL,
		UNIQUE (conversation_id, name)
	)`,
	`CREATE INDEX documents_by_conv ON documents (conversation_id, id)`,
	`CREATE TRIGGER tasks_terminal_immutable
		BEFORE UPDATE ON tasks
		WHEN OLD.status = 'terminal'
		BEGIN
			SELECT RAISE(ABORT, 'durable: task is terminal and immutable');
		END`,
}

// sqliteDSN builds the driver DSN: WAL for reader/writer isolation, NORMAL
// synchronous for process-crash consistency (matching JSONL's fsync=false
// default), a busy timeout for lock contention, and immediate transactions so
// a stray second writer fails fast instead of deadlocking at commit.
func sqliteDSN(path string) string {
	u := &url.URL{Scheme: "file", Path: path}
	q := u.Query()
	q.Set("_journal_mode", "WAL")
	q.Set("_synchronous", "NORMAL")
	q.Set("_busy_timeout", "5000")
	q.Set("_txlock", "immediate")
	u.RawQuery = q.Encode()
	return u.String()
}

// OpenSQLite opens (creating and migrating if needed) the database at path.
func OpenSQLite(path string) (*SQLiteStorage, error) {
	db, err := sql.Open("sqlite", sqliteDSN(path))
	if err != nil {
		return nil, fmt.Errorf("durable: open sqlite: %w", err)
	}
	db.SetMaxOpenConns(4)
	s := &SQLiteStorage{db: db}
	if err := db.Ping(); err != nil {
		db.Close()
		return nil, fmt.Errorf("durable: open sqlite: %w", err)
	}
	if err := s.migrate(context.Background()); err != nil {
		db.Close()
		return nil, err
	}
	return s, nil
}

// migrate applies every migration above the database's current user_version,
// each in its own transaction with the version bump inside the same
// transaction.
func (s *SQLiteStorage) migrate(ctx context.Context) error {
	var v int
	if err := s.db.QueryRowContext(ctx, "PRAGMA user_version").Scan(&v); err != nil {
		return fmt.Errorf("durable: read schema version: %w", err)
	}
	for _, m := range migrations {
		if m.version <= v {
			continue
		}
		tx, err := s.db.BeginTx(ctx, nil)
		if err != nil {
			return err
		}
		for _, stmt := range m.sql {
			if _, err := tx.ExecContext(ctx, stmt); err != nil {
				tx.Rollback()
				return fmt.Errorf("durable: migrate to v%d: %w", m.version, err)
			}
		}
		if _, err := tx.ExecContext(ctx, fmt.Sprintf("PRAGMA user_version = %d", m.version)); err != nil {
			tx.Rollback()
			return fmt.Errorf("durable: migrate to v%d: %w", m.version, err)
		}
		if err := tx.Commit(); err != nil {
			return fmt.Errorf("durable: migrate to v%d: %w", m.version, err)
		}
	}
	return nil
}

// Close closes the database.
func (s *SQLiteStorage) Close(ctx context.Context) error {
	if s.db == nil {
		return ctx.Err()
	}
	return errors.Join(s.db.Close(), ctx.Err())
}

// Commit persists the batch in one transaction and returns its sequence. Any
// error rolls the whole batch back.
func (s *SQLiteStorage) Commit(ctx context.Context, writes []StorageWrite) (Seq, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	if err := validateWrites(writes); err != nil {
		return 0, err
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, err
	}
	for _, w := range writes {
		if err := writeOne(ctx, tx, w); err != nil {
			tx.Rollback()
			return 0, err
		}
	}
	var seq Seq
	if err := tx.QueryRowContext(ctx,
		`UPDATE meta SET last_seq = last_seq + 1 WHERE id = 1 RETURNING last_seq`).Scan(&seq); err != nil {
		tx.Rollback()
		return 0, err
	}
	if err := tx.Commit(); err != nil {
		return 0, err
	}
	return seq, nil
}

// MintID allocates an ID from the persistent global counter, atomic across
// concurrent mints (which happen outside the Session's commit lock).
func (s *SQLiteStorage) MintID(ctx context.Context) (ID, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	var id ID
	err := s.db.QueryRowContext(ctx,
		`UPDATE meta SET next_id = next_id + 1 WHERE id = 1 RETURNING next_id - 1`).Scan(&id)
	return id, err
}

// validateWrites performs the structural checks SQL cannot express naturally:
// value type, unknown write type, zero IDs, and document field requirements.
// State-dependent invariants (entry immutability, document-name uniqueness,
// task terminal immutability) are enforced by SQL constraints and triggers.
func validateWrites(writes []StorageWrite) error {
	for _, w := range writes {
		switch w.Type {
		case WriteConversation:
			rec, ok := w.Value.(*ConversationRecord)
			if !ok {
				return fmt.Errorf("durable: conversation write carries %T", w.Value)
			}
			if rec.ID == 0 {
				return errors.New("durable: conversation with zero ID")
			}
		case WriteEntry:
			rec, ok := w.Value.(*EntryRecord)
			if !ok {
				return fmt.Errorf("durable: entry write carries %T", w.Value)
			}
			if rec.ID == 0 {
				return errors.New("durable: entry with zero ID")
			}
		case WriteTask:
			rec, ok := w.Value.(*TaskRecord)
			if !ok {
				return fmt.Errorf("durable: task write carries %T", w.Value)
			}
			if rec.ID == 0 {
				return errors.New("durable: task with zero ID")
			}
		case WriteSubmission:
			rec, ok := w.Value.(*SubmissionRecord)
			if !ok {
				return fmt.Errorf("durable: submission write carries %T", w.Value)
			}
			if rec.ID == 0 {
				return errors.New("durable: submission with zero ID")
			}
		case WriteDocument:
			rec, ok := w.Value.(*DocumentRecord)
			if !ok {
				return fmt.Errorf("durable: document write carries %T", w.Value)
			}
			if rec.ID == 0 {
				return errors.New("durable: document with zero ID")
			}
			if rec.ConversationID == 0 {
				return errors.New("durable: document with zero conversation ID")
			}
			if rec.Name == "" {
				return errors.New("durable: document with empty name")
			}
		default:
			return fmt.Errorf("durable: unknown write type %q", w.Type)
		}
	}
	return nil
}

// writeOne applies one already-validated write inside tx. Entries are
// immutable (plain INSERT); the other record kinds upsert in place.
func writeOne(ctx context.Context, tx *sql.Tx, w StorageWrite) error {
	switch w.Type {
	case WriteConversation:
		rec := w.Value.(*ConversationRecord)
		body, err := json.Marshal(rec)
		if err != nil {
			return err
		}
		var oc, ot any
		if rec.Owner != nil {
			oc, ot = rec.Owner.ConversationID, rec.Owner.TaskID
		}
		_, err = tx.ExecContext(ctx,
			`INSERT INTO conversations (id, owner_conversation_id, owner_task_id, body)
			 VALUES (?, ?, ?, ?)
			 ON CONFLICT(id) DO UPDATE SET
			   owner_conversation_id = excluded.owner_conversation_id,
			   owner_task_id = excluded.owner_task_id,
			   body = excluded.body`,
			rec.ID, oc, ot, string(body))
		return err
	case WriteEntry:
		rec := w.Value.(*EntryRecord)
		body, err := json.Marshal(rec)
		if err != nil {
			return err
		}
		_, err = tx.ExecContext(ctx,
			`INSERT INTO entries (id, conversation_id, kind, body) VALUES (?, ?, ?, ?)`,
			rec.ID, rec.ConversationID, rec.Kind, string(body))
		return err
	case WriteTask:
		rec := w.Value.(*TaskRecord)
		body, err := json.Marshal(rec)
		if err != nil {
			return err
		}
		var owner any
		if rec.OwnerID != nil {
			owner = *rec.OwnerID
		}
		bg := 0
		if rec.Background {
			bg = 1
		}
		_, err = tx.ExecContext(ctx,
			`INSERT INTO tasks (id, conversation_id, owner_id, kind, status, background, body)
			 VALUES (?, ?, ?, ?, ?, ?, ?)
			 ON CONFLICT(id) DO UPDATE SET
			   conversation_id = excluded.conversation_id,
			   owner_id = excluded.owner_id,
			   kind = excluded.kind,
			   status = excluded.status,
			   background = excluded.background,
			   body = excluded.body`,
			rec.ID, rec.ConversationID, owner, rec.Kind, string(rec.State.Status), bg, string(body))
		return err
	case WriteSubmission:
		rec := w.Value.(*SubmissionRecord)
		body, err := json.Marshal(rec)
		if err != nil {
			return err
		}
		_, err = tx.ExecContext(ctx,
			`INSERT INTO submissions (id, conversation_id, request_id, status, body)
			 VALUES (?, ?, ?, ?, ?)
			 ON CONFLICT(id) DO UPDATE SET
			   conversation_id = excluded.conversation_id,
			   request_id = excluded.request_id,
			   status = excluded.status,
			   body = excluded.body`,
			rec.ID, rec.ConversationID, rec.RequestID, string(rec.Status), string(body))
		return err
	case WriteDocument:
		rec := w.Value.(*DocumentRecord)
		body, err := json.Marshal(rec)
		if err != nil {
			return err
		}
		_, err = tx.ExecContext(ctx,
			`INSERT INTO documents (id, conversation_id, name, body)
			 VALUES (?, ?, ?, ?)
			 ON CONFLICT(id) DO UPDATE SET
			   conversation_id = excluded.conversation_id,
			   name = excluded.name,
			   body = excluded.body`,
			rec.ID, rec.ConversationID, rec.Name, string(body))
		return err
	}
	return nil
}

// queryer is the subset of *sql.DB and *sql.Tx the read helpers need.
type queryer interface {
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
}

// readOne loads one record body and decodes it, returning nil when no row
// matches.
func readOne[T any](ctx context.Context, q queryer, query string, args ...any) (*T, error) {
	var body string
	err := q.QueryRowContext(ctx, query, args...).Scan(&body)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var out T
	if err := json.Unmarshal([]byte(body), &out); err != nil {
		return nil, err
	}
	return &out, nil
}

// queryPage runs a scan query that returns id-ordered body rows and pages them
// with recordIndex.page semantics: fetch limit+1 rows, return the first limit,
// and set Next to the last returned row's ID when an extra row was present.
func queryPage[T any](ctx context.Context, q queryer, query string, args []any, limit int, idOf func(T) ID) (*Page[T], error) {
	rows, err := q.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var items []T
	for rows.Next() {
		it, err := decodeBody[T](rows)
		if err != nil {
			return nil, err
		}
		items = append(items, it)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	out := &Page[T]{}
	if limit > 0 && len(items) > limit {
		out.Items = items[:limit]
		out.Next = &Cursor{LastID: idOf(items[limit-1])}
	} else {
		out.Items = items
	}
	return out, nil
}

func decodeBody[T any](rows *sql.Rows) (T, error) {
	var out T
	var body string
	if err := rows.Scan(&body); err != nil {
		return out, err
	}
	if err := json.Unmarshal([]byte(body), &out); err != nil {
		return out, err
	}
	return out, nil
}

// cursorCond appends the id-cursor condition for an id-ordered scan.
func cursorCond(where string, args []any, order ScanOrder, cursor *Cursor) (string, []any) {
	if cursor == nil {
		return where, args
	}
	if order == OrderDescending {
		return where + " AND id < ?", append(args, cursor.LastID)
	}
	return where + " AND id > ?", append(args, cursor.LastID)
}

// scanPage finishes an id-ordered scan: it applies the cursor condition and
// ordering, then pages the result. table is a compile-time constant.
func scanPage[T any](ctx context.Context, q queryer, table, where string, args []any, order ScanOrder, limit int, cursor *Cursor, idOf func(T) ID) (*Page[T], error) {
	where, args = cursorCond(where, args, order, cursor)
	orderSQL := "ASC"
	if order == OrderDescending {
		orderSQL = "DESC"
	}
	limitArg := -1
	if limit > 0 {
		limitArg = limit + 1
	}
	args = append(args, limitArg)
	query := "SELECT body FROM " + table + " WHERE " + where + " ORDER BY id " + orderSQL + " LIMIT ?"
	return queryPage(ctx, q, query, args, limit, idOf)
}

func (s *SQLiteStorage) Conversation(ctx context.Context, id ID) (*ConversationRecord, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return readOne[ConversationRecord](ctx, s.db, `SELECT body FROM conversations WHERE id = ?`, id)
}

func (s *SQLiteStorage) ScanConversations(ctx context.Context, q ConversationQuery, limit int, cursor *Cursor) (*Page[ConversationRecord], error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	where := "1=1"
	var args []any
	if q.ExcludeRoot {
		where += " AND id != ?"
		args = append(args, RootConversationID)
	}
	if q.OwnerConversationID != nil {
		where += " AND owner_conversation_id = ?"
		args = append(args, *q.OwnerConversationID)
	}
	if q.OwnerTaskID != nil {
		where += " AND owner_task_id = ?"
		args = append(args, *q.OwnerTaskID)
	}
	return scanPage(ctx, s.db, "conversations", where, args, q.Order, limit, cursor, func(c ConversationRecord) ID { return c.ID })
}

func (s *SQLiteStorage) Entry(ctx context.Context, id ID) (*EntryRecord, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return readOne[EntryRecord](ctx, s.db, `SELECT body FROM entries WHERE id = ?`, id)
}

func (s *SQLiteStorage) ScanEntries(ctx context.Context, q EntryQuery, limit int, cursor *Cursor) (*Page[EntryRecord], error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	where := "conversation_id = ?"
	var args []any
	args = append(args, q.ConversationID)
	if q.MinID != nil {
		where += " AND id >= ?"
		args = append(args, *q.MinID)
	}
	if q.MaxID != nil {
		where += " AND id <= ?"
		args = append(args, *q.MaxID)
	}
	return scanPage(ctx, s.db, "entries", where, args, q.Order, limit, cursor, func(e EntryRecord) ID { return e.ID })
}

func (s *SQLiteStorage) Task(ctx context.Context, id ID) (*TaskRecord, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return readOne[TaskRecord](ctx, s.db, `SELECT body FROM tasks WHERE id = ?`, id)
}

func (s *SQLiteStorage) ScanTasks(ctx context.Context, q TaskQuery, limit int, cursor *Cursor) (*Page[TaskRecord], error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	where := "1=1"
	var args []any
	if q.ConversationID != nil {
		where += " AND conversation_id = ?"
		args = append(args, *q.ConversationID)
	}
	if q.OwnerID != nil {
		where += " AND owner_id = ?"
		args = append(args, *q.OwnerID)
	}
	if q.Kind != "" {
		where += " AND kind = ?"
		args = append(args, q.Kind)
	}
	if q.Status != "" {
		where += " AND status = ?"
		args = append(args, string(q.Status))
	}
	if q.Background != nil {
		bg := 0
		if *q.Background {
			bg = 1
		}
		where += " AND background = ?"
		args = append(args, bg)
	}
	return scanPage(ctx, s.db, "tasks", where, args, q.Order, limit, cursor, func(t TaskRecord) ID { return t.ID })
}

func (s *SQLiteStorage) Submission(ctx context.Context, id ID) (*SubmissionRecord, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return readOne[SubmissionRecord](ctx, s.db, `SELECT body FROM submissions WHERE id = ?`, id)
}

func (s *SQLiteStorage) ScanSubmissions(ctx context.Context, q SubmissionQuery, limit int, cursor *Cursor) (*Page[SubmissionRecord], error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	where := "1=1"
	var args []any
	if q.ConversationID != nil {
		where += " AND conversation_id = ?"
		args = append(args, *q.ConversationID)
	}
	if q.Status != "" {
		where += " AND status = ?"
		args = append(args, string(q.Status))
	}
	return scanPage(ctx, s.db, "submissions", where, args, q.Order, limit, cursor, func(r SubmissionRecord) ID { return r.ID })
}

func (s *SQLiteStorage) SubmissionByRequest(ctx context.Context, requestID string) (*SubmissionRecord, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if requestID == "" {
		return nil, nil
	}
	return readOne[SubmissionRecord](ctx, s.db,
		`SELECT body FROM submissions WHERE request_id = ? ORDER BY id DESC LIMIT 1`, requestID)
}

func (s *SQLiteStorage) Document(ctx context.Context, id ID) (*DocumentRecord, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return readOne[DocumentRecord](ctx, s.db, `SELECT body FROM documents WHERE id = ?`, id)
}

func (s *SQLiteStorage) DocumentByName(ctx context.Context, conversationID ID, name string) (*DocumentRecord, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return readOne[DocumentRecord](ctx, s.db,
		`SELECT body FROM documents WHERE conversation_id = ? AND name = ?`, conversationID, name)
}

func (s *SQLiteStorage) ScanDocuments(ctx context.Context, q DocumentQuery, limit int, cursor *Cursor) (*Page[DocumentRecord], error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	where := "1=1"
	var args []any
	if q.ConversationID != nil {
		where += " AND conversation_id = ?"
		args = append(args, *q.ConversationID)
	}
	if q.Name != "" {
		where += " AND name = ?"
		args = append(args, q.Name)
	}
	return scanPage(ctx, s.db, "documents", where, args, q.Order, limit, cursor, func(d DocumentRecord) ID { return d.ID })
}
