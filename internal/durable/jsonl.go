package durable

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"
)

// JSONLStorage persists records to a single append-only JSONL file: one line
// per record write plus one commit-marker line per commit. On open it replays
// the file into an in-memory index; a torn trailing line or a commit without a
// final marker is discarded. It provides process-crash consistency; fsync
// extends that toward host/power failure.
type JSONLStorage struct {
	ix      *recordIndex
	nextID  ID
	nextSeq Seq
	path    string
	f       *os.File
	fsync   bool
	w       *bufio.Writer
}

// jsonlLine is one line of the log: a record write or a commit marker.
type jsonlLine struct {
	Type  string          `json:"type"`
	Seq   uint64          `json:"seq,omitempty"`
	Value json.RawMessage `json:"value,omitempty"`
}

// OpenJSONL opens (creating if needed) the log at path and replays it. fsync
// controls whether each commit is synced to disk before it is acknowledged.
func OpenJSONL(path string, fsync bool) (*JSONLStorage, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return nil, fmt.Errorf("durable: open jsonl: %w", err)
	}
	j := &JSONLStorage{
		ix:     newRecordIndex(),
		nextID: 2,
		path:   path,
		f:      f,
		fsync:  fsync,
		w:      bufio.NewWriter(f),
	}
	if err := j.replay(f); err != nil {
		f.Close()
		return nil, fmt.Errorf("durable: replay jsonl %s: %w", path, err)
	}
	return j, nil
}

// replay rebuilds the index from committed records, discarding anything after
// the last complete commit marker.
func (j *JSONLStorage) replay(f *os.File) error {
	if _, err := f.Seek(0, 0); err != nil {
		return err
	}
	raw, err := os.ReadFile(j.path)
	if err != nil {
		return err
	}
	if len(raw) == 0 {
		return nil
	}
	text := string(raw)
	parts := strings.Split(text, "\n")
	if !strings.HasSuffix(text, "\n") {
		parts = parts[:len(parts)-1] // torn trailing line
	}

	var pending []StorageWrite
	var maxStored ID
	for _, p := range parts {
		if p == "" {
			continue
		}
		var line jsonlLine
		if err := json.Unmarshal([]byte(p), &line); err != nil {
			return fmt.Errorf("corrupt line: %w", err)
		}
		switch line.Type {
		case "commit":
			if err := j.ix.apply(pending); err != nil {
				return err
			}
			pending = nil
			j.nextSeq = Seq(line.Seq)
		case "conversation", "entry", "task", "submission":
			w, err := decodeWrite(line)
			if err != nil {
				return err
			}
			pending = append(pending, w)
		default:
			return fmt.Errorf("unknown line type %q", line.Type)
		}
	}
	// pending writes without a final marker are discarded (torn commit).
	for id := range j.ix.conversations {
		if id > maxStored {
			maxStored = id
		}
	}
	for id := range j.ix.entries {
		if id > maxStored {
			maxStored = id
		}
	}
	for id := range j.ix.tasks {
		if id > maxStored {
			maxStored = id
		}
	}
	for id := range j.ix.submissions {
		if id > maxStored {
			maxStored = id
		}
	}
	if maxStored+1 > j.nextID {
		j.nextID = maxStored + 1
	}
	return nil
}

func decodeWrite(line jsonlLine) (StorageWrite, error) {
	switch line.Type {
	case "conversation":
		var rec ConversationRecord
		if err := json.Unmarshal(line.Value, &rec); err != nil {
			return StorageWrite{}, err
		}
		return StorageWrite{Type: WriteConversation, Value: &rec}, nil
	case "entry":
		var rec EntryRecord
		if err := json.Unmarshal(line.Value, &rec); err != nil {
			return StorageWrite{}, err
		}
		return StorageWrite{Type: WriteEntry, Value: &rec}, nil
	case "task":
		var rec TaskRecord
		if err := json.Unmarshal(line.Value, &rec); err != nil {
			return StorageWrite{}, err
		}
		return StorageWrite{Type: WriteTask, Value: &rec}, nil
	case "submission":
		var rec SubmissionRecord
		if err := json.Unmarshal(line.Value, &rec); err != nil {
			return StorageWrite{}, err
		}
		return StorageWrite{Type: WriteSubmission, Value: &rec}, nil
	default:
		return StorageWrite{}, fmt.Errorf("unknown write type %q", line.Type)
	}
}

func (j *JSONLStorage) Commit(ctx context.Context, writes []StorageWrite) (Seq, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	if err := j.ix.validate(writes); err != nil {
		return 0, err
	}
	j.nextSeq++

	var buf strings.Builder
	for _, w := range writes {
		value, err := json.Marshal(w.Value)
		if err != nil {
			return 0, err
		}
		line, err := json.Marshal(jsonlLine{Type: string(w.Type), Value: value})
		if err != nil {
			return 0, err
		}
		buf.Write(line)
		buf.WriteByte('\n')
	}
	marker, _ := json.Marshal(jsonlLine{Type: "commit", Seq: uint64(j.nextSeq)})
	buf.Write(marker)
	buf.WriteByte('\n')

	if _, err := j.w.WriteString(buf.String()); err != nil {
		return 0, err
	}
	if err := j.w.Flush(); err != nil {
		return 0, err
	}
	if j.fsync {
		if err := j.f.Sync(); err != nil {
			return 0, err
		}
	}
	// The marker is durable; publish.
	if err := j.ix.apply(writes); err != nil {
		return 0, err
	}
	return j.nextSeq, nil
}

func (j *JSONLStorage) MintID(ctx context.Context) (ID, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	id := j.nextID
	j.nextID++
	return id, nil
}

func (j *JSONLStorage) Conversation(ctx context.Context, id ID) (*ConversationRecord, error) {
	return j.ix.conversation(id), ctx.Err()
}

func (j *JSONLStorage) ScanConversations(ctx context.Context, q ConversationQuery, limit int, cursor *Cursor) (*Page[ConversationRecord], error) {
	return j.ix.scanConversations(q, limit, cursor), ctx.Err()
}

func (j *JSONLStorage) Entry(ctx context.Context, id ID) (*EntryRecord, error) {
	return j.ix.entry(id), ctx.Err()
}

func (j *JSONLStorage) ScanEntries(ctx context.Context, q EntryQuery, limit int, cursor *Cursor) (*Page[EntryRecord], error) {
	return j.ix.scanEntries(q, limit, cursor), ctx.Err()
}

func (j *JSONLStorage) Task(ctx context.Context, id ID) (*TaskRecord, error) {
	return j.ix.task(id), ctx.Err()
}

func (j *JSONLStorage) ScanTasks(ctx context.Context, q TaskQuery, limit int, cursor *Cursor) (*Page[TaskRecord], error) {
	return j.ix.scanTasks(q, limit, cursor), ctx.Err()
}

func (j *JSONLStorage) Submission(ctx context.Context, id ID) (*SubmissionRecord, error) {
	return j.ix.submission(id), ctx.Err()
}

func (j *JSONLStorage) ScanSubmissions(ctx context.Context, q SubmissionQuery, limit int, cursor *Cursor) (*Page[SubmissionRecord], error) {
	return j.ix.scanSubmissions(q, limit, cursor), ctx.Err()
}

func (j *JSONLStorage) SubmissionByRequest(ctx context.Context, requestID string) (*SubmissionRecord, error) {
	return j.ix.submissionByRequest(requestID), ctx.Err()
}

func (j *JSONLStorage) Close(ctx context.Context) error {
	if err := j.w.Flush(); err != nil {
		return err
	}
	err1 := j.f.Close()
	err2 := ctx.Err()
	return errors.Join(err1, err2)
}
