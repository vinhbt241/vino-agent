package durable

import (
	"encoding/json"
	"errors"
	"fmt"
	"sort"
)

// recordIndex is the shared in-memory record store behind MemoryStorage and
// JSONLStorage. It enforces immutability and clone-on-read so neither backend
// can alias commit arguments into its tables.
type recordIndex struct {
	conversations map[ID]*ConversationRecord
	entries       map[ID]*EntryRecord
	tasks         map[ID]*TaskRecord
	submissions   map[ID]*SubmissionRecord
	reqIndex      map[string]*SubmissionRecord
}

func newRecordIndex() *recordIndex {
	return &recordIndex{
		conversations: make(map[ID]*ConversationRecord),
		entries:       make(map[ID]*EntryRecord),
		tasks:         make(map[ID]*TaskRecord),
		submissions:   make(map[ID]*SubmissionRecord),
		reqIndex:      make(map[string]*SubmissionRecord),
	}
}

// apply validates one commit batch without mutating, then stores it. The two
// phases are split so a batch that fails validation never partially applies.
func (ix *recordIndex) apply(writes []StorageWrite) error {
	if err := ix.validate(writes); err != nil {
		return err
	}
	for _, w := range writes {
		ix.store(w)
	}
	return nil
}

// validate checks the whole batch before any store. It makes the same checks
// store relies on, so store itself cannot fail.
func (ix *recordIndex) validate(writes []StorageWrite) error {
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
			if _, exists := ix.conversations[rec.ID]; exists {
				return fmt.Errorf("durable: conversation %d already exists", rec.ID)
			}
		case WriteEntry:
			rec, ok := w.Value.(*EntryRecord)
			if !ok {
				return fmt.Errorf("durable: entry write carries %T", w.Value)
			}
			if rec.ID == 0 {
				return errors.New("durable: entry with zero ID")
			}
			if _, exists := ix.entries[rec.ID]; exists {
				return fmt.Errorf("durable: entry %d already exists", rec.ID)
			}
		case WriteTask:
			rec, ok := w.Value.(*TaskRecord)
			if !ok {
				return fmt.Errorf("durable: task write carries %T", w.Value)
			}
			if rec.ID == 0 {
				return errors.New("durable: task with zero ID")
			}
			if existing, exists := ix.tasks[rec.ID]; exists {
				if existing.State.Status == TaskTerminal {
					return fmt.Errorf("durable: task %d is terminal and immutable", rec.ID)
				}
			}
		case WriteSubmission:
			rec, ok := w.Value.(*SubmissionRecord)
			if !ok {
				return fmt.Errorf("durable: submission write carries %T", w.Value)
			}
			if rec.ID == 0 {
				return errors.New("durable: submission with zero ID")
			}
		default:
			return fmt.Errorf("durable: unknown write type %q", w.Type)
		}
	}
	return nil
}

// store applies one already-validated write, cloning values so later caller
// mutation cannot reach stored state.
func (ix *recordIndex) store(w StorageWrite) {
	switch w.Type {
	case WriteConversation:
		rec := w.Value.(*ConversationRecord)
		ix.conversations[rec.ID] = clone(rec)
	case WriteEntry:
		rec := w.Value.(*EntryRecord)
		ix.entries[rec.ID] = clone(rec)
	case WriteTask:
		rec := w.Value.(*TaskRecord)
		ix.tasks[rec.ID] = clone(rec)
	case WriteSubmission:
		rec := w.Value.(*SubmissionRecord)
		ix.submissions[rec.ID] = clone(rec)
		if rec.RequestID != "" {
			ix.reqIndex[rec.RequestID] = clone(rec)
		}
	}
}

// clone round-trips a record through JSON so the index never shares memory
// with callers. Records are JSON-marshalable by construction.
func clone[T any](v T) T {
	b, err := json.Marshal(v)
	if err != nil {
		// A record that cannot marshal is a programmer error; surface it as a
		// panic rather than silently corrupting the index.
		panic(fmt.Sprintf("durable: clone: %v", err))
	}
	var out T
	if err := json.Unmarshal(b, &out); err != nil {
		panic(fmt.Sprintf("durable: clone: %v", err))
	}
	return out
}

// page slices ID-sorted (ascending) items into one scan page.
func page[T any](items []T, order ScanOrder, limit int, cursor *Cursor, idOf func(T) ID) *Page[T] {
	filtered := make([]T, 0, len(items))
	for _, it := range items {
		id := idOf(it)
		if cursor != nil {
			if order == OrderAscending && id <= cursor.LastID {
				continue
			}
			if order == OrderDescending && id >= cursor.LastID {
				continue
			}
		}
		filtered = append(filtered, it)
	}
	if order == OrderDescending {
		reverse(filtered)
	}
	if limit <= 0 {
		limit = len(filtered)
	}
	out := &Page[T]{}
	if len(filtered) > limit {
		out.Items = append([]T(nil), filtered[:limit]...)
		out.Next = &Cursor{LastID: idOf(filtered[limit-1])}
	} else {
		out.Items = append([]T(nil), filtered...)
	}
	return out
}

func reverse[T any](s []T) {
	for i, j := 0, len(s)-1; i < j; i, j = i+1, j-1 {
		s[i], s[j] = s[j], s[i]
	}
}

// ---- reads ----

func (ix *recordIndex) conversation(id ID) *ConversationRecord {
	if rec, ok := ix.conversations[id]; ok {
		return clone(rec)
	}
	return nil
}

func (ix *recordIndex) entry(id ID) *EntryRecord {
	if rec, ok := ix.entries[id]; ok {
		return clone(rec)
	}
	return nil
}

func (ix *recordIndex) task(id ID) *TaskRecord {
	if rec, ok := ix.tasks[id]; ok {
		return clone(rec)
	}
	return nil
}

func (ix *recordIndex) submission(id ID) *SubmissionRecord {
	if rec, ok := ix.submissions[id]; ok {
		return clone(rec)
	}
	return nil
}

func (ix *recordIndex) submissionByRequest(requestID string) *SubmissionRecord {
	if rec, ok := ix.reqIndex[requestID]; ok {
		return clone(rec)
	}
	return nil
}

// ---- scans ----

func (ix *recordIndex) scanConversations(q ConversationQuery, limit int, cursor *Cursor) *Page[ConversationRecord] {
	var items []ConversationRecord
	for _, rec := range ix.conversations {
		if q.ExcludeRoot && rec.ID == RootConversationID {
			continue
		}
		if q.OwnerConversationID != nil && (rec.Owner == nil || rec.Owner.ConversationID != *q.OwnerConversationID) {
			continue
		}
		if q.OwnerTaskID != nil && (rec.Owner == nil || rec.Owner.TaskID != *q.OwnerTaskID) {
			continue
		}
		items = append(items, *clone(rec))
	}
	sort.Slice(items, func(i, j int) bool { return items[i].ID < items[j].ID })
	return page(items, q.Order, limit, cursor, func(c ConversationRecord) ID { return c.ID })
}

func (ix *recordIndex) scanEntries(q EntryQuery, limit int, cursor *Cursor) *Page[EntryRecord] {
	var items []EntryRecord
	for _, rec := range ix.entries {
		if rec.ConversationID != q.ConversationID {
			continue
		}
		if q.MinID != nil && rec.ID < *q.MinID {
			continue
		}
		if q.MaxID != nil && rec.ID > *q.MaxID {
			continue
		}
		items = append(items, *clone(rec))
	}
	sort.Slice(items, func(i, j int) bool { return items[i].ID < items[j].ID })
	return page(items, q.Order, limit, cursor, func(e EntryRecord) ID { return e.ID })
}

func (ix *recordIndex) scanTasks(q TaskQuery, limit int, cursor *Cursor) *Page[TaskRecord] {
	var items []TaskRecord
	for _, rec := range ix.tasks {
		if q.ConversationID != nil && rec.ConversationID != *q.ConversationID {
			continue
		}
		if q.Kind != "" && rec.Kind != q.Kind {
			continue
		}
		if q.Status != "" && rec.State.Status != q.Status {
			continue
		}
		if q.Background != nil && rec.Background != *q.Background {
			continue
		}
		items = append(items, *clone(rec))
	}
	sort.Slice(items, func(i, j int) bool { return items[i].ID < items[j].ID })
	return page(items, q.Order, limit, cursor, func(t TaskRecord) ID { return t.ID })
}

func (ix *recordIndex) scanSubmissions(q SubmissionQuery, limit int, cursor *Cursor) *Page[SubmissionRecord] {
	var items []SubmissionRecord
	for _, rec := range ix.submissions {
		if q.ConversationID != nil && rec.ConversationID != *q.ConversationID {
			continue
		}
		if q.Status != "" && rec.Status != q.Status {
			continue
		}
		items = append(items, *clone(rec))
	}
	sort.Slice(items, func(i, j int) bool { return items[i].ID < items[j].ID })
	return page(items, q.Order, limit, cursor, func(s SubmissionRecord) ID { return s.ID })
}
