package durable

import (
	"context"
	"encoding/json"
	"errors"
	"path/filepath"
	"testing"

	"vino/internal/agent"
)

// docTaskDef is a test task that manipulates documents through TaskRuntime. It
// records what rt.Document returned (or errored with) so the public behavior
// is observable at the seam.
type docTaskDef struct {
	steps []docStep
	got   json.RawMessage
	err   error
}

type docStep struct {
	action string // "write", "writeTwice", "writeFail", "read", "readAfterWrite"
	name   string
	value  json.RawMessage
	value2 json.RawMessage
}

func (d *docTaskDef) Kind() string { return "test.doc" }
func (d *docTaskDef) Version() int { return 1 }
func (d *docTaskDef) Initial(json.RawMessage) json.RawMessage {
	return mustJSON(0)
}
func (d *docTaskDef) Step(_ context.Context, checkpoint json.RawMessage, rt *TaskRuntime) (NextState, error) {
	var i int
	if len(checkpoint) > 0 {
		_ = json.Unmarshal(checkpoint, &i)
	}
	if i >= len(d.steps) {
		return NextState{Status: TaskTerminal, Outcome: &Outcome{Status: OutcomeCompleted}}, nil
	}
	s := d.steps[i]
	i++
	next := mustJSON(i)

	switch s.action {
	case "write":
		if err := rt.SetDocument(s.name, s.value); err != nil {
			return NextState{}, err
		}
		entryID, err := rt.MintID()
		if err != nil {
			return NextState{}, err
		}
		rt.AppendEntry(&EntryRecord{ID: entryID, ConversationID: rt.ConversationID(), Kind: kindUser,
			Model: []agent.Message{{Role: agent.RoleUser, Content: "marker"}}})
	case "writeFail":
		if err := rt.SetDocument(s.name, s.value); err != nil {
			return NextState{}, err
		}
		return NextState{}, errors.New("boom after write")
	case "writeTwice":
		if err := rt.SetDocument(s.name, s.value); err != nil {
			return NextState{}, err
		}
		if err := rt.SetDocument(s.name, s.value2); err != nil {
			return NextState{}, err
		}
	case "read":
		got, err := rt.Document(s.name)
		if err != nil {
			d.err = err
		} else {
			d.got = got
		}
	case "readAfterWrite":
		if err := rt.SetDocument(s.name, s.value); err != nil {
			return NextState{}, err
		}
		got, err := rt.Document(s.name)
		if err != nil {
			return NextState{}, err
		}
		d.got = got
	}
	return NextState{Status: TaskRunning, Checkpoint: next}, nil
}

// seedDocTask writes a conversation and a pending "test.doc" task into st and
// returns the conversation ID.
func seedDocTask(t *testing.T, st Storage, ctx context.Context) ID {
	t.Helper()
	convID := mint(t, st, ctx)
	write(t, st, ctx, []StorageWrite{
		{Type: WriteConversation, Value: &ConversationRecord{ID: RootConversationID}},
		{Type: WriteConversation, Value: &ConversationRecord{ID: convID}},
		{Type: WriteTask, Value: &TaskRecord{ID: mint(t, st, ctx), ConversationID: convID, Kind: "test.doc", Version: 1,
			Input: json.RawMessage(`{}`), State: TaskState{Status: TaskPending, Checkpoint: json.RawMessage(`0`)}}},
	})
	return convID
}

func TestDocumentReadReturnsDefault(t *testing.T) {
	ctx := context.Background()
	h, err := Open(ctx, NewMemoryStorage(), HarnessOptions{
		LLM:       &fakeLLM{},
		Documents: []DocumentDefinition{{Name: "plan", Default: json.RawMessage(`{"n":0}`)}},
	})
	if err != nil {
		t.Fatal(err)
	}
	v, err := h.Root().Document(ctx, "plan")
	if err != nil {
		t.Fatal(err)
	}
	if string(v) != `{"n":0}` {
		t.Fatalf("Document = %s, want default {\"n\":0}", v)
	}
}

func TestDocumentUndeclaredErrors(t *testing.T) {
	ctx := context.Background()
	def := &docTaskDef{steps: []docStep{{action: "read", name: "nope"}}}
	h, err := Open(ctx, NewMemoryStorage(), HarnessOptions{
		LLM:         &fakeLLM{},
		Documents:   []DocumentDefinition{{Name: "plan"}},
		Definitions: []TaskDefinition{def},
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := h.Root().Document(ctx, "nope"); !errors.Is(err, ErrDocumentUndeclared) {
		t.Fatalf("host Document(undeclared) error = %v, want ErrDocumentUndeclared", err)
	}

	convID := seedDocTask(t, h.session.storage, ctx)
	if err := h.Run(ctx); err != nil {
		t.Fatal(err)
	}
	if !errors.Is(def.err, ErrDocumentUndeclared) {
		t.Fatalf("task Document(undeclared) error = %v, want ErrDocumentUndeclared", def.err)
	}
	_ = convID
}

func TestDocumentSetReplaceAndRead(t *testing.T) {
	ctx := context.Background()
	def := &docTaskDef{steps: []docStep{
		{action: "write", name: "plan", value: json.RawMessage(`{"n":1}`)},
		{action: "write", name: "plan", value: json.RawMessage(`{"n":2}`)},
	}}
	h, err := Open(ctx, NewMemoryStorage(), HarnessOptions{
		LLM:         &fakeLLM{},
		Documents:   []DocumentDefinition{{Name: "plan"}},
		Definitions: []TaskDefinition{def},
	})
	if err != nil {
		t.Fatal(err)
	}
	convID := seedDocTask(t, h.session.storage, ctx)
	if err := h.Run(ctx); err != nil {
		t.Fatal(err)
	}
	c, err := h.Conversation(ctx, convID)
	if err != nil {
		t.Fatal(err)
	}
	v, err := c.Document(ctx, "plan")
	if err != nil {
		t.Fatal(err)
	}
	if string(v) != `{"n":2}` {
		t.Fatalf("Document = %s, want replaced value {\"n\":2}", v)
	}
}

func TestDocumentAtomicWithStep(t *testing.T) {
	ctx := context.Background()
	def := &docTaskDef{steps: []docStep{{action: "writeFail", name: "plan", value: json.RawMessage(`{"n":9}`)}}}
	h, err := Open(ctx, NewMemoryStorage(), HarnessOptions{
		LLM:         &fakeLLM{},
		Documents:   []DocumentDefinition{{Name: "plan", Default: json.RawMessage(`{"n":0}`)}},
		Definitions: []TaskDefinition{def},
	})
	if err != nil {
		t.Fatal(err)
	}
	convID := seedDocTask(t, h.session.storage, ctx)
	if err := h.Run(ctx); err != nil {
		t.Fatal(err)
	}

	// The failed step staged a document write and an entry; neither landed.
	c, err := h.Conversation(ctx, convID)
	if err != nil {
		t.Fatal(err)
	}
	v, err := c.Document(ctx, "plan")
	if err != nil {
		t.Fatal(err)
	}
	if string(v) != `{"n":0}` {
		t.Fatalf("Document = %s, want default {\"n\":0} (staged write discarded)", v)
	}
	entries, err := c.Entries(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 0 {
		t.Fatalf("entries = %d, want 0 (staged entry discarded)", len(entries))
	}
}

func TestDocumentStagedReadSeesCommittedOnly(t *testing.T) {
	ctx := context.Background()
	def := &docTaskDef{steps: []docStep{
		{action: "write", name: "plan", value: json.RawMessage(`"old"`)},
		{action: "readAfterWrite", name: "plan", value: json.RawMessage(`"new"`)},
	}}
	h, err := Open(ctx, NewMemoryStorage(), HarnessOptions{
		LLM:         &fakeLLM{},
		Documents:   []DocumentDefinition{{Name: "plan"}},
		Definitions: []TaskDefinition{def},
	})
	if err != nil {
		t.Fatal(err)
	}
	convID := seedDocTask(t, h.session.storage, ctx)
	if err := h.Run(ctx); err != nil {
		t.Fatal(err)
	}
	if string(def.got) != `"old"` {
		t.Fatalf("in-step Document after SetDocument = %s, want committed value \"old\"", def.got)
	}
	c, err := h.Conversation(ctx, convID)
	if err != nil {
		t.Fatal(err)
	}
	v, err := c.Document(ctx, "plan")
	if err != nil {
		t.Fatal(err)
	}
	if string(v) != `"new"` {
		t.Fatalf("Document = %s, want the staged write to have landed as \"new\"", v)
	}
}

func TestDocumentSetTwiceInStepLastWriteWins(t *testing.T) {
	ctx := context.Background()
	def := &docTaskDef{steps: []docStep{{action: "writeTwice", name: "plan", value: json.RawMessage(`"a"`), value2: json.RawMessage(`"b"`)}}}
	h, err := Open(ctx, NewMemoryStorage(), HarnessOptions{
		LLM:         &fakeLLM{},
		Documents:   []DocumentDefinition{{Name: "plan"}},
		Definitions: []TaskDefinition{def},
	})
	if err != nil {
		t.Fatal(err)
	}
	convID := seedDocTask(t, h.session.storage, ctx)
	if err := h.Run(ctx); err != nil {
		t.Fatal(err)
	}
	c, err := h.Conversation(ctx, convID)
	if err != nil {
		t.Fatal(err)
	}
	v, err := c.Document(ctx, "plan")
	if err != nil {
		t.Fatal(err)
	}
	if string(v) != `"b"` {
		t.Fatalf("Document = %s, want last write \"b\"", v)
	}
}

func TestDocumentSurvivesJSONLReopen(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "agent.jsonl")
	def := &docTaskDef{steps: []docStep{{action: "write", name: "plan", value: json.RawMessage(`{"n":7}`)}}}

	open := func() (*Harness, error) {
		st, err := OpenJSONL(path, false)
		if err != nil {
			return nil, err
		}
		return Open(ctx, st, HarnessOptions{
			LLM:         &fakeLLM{},
			Documents:   []DocumentDefinition{{Name: "plan"}},
			Definitions: []TaskDefinition{def},
		})
	}

	h, err := open()
	if err != nil {
		t.Fatal(err)
	}
	convID := seedDocTask(t, h.session.storage, ctx)
	if err := h.Run(ctx); err != nil {
		t.Fatal(err)
	}
	if err := h.Close(ctx); err != nil {
		t.Fatal(err)
	}

	h, err = open()
	if err != nil {
		t.Fatal(err)
	}
	c, err := h.Conversation(ctx, convID)
	if err != nil {
		t.Fatal(err)
	}
	v, err := c.Document(ctx, "plan")
	if err != nil {
		t.Fatal(err)
	}
	if string(v) != `{"n":7}` {
		t.Fatalf("Document after reopen = %s, want {\"n\":7}", v)
	}
}

func TestForkCopiesCurrentValue(t *testing.T) {
	ctx := context.Background()
	st := NewMemoryStorage()
	convID := mint(t, st, ctx)
	entryID := mint(t, st, ctx)
	docID := mint(t, st, ctx)
	write(t, st, ctx, []StorageWrite{
		{Type: WriteConversation, Value: &ConversationRecord{ID: RootConversationID}},
		{Type: WriteConversation, Value: &ConversationRecord{ID: convID}},
		{Type: WriteEntry, Value: &EntryRecord{ID: entryID, ConversationID: convID, Kind: kindUser,
			Model: []agent.Message{{Role: agent.RoleUser, Content: "hi"}}}},
		{Type: WriteDocument, Value: &DocumentRecord{ID: docID, ConversationID: convID, Name: "plan", Value: json.RawMessage(`"A"`)}},
	})
	h, err := Open(ctx, st, HarnessOptions{LLM: &fakeLLM{}, Documents: []DocumentDefinition{{Name: "plan"}}})
	if err != nil {
		t.Fatal(err)
	}
	parent, err := h.Conversation(ctx, convID)
	if err != nil {
		t.Fatal(err)
	}
	at, err := h.Entry(ctx, entryID)
	if err != nil {
		t.Fatal(err)
	}
	child, err := parent.Fork(ctx, at, AgentConfig{})
	if err != nil {
		t.Fatal(err)
	}

	v, err := child.Document(ctx, "plan")
	if err != nil {
		t.Fatal(err)
	}
	if string(v) != `"A"` {
		t.Fatalf("fork Document = %s, want parent's current value \"A\"", v)
	}

	// The fork is a snapshot: the parent changing later must not reach it.
	write(t, st, ctx, []StorageWrite{
		{Type: WriteDocument, Value: &DocumentRecord{ID: docID, ConversationID: convID, Name: "plan", Value: json.RawMessage(`"B"`)}},
	})
	v, err = child.Document(ctx, "plan")
	if err != nil {
		t.Fatal(err)
	}
	if string(v) != `"A"` {
		t.Fatalf("fork Document after parent change = %s, want snapshot \"A\"", v)
	}
}

func TestForkFreshUsesDefault(t *testing.T) {
	ctx := context.Background()
	st := NewMemoryStorage()
	convID := mint(t, st, ctx)
	entryID := mint(t, st, ctx)
	write(t, st, ctx, []StorageWrite{
		{Type: WriteConversation, Value: &ConversationRecord{ID: RootConversationID}},
		{Type: WriteConversation, Value: &ConversationRecord{ID: convID}},
		{Type: WriteEntry, Value: &EntryRecord{ID: entryID, ConversationID: convID, Kind: kindUser,
			Model: []agent.Message{{Role: agent.RoleUser, Content: "hi"}}}},
		{Type: WriteDocument, Value: &DocumentRecord{ID: mint(t, st, ctx), ConversationID: convID, Name: "plan", Value: json.RawMessage(`"A"`)}},
	})
	h, err := Open(ctx, st, HarnessOptions{LLM: &fakeLLM{}, Documents: []DocumentDefinition{{Name: "plan", Default: json.RawMessage(`"fresh"`), Fork: DocumentForkFresh}}})
	if err != nil {
		t.Fatal(err)
	}
	parent, err := h.Conversation(ctx, convID)
	if err != nil {
		t.Fatal(err)
	}
	at, err := h.Entry(ctx, entryID)
	if err != nil {
		t.Fatal(err)
	}
	child, err := parent.Fork(ctx, at, AgentConfig{})
	if err != nil {
		t.Fatal(err)
	}
	v, err := child.Document(ctx, "plan")
	if err != nil {
		t.Fatal(err)
	}
	if string(v) != `"fresh"` {
		t.Fatalf("fork Document = %s, want fresh default \"fresh\"", v)
	}
}

func TestForkCurrentMaterializesDefaultWhenUnset(t *testing.T) {
	ctx := context.Background()
	st := NewMemoryStorage()
	convID := mint(t, st, ctx)
	entryID := mint(t, st, ctx)
	write(t, st, ctx, []StorageWrite{
		{Type: WriteConversation, Value: &ConversationRecord{ID: RootConversationID}},
		{Type: WriteConversation, Value: &ConversationRecord{ID: convID}},
		{Type: WriteEntry, Value: &EntryRecord{ID: entryID, ConversationID: convID, Kind: kindUser,
			Model: []agent.Message{{Role: agent.RoleUser, Content: "hi"}}}},
	})
	h, err := Open(ctx, st, HarnessOptions{LLM: &fakeLLM{}, Documents: []DocumentDefinition{{Name: "plan", Default: json.RawMessage(`{"n":0}`)}}})
	if err != nil {
		t.Fatal(err)
	}
	parent, err := h.Conversation(ctx, convID)
	if err != nil {
		t.Fatal(err)
	}
	at, err := h.Entry(ctx, entryID)
	if err != nil {
		t.Fatal(err)
	}
	child, err := parent.Fork(ctx, at, AgentConfig{})
	if err != nil {
		t.Fatal(err)
	}

	v, err := child.Document(ctx, "plan")
	if err != nil {
		t.Fatal(err)
	}
	if string(v) != `{"n":0}` {
		t.Fatalf("fork Document = %s, want default {\"n\":0}", v)
	}
	page, err := st.ScanDocuments(ctx, DocumentQuery{ConversationID: &child.id, Order: OrderAscending}, 0, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(page.Items) != 1 || page.Items[0].Name != "plan" || string(page.Items[0].Value) != `{"n":0}` {
		t.Fatalf("fork materialized document = %+v, want one explicit plan record with the default", page.Items)
	}
}
