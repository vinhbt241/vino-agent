package durable

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"vino/internal/agent"
)

// TestJSONLRecoveryAcrossReopen is the end-to-end durability proof: a process
// "dies" mid-tool-call (running tool task), a new process reopens the same
// file, and the harness resumes the safe tool call to completion.
func TestJSONLRecoveryAcrossReopen(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "agent.jsonl")

	st, err := OpenJSONL(path, false)
	if err != nil {
		t.Fatal(err)
	}
	seedCrashedTool(t, st, true)
	if err := st.Close(ctx); err != nil {
		t.Fatal(err)
	}

	// New process: reopen the same log.
	st2, err := OpenJSONL(path, false)
	if err != nil {
		t.Fatal(err)
	}
	defer st2.Close(ctx)

	llm := &fakeLLM{steps: []llmStep{{resp: agent.Response{Content: "recovered"}}}}
	tools := &fakeRegistry{results: map[string]string{"search": "hit"}}

	h, err := Open(ctx, st2, HarnessOptions{LLM: llm, Tools: tools})
	if err != nil {
		t.Fatal(err)
	}
	if err := h.Run(ctx); err != nil {
		t.Fatal(err)
	}
	if len(tools.calls) != 1 || tools.calls[0] != "search" {
		t.Fatalf("tool calls = %v, want [search] re-run after reopen", tools.calls)
	}
	entries, _ := h.Root().Entries(ctx)
	if got := toolEntryContent(t, entries); got != "hit" {
		t.Fatalf("tool entry = %q, want hit", got)
	}
	// The generation task resumed to the model phase and answered.
	found := false
	for _, e := range entries {
		if e.Kind == kindAssistant && len(e.Model) > 0 && e.Model[0].Content == "recovered" {
			found = true
		}
	}
	if !found {
		t.Fatalf("resumed answer not found in transcript: %+v", entries)
	}
}

// TestJSONLTornCommitDiscarded proves a write whose commit marker never made
// it to disk is discarded on reopen.
func TestJSONLTornCommitDiscarded(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "agent.jsonl")

	st, err := OpenJSONL(path, false)
	if err != nil {
		t.Fatal(err)
	}
	id := mint(t, st, ctx)
	write(t, st, ctx, []StorageWrite{
		{Type: WriteEntry, Value: &EntryRecord{ID: id, ConversationID: 1, Kind: kindUser}},
	})
	if err := st.Close(ctx); err != nil {
		t.Fatal(err)
	}

	// Simulate a crash between appending a record and appending its marker.
	f, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	torn := EntryRecord{ID: id + 100, ConversationID: 1, Kind: kindUser}
	raw, _ := json.Marshal(torn)
	line, _ := json.Marshal(jsonlLine{Type: "entry", Value: raw})
	if _, err := f.Write(append(line, '\n')); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, err := OpenJSONL(path, false)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close(ctx)

	if e, _ := reopened.Entry(ctx, id+100); e != nil {
		t.Fatalf("torn write survived reopen: %+v", e)
	}
	if e, _ := reopened.Entry(ctx, id); e == nil {
		t.Fatal("committed entry was lost")
	}
}
