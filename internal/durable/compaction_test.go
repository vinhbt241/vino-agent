package durable

import (
	"context"
	"strings"
	"testing"

	"vino/internal/agent"
)

// latestCompactionEntry returns the most recent compaction entry in an
// ID-ordered entries slice, or nil when there is none.
func latestCompactionEntry(t *testing.T, entries []EntryRecord) *EntryRecord {
	t.Helper()
	for i := len(entries) - 1; i >= 0; i-- {
		if entries[i].Kind == kindCompaction {
			return &entries[i]
		}
	}
	return nil
}

func TestCompactSummarizesHistory(t *testing.T) {
	ctx := context.Background()
	llm := &fakeLLM{steps: []llmStep{
		{resp: agent.Response{Content: "first"}},
		{resp: agent.Response{Content: "second"}},
		{resp: agent.Response{Content: "summary text"}},
		{resp: agent.Response{Content: "third"}},
	}}
	h, err := Open(ctx, NewMemoryStorage(), HarnessOptions{LLM: llm})
	if err != nil {
		t.Fatal(err)
	}
	root := h.Root()

	for _, m := range []string{"one", "two"} {
		sub, err := root.Submit(ctx, SubmitInput{Content: m})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := sub.Wait(ctx); err != nil {
			t.Fatal(err)
		}
	}

	if err := root.Compact(ctx, "keep names"); err != nil {
		t.Fatal(err)
	}

	entries, err := root.Entries(ctx)
	if err != nil {
		t.Fatal(err)
	}
	comp := latestCompactionEntry(t, entries)
	if comp == nil {
		t.Fatal("no compaction entry written")
	}
	if comp.Head == nil {
		t.Fatal("compaction entry has no head")
	}
	if len(comp.Model) != 1 || comp.Model[0].Content != "summary text" {
		t.Fatalf("compaction summary = %#v, want system message \"summary text\"", comp.Model)
	}
	if want := entries[3].ID; *comp.Head != want {
		t.Fatalf("head = %d, want %d (last pre-compaction entry)", *comp.Head, want)
	}

	// The next run's context is the summary plus the new turn — not the
	// summarized turns.
	sub, err := root.Submit(ctx, SubmitInput{Content: "three"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := sub.Wait(ctx); err != nil {
		t.Fatal(err)
	}

	if llm.calls != 4 {
		t.Fatalf("llm calls = %d, want 4", llm.calls)
	}
	gen := llm.messages[3]
	if len(gen) != 2 {
		t.Fatalf("generation context = %d messages, want 2 (summary + new turn): %#v", len(gen), gen)
	}
	if gen[0].Role != agent.RoleSystem || gen[0].Content != "summary text" {
		t.Fatalf("first context message = %#v, want system summary", gen[0])
	}
	if gen[1].Role != agent.RoleUser || gen[1].Content != "three" {
		t.Fatalf("second context message = %#v, want user \"three\"", gen[1])
	}
}

func TestCompactKeepsHistoryInStorage(t *testing.T) {
	ctx := context.Background()
	llm := &fakeLLM{steps: []llmStep{
		{resp: agent.Response{Content: "first"}},
		{resp: agent.Response{Content: "summary"}},
	}}
	h, err := Open(ctx, NewMemoryStorage(), HarnessOptions{LLM: llm})
	if err != nil {
		t.Fatal(err)
	}
	root := h.Root()

	sub, err := root.Submit(ctx, SubmitInput{Content: "one"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := sub.Wait(ctx); err != nil {
		t.Fatal(err)
	}
	if err := root.Compact(ctx, ""); err != nil {
		t.Fatal(err)
	}

	entries, err := root.Entries(ctx)
	if err != nil {
		t.Fatal(err)
	}
	// user "one" + assistant "first" + the compaction entry: nothing deleted.
	if len(entries) != 3 {
		t.Fatalf("entries = %d, want 3 (compacted history retained)", len(entries))
	}
	if entries[0].Kind != kindUser || entries[0].Model[0].Content != "one" {
		t.Fatalf("user entry lost: %#v", entries[0])
	}
	if entries[1].Kind != kindAssistant || entries[1].Model[0].Content != "first" {
		t.Fatalf("assistant entry lost: %#v", entries[1])
	}
}

func TestCompactEmptyConversationNoop(t *testing.T) {
	ctx := context.Background()
	llm := &fakeLLM{}
	h, err := Open(ctx, NewMemoryStorage(), HarnessOptions{LLM: llm})
	if err != nil {
		t.Fatal(err)
	}
	root := h.Root()

	if err := root.Compact(ctx, ""); err != nil {
		t.Fatal(err)
	}
	if llm.calls != 0 {
		t.Fatalf("llm calls = %d, want 0 (empty conversation must not summarize)", llm.calls)
	}
	entries, err := root.Entries(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 0 {
		t.Fatalf("entries = %d, want 0", len(entries))
	}
}

func TestCompactTwiceFoldsPriorSummary(t *testing.T) {
	ctx := context.Background()
	llm := &fakeLLM{steps: []llmStep{
		{resp: agent.Response{Content: "first"}},
		{resp: agent.Response{Content: "summary one"}},
		{resp: agent.Response{Content: "second"}},
		{resp: agent.Response{Content: "summary two"}},
		{resp: agent.Response{Content: "third"}},
	}}
	h, err := Open(ctx, NewMemoryStorage(), HarnessOptions{LLM: llm})
	if err != nil {
		t.Fatal(err)
	}
	root := h.Root()

	sub, err := root.Submit(ctx, SubmitInput{Content: "one"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := sub.Wait(ctx); err != nil {
		t.Fatal(err)
	}
	if err := root.Compact(ctx, ""); err != nil {
		t.Fatal(err)
	}

	sub, err = root.Submit(ctx, SubmitInput{Content: "two"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := sub.Wait(ctx); err != nil {
		t.Fatal(err)
	}
	if err := root.Compact(ctx, ""); err != nil {
		t.Fatal(err)
	}

	if llm.calls != 4 {
		t.Fatalf("llm calls = %d, want 4 (two submits + two compacts)", llm.calls)
	}
	// The second summarization call (index 3) must have seen the first summary.
	summarize := llm.messages[3]
	if len(summarize) != 4 {
		t.Fatalf("second summarize input = %d messages, want 4 (instr + prior summary + 2 turns): %#v", len(summarize), summarize)
	}
	if !strings.Contains(summarize[1].Content, "summary one") {
		t.Fatalf("second summarize input did not carry the prior summary: %#v", summarize[1])
	}

	// Only the newest summary appears in the next run's context.
	sub, err = root.Submit(ctx, SubmitInput{Content: "three"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := sub.Wait(ctx); err != nil {
		t.Fatal(err)
	}
	gen := llm.messages[4]
	if len(gen) != 2 {
		t.Fatalf("generation context = %d messages, want 2: %#v", len(gen), gen)
	}
	if gen[0].Content != "summary two" {
		t.Fatalf("context summary = %q, want \"summary two\"", gen[0].Content)
	}
	if gen[1].Content != "three" {
		t.Fatalf("context tail = %q, want \"three\"", gen[1].Content)
	}
}
