package tools

import (
	"os"
	"strings"
	"testing"
)

func TestEdit_SingleReplacement(t *testing.T) {
	root := t.TempDir()
	p := writeFile(t, root, "a.txt", "hello world\n")
	r := mustNew(t, Options{RootDir: root})

	got, err := call(t, r, "edit", editArgs{Path: "a.txt", Edits: []editReplacement{{OldText: "hello", NewText: "goodbye"}}})
	if err != nil {
		t.Fatalf("edit error = %v", err)
	}
	if got != "Successfully edited a.txt" {
		t.Fatalf("edit = %q", got)
	}
	b, _ := os.ReadFile(p)
	if string(b) != "goodbye world\n" {
		t.Fatalf("file = %q, want %q", b, "goodbye world\n")
	}
}

func TestEdit_MultipleEditsAgainstOriginal(t *testing.T) {
	root := t.TempDir()
	p := writeFile(t, root, "a.txt", "abc")
	r := mustNew(t, Options{RootDir: root})

	_, err := call(t, r, "edit", editArgs{Path: "a.txt", Edits: []editReplacement{
		{OldText: "a", NewText: "b"},
		{OldText: "b", NewText: "c"},
	}})
	if err != nil {
		t.Fatalf("edit error = %v", err)
	}
	// Matched against the original: a→b and b→c -> "bcc" (not incremental "ccc").
	b, _ := os.ReadFile(p)
	if string(b) != "bcc" {
		t.Fatalf("file = %q, want %q", b, "bcc")
	}
}

func TestEdit_OldTextNotFound(t *testing.T) {
	root := t.TempDir()
	writeFile(t, root, "a.txt", "abc")
	r := mustNew(t, Options{RootDir: root})

	_, err := call(t, r, "edit", editArgs{Path: "a.txt", Edits: []editReplacement{{OldText: "zzz", NewText: "x"}}})
	if err == nil || !strings.Contains(err.Error(), "not found") {
		t.Fatalf("edit error = %v, want not-found error", err)
	}
}

func TestEdit_OldTextNotUnique(t *testing.T) {
	root := t.TempDir()
	writeFile(t, root, "a.txt", "a a")
	r := mustNew(t, Options{RootDir: root})

	_, err := call(t, r, "edit", editArgs{Path: "a.txt", Edits: []editReplacement{{OldText: "a", NewText: "b"}}})
	if err == nil || !strings.Contains(err.Error(), "not unique") {
		t.Fatalf("edit error = %v, want not-unique error", err)
	}
}

func TestEdit_OverlappingEdits(t *testing.T) {
	root := t.TempDir()
	writeFile(t, root, "a.txt", "abc")
	r := mustNew(t, Options{RootDir: root})

	_, err := call(t, r, "edit", editArgs{Path: "a.txt", Edits: []editReplacement{
		{OldText: "ab", NewText: "x"},
		{OldText: "bc", NewText: "y"},
	}})
	if err == nil || !strings.Contains(err.Error(), "overlap") {
		t.Fatalf("edit error = %v, want overlap error", err)
	}
}

func TestEdit_EmptyOldText(t *testing.T) {
	root := t.TempDir()
	writeFile(t, root, "a.txt", "abc")
	r := mustNew(t, Options{RootDir: root})

	_, err := call(t, r, "edit", editArgs{Path: "a.txt", Edits: []editReplacement{{OldText: "", NewText: "x"}}})
	if err == nil || !strings.Contains(err.Error(), "non-empty") {
		t.Fatalf("edit error = %v, want non-empty error", err)
	}
}
