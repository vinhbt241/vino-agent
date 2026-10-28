package tools

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestWrite_CreatesFileAndParents(t *testing.T) {
	root := t.TempDir()
	r := mustNew(t, Options{RootDir: root})

	got, err := call(t, r, "write", writeArgs{Path: "dir/sub/f.txt", Content: "hello"})
	if err != nil {
		t.Fatalf("write error = %v", err)
	}
	if got != "Successfully wrote to dir/sub/f.txt" {
		t.Fatalf("write = %q", got)
	}
	b, err := os.ReadFile(filepath.Join(root, "dir", "sub", "f.txt"))
	if err != nil {
		t.Fatalf("read written file: %v", err)
	}
	if string(b) != "hello" {
		t.Fatalf("file content = %q, want %q", b, "hello")
	}
}

func TestWrite_Overwrites(t *testing.T) {
	root := t.TempDir()
	writeFile(t, root, "a.txt", "old")
	r := mustNew(t, Options{RootDir: root})

	if _, err := call(t, r, "write", writeArgs{Path: "a.txt", Content: "new"}); err != nil {
		t.Fatalf("write error = %v", err)
	}
	b, _ := os.ReadFile(filepath.Join(root, "a.txt"))
	if string(b) != "new" {
		t.Fatalf("file content = %q, want %q", b, "new")
	}
}

func TestWrite_RejectsEscape(t *testing.T) {
	r := mustNew(t, Options{RootDir: t.TempDir()})
	_, err := call(t, r, "write", writeArgs{Path: "../outside.txt", Content: "x"})
	if err == nil || !strings.Contains(err.Error(), "escapes") {
		t.Fatalf("write error = %v, want escape rejection", err)
	}
}
