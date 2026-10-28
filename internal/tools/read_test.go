package tools

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func writeFile(t *testing.T, root, name, content string) string {
	t.Helper()
	p := filepath.Join(root, name)
	if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
		t.Fatalf("mkdir %s: %v", filepath.Dir(p), err)
	}
	if err := os.WriteFile(p, []byte(content), 0o644); err != nil {
		t.Fatalf("write %s: %v", name, err)
	}
	return p
}

func TestRead_WholeFile(t *testing.T) {
	root := t.TempDir()
	writeFile(t, root, "a.txt", "line1\nline2\nline3\n")
	r := mustNew(t, Options{RootDir: root})

	got, err := call(t, r, "read", readArgs{Path: "a.txt"})
	if err != nil {
		t.Fatalf("read error = %v", err)
	}
	if got != "line1\nline2\nline3" {
		t.Fatalf("read = %q, want %q", got, "line1\nline2\nline3")
	}
}

func TestRead_OffsetAndLimit(t *testing.T) {
	root := t.TempDir()
	writeFile(t, root, "a.txt", "l1\nl2\nl3\n")
	r := mustNew(t, Options{RootDir: root})

	got, err := call(t, r, "read", readArgs{Path: "a.txt", Offset: ptr(2), Limit: ptr(2)})
	if err != nil {
		t.Fatalf("read error = %v", err)
	}
	if got != "l2\nl3" {
		t.Fatalf("read = %q, want %q", got, "l2\nl3")
	}
}

func TestRead_LimitTruncates(t *testing.T) {
	root := t.TempDir()
	writeFile(t, root, "a.txt", "l1\nl2\nl3\nl4\nl5\n")
	r := mustNew(t, Options{RootDir: root})

	got, err := call(t, r, "read", readArgs{Path: "a.txt", Limit: ptr(3)})
	if err != nil {
		t.Fatalf("read error = %v", err)
	}
	if !strings.HasPrefix(got, "l1\nl2\nl3") || !strings.Contains(got, "truncated") {
		t.Fatalf("read = %q, want first 3 lines + truncation notice", got)
	}
}

func TestRead_RejectsAbsolutePath(t *testing.T) {
	r := mustNew(t, Options{RootDir: t.TempDir()})
	_, err := call(t, r, "read", readArgs{Path: "/etc/hosts"})
	if err == nil || !strings.Contains(err.Error(), "absolute") {
		t.Fatalf("read error = %v, want absolute-path rejection", err)
	}
}

func TestRead_RejectsEscape(t *testing.T) {
	root := t.TempDir()
	writeFile(t, filepath.Dir(root), "outside.txt", "secret")
	r := mustNew(t, Options{RootDir: root})

	_, err := call(t, r, "read", readArgs{Path: "../outside.txt"})
	if err == nil || !strings.Contains(err.Error(), "escapes") {
		t.Fatalf("read error = %v, want escape rejection", err)
	}
}

func TestRead_MissingFile(t *testing.T) {
	r := mustNew(t, Options{RootDir: t.TempDir()})
	_, err := call(t, r, "read", readArgs{Path: "nope.txt"})
	if err == nil {
		t.Fatal("read error = nil, want not-exist error")
	}
}

func TestRead_InvalidOffset(t *testing.T) {
	r := mustNew(t, Options{RootDir: t.TempDir()})
	_, err := call(t, r, "read", readArgs{Path: "a.txt", Offset: ptr(0)})
	if err == nil || !strings.Contains(err.Error(), "offset") {
		t.Fatalf("read error = %v, want offset validation error", err)
	}
}

func ptr[T any](v T) *T { return &v }
