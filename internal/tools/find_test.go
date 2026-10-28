package tools

import (
	"os/exec"
	"strings"
	"testing"
)

func requireFd(t *testing.T) {
	t.Helper()
	if _, err := exec.LookPath("fd"); err != nil {
		t.Skip("fd not available")
	}
}

func TestFind_MatchesAndRespectsGitignore(t *testing.T) {
	requireFd(t)
	root := t.TempDir()
	writeFile(t, root, "a.txt", "x")
	writeFile(t, root, "sub/b.txt", "x")
	writeFile(t, root, "secret.txt", "x")
	writeFile(t, root, "c.go", "x")
	writeFile(t, root, ".gitignore", "secret.txt\n")
	r := mustNew(t, Options{RootDir: root})

	got, err := call(t, r, "find", findArgs{Pattern: "*.txt"})
	if err != nil {
		t.Fatalf("find error = %v", err)
	}
	if got != "a.txt\nsub/b.txt" {
		t.Fatalf("find = %q, want %q", got, "a.txt\nsub/b.txt")
	}
}

func TestFind_NoMatches(t *testing.T) {
	requireFd(t)
	root := t.TempDir()
	writeFile(t, root, "a.txt", "x")
	r := mustNew(t, Options{RootDir: root})

	got, err := call(t, r, "find", findArgs{Pattern: "*.md"})
	if err != nil {
		t.Fatalf("find error = %v", err)
	}
	if got != "No files found matching pattern" {
		t.Fatalf("find = %q, want %q", got, "No files found matching pattern")
	}
}

func TestFind_Subdir(t *testing.T) {
	requireFd(t)
	root := t.TempDir()
	writeFile(t, root, "sub/b.txt", "x")
	writeFile(t, root, "a.txt", "x")
	r := mustNew(t, Options{RootDir: root})

	got, err := call(t, r, "find", findArgs{Pattern: "*.txt", Path: "sub"})
	if err != nil {
		t.Fatalf("find error = %v", err)
	}
	if got != "sub/b.txt" {
		t.Fatalf("find = %q, want %q", got, "sub/b.txt")
	}
}

func TestFind_Limit(t *testing.T) {
	requireFd(t)
	root := t.TempDir()
	writeFile(t, root, "a.txt", "x")
	writeFile(t, root, "b.txt", "x")
	writeFile(t, root, "c.txt", "x")
	r := mustNew(t, Options{RootDir: root})

	got, err := call(t, r, "find", findArgs{Pattern: "*.txt", Limit: ptr(2)})
	if err != nil {
		t.Fatalf("find error = %v", err)
	}
	if !strings.Contains(got, "2 results limit reached") {
		t.Fatalf("find = %q, want limit notice", got)
	}
	pathLines := 0
	for _, line := range strings.Split(got, "\n") {
		if line != "" && !strings.Contains(line, "limit reached") {
			pathLines++
		}
	}
	if pathLines != 2 {
		t.Fatalf("find = %q, want exactly 2 path lines", got)
	}
}
