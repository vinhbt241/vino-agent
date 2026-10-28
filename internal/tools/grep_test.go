package tools

import (
	"os/exec"
	"strings"
	"testing"
)

func requireRg(t *testing.T) {
	t.Helper()
	if _, err := exec.LookPath("rg"); err != nil {
		t.Skip("rg not available")
	}
}

func TestGrep_MatchesAndRespectsGitignore(t *testing.T) {
	requireRg(t)
	root := t.TempDir()
	writeFile(t, root, "a.txt", "hello world\n")
	writeFile(t, root, "sub/b.txt", "hello again\n")
	writeFile(t, root, "secret.txt", "hello secret\n")
	writeFile(t, root, ".gitignore", "secret.txt\n")
	r := mustNew(t, Options{RootDir: root})

	got, err := call(t, r, "grep", grepArgs{Pattern: "hello"})
	if err != nil {
		t.Fatalf("grep error = %v", err)
	}
	if !strings.Contains(got, "a.txt:1:hello world") {
		t.Fatalf("grep = %q, want a.txt match", got)
	}
	if !strings.Contains(got, "sub/b.txt:1:hello again") {
		t.Fatalf("grep = %q, want sub/b.txt match", got)
	}
	if strings.Contains(got, "secret.txt") {
		t.Fatalf("grep = %q, must not include gitignored secret.txt", got)
	}
}

func TestGrep_NoMatches(t *testing.T) {
	requireRg(t)
	root := t.TempDir()
	writeFile(t, root, "a.txt", "hello\n")
	r := mustNew(t, Options{RootDir: root})

	got, err := call(t, r, "grep", grepArgs{Pattern: "zzz"})
	if err != nil {
		t.Fatalf("grep error = %v", err)
	}
	if got != "No matches found" {
		t.Fatalf("grep = %q, want %q", got, "No matches found")
	}
}

func TestGrep_Limit(t *testing.T) {
	requireRg(t)
	root := t.TempDir()
	writeFile(t, root, "a.txt", "hello\n")
	writeFile(t, root, "b.txt", "hello\n")
	writeFile(t, root, "c.txt", "hello\n")
	r := mustNew(t, Options{RootDir: root})

	got, err := call(t, r, "grep", grepArgs{Pattern: "hello", Limit: ptr(1)})
	if err != nil {
		t.Fatalf("grep error = %v", err)
	}
	if !strings.Contains(got, "1 matches limit reached") {
		t.Fatalf("grep = %q, want limit notice", got)
	}
	matchLines := 0
	for _, line := range strings.Split(got, "\n") {
		if strings.Contains(line, ":1:hello") {
			matchLines++
		}
	}
	if matchLines != 1 {
		t.Fatalf("grep = %q, want exactly 1 match line", got)
	}
}

func TestGrep_Literal(t *testing.T) {
	requireRg(t)
	root := t.TempDir()
	writeFile(t, root, "x.txt", "a.b\naxb\n")
	r := mustNew(t, Options{RootDir: root})

	got, err := call(t, r, "grep", grepArgs{Pattern: "a.b", Literal: true})
	if err != nil {
		t.Fatalf("grep error = %v", err)
	}
	if !strings.Contains(got, ":1:a.b") || strings.Contains(got, ":2:axb") {
		t.Fatalf("grep = %q, want literal a.b only", got)
	}
}

func TestGrep_RejectsAbsolutePath(t *testing.T) {
	requireRg(t)
	r := mustNew(t, Options{RootDir: t.TempDir()})
	_, err := call(t, r, "grep", grepArgs{Pattern: "x", Path: "/etc"})
	if err == nil || !strings.Contains(err.Error(), "absolute") {
		t.Fatalf("grep error = %v, want absolute-path rejection", err)
	}
}
