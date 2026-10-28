package tools

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestBash_Stdout(t *testing.T) {
	r := mustNew(t, Options{RootDir: t.TempDir()})
	got, err := call(t, r, "bash", bashArgs{Command: "printf hello"})
	if err != nil {
		t.Fatalf("bash error = %v", err)
	}
	if got != "hello" {
		t.Fatalf("bash = %q, want %q", got, "hello")
	}
}

func TestBash_StderrAppended(t *testing.T) {
	r := mustNew(t, Options{RootDir: t.TempDir()})
	got, err := call(t, r, "bash", bashArgs{Command: "printf out; printf err >&2"})
	if err != nil {
		t.Fatalf("bash error = %v", err)
	}
	if got != "out\nerr" {
		t.Fatalf("bash = %q, want %q", got, "out\nerr")
	}
}

func TestBash_NonZeroExitIsError(t *testing.T) {
	r := mustNew(t, Options{RootDir: t.TempDir()})
	_, err := call(t, r, "bash", bashArgs{Command: "printf boom >&2; exit 3"})
	if err == nil || !strings.Contains(err.Error(), "exit status 3") || !strings.Contains(err.Error(), "boom") {
		t.Fatalf("bash error = %v, want exit status 3 + stderr", err)
	}
}

func TestBash_Timeout(t *testing.T) {
	r := mustNew(t, Options{RootDir: t.TempDir()})
	_, err := call(t, r, "bash", bashArgs{Command: "sleep 5", Timeout: ptr(1)})
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("bash error = %v, want context.DeadlineExceeded", err)
	}
}

func TestBash_EmptyCommand(t *testing.T) {
	r := mustNew(t, Options{RootDir: t.TempDir()})
	_, err := call(t, r, "bash", bashArgs{Command: ""})
	if err == nil || !strings.Contains(err.Error(), "required") {
		t.Fatalf("bash error = %v, want required error", err)
	}
}

func TestBash_UsesWorkDir(t *testing.T) {
	root := t.TempDir()
	work := filepath.Join(root, "sub")
	if err := mkdirAll(work); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	r := mustNew(t, Options{RootDir: root, WorkDir: work})

	got, err := call(t, r, "bash", bashArgs{Command: "pwd"})
	if err != nil {
		t.Fatalf("bash error = %v", err)
	}
	if strings.TrimSpace(got) != work {
		t.Fatalf("pwd = %q, want %q", got, work)
	}
}

func mkdirAll(p string) error {
	return os.MkdirAll(p, 0o755)
}
