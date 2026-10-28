package tools

import (
	"context"
	"os"
	"path/filepath"
	"testing"
)

func TestNew_DefaultsRootToWorkingDir(t *testing.T) {
	r := mustNew(t, Options{})
	wd, err := os.Getwd()
	if err != nil {
		t.Fatalf("os.Getwd: %v", err)
	}
	if r.rootDir != wd {
		t.Fatalf("rootDir = %q, want %q", r.rootDir, wd)
	}
	if r.workDir != r.rootDir {
		t.Fatalf("workDir = %q, want rootDir %q", r.workDir, r.rootDir)
	}
}

func TestNew_ResolvesRootAndWorkDir(t *testing.T) {
	root := t.TempDir()
	work := t.TempDir()
	r := mustNew(t, Options{RootDir: root, WorkDir: work})
	if r.rootDir != filepath.Clean(root) {
		t.Fatalf("rootDir = %q, want %q", r.rootDir, filepath.Clean(root))
	}
	if r.workDir != filepath.Clean(work) {
		t.Fatalf("workDir = %q, want %q", r.workDir, filepath.Clean(work))
	}
}

func TestSpecs_SortedNames(t *testing.T) {
	r := mustNew(t, Options{RootDir: t.TempDir()})
	specs := r.Specs()
	want := []string{"bash", "edit", "find", "grep", "ls", "read", "write"}
	if len(specs) != len(want) {
		t.Fatalf("len(Specs()) = %d, want %d: %#v", len(specs), len(want), specs)
	}
	for i, name := range want {
		if specs[i].Name != name {
			t.Fatalf("Specs()[%d].Name = %q, want %q", i, specs[i].Name, name)
		}
		if specs[i].Parameters == nil {
			t.Fatalf("Specs()[%d].Parameters = nil, want JSON schema", i)
		}
	}
}

func TestCall_UnknownTool(t *testing.T) {
	r := mustNew(t, Options{RootDir: t.TempDir()})
	_, err := r.Call(context.Background(), "nope", nil)
	if err == nil {
		t.Fatal("Call(unknown) error = nil, want error")
	}
}
