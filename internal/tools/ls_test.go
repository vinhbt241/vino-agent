package tools

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestLs_SortedWithDirSuffixAndDotfiles(t *testing.T) {
	root := t.TempDir()
	writeFile(t, root, "b.txt", "x")
	writeFile(t, root, "a.txt", "x")
	writeFile(t, root, ".hidden", "x")
	if err := os.MkdirAll(filepath.Join(root, "sub"), 0o755); err != nil {
		t.Fatalf("mkdir sub: %v", err)
	}
	r := mustNew(t, Options{RootDir: root})

	got, err := call(t, r, "ls", nil)
	if err != nil {
		t.Fatalf("ls error = %v", err)
	}
	want := ".hidden\na.txt\nb.txt\nsub/"
	if got != want {
		t.Fatalf("ls = %q, want %q", got, want)
	}
}

func TestLs_EmptyDir(t *testing.T) {
	r := mustNew(t, Options{RootDir: t.TempDir()})
	got, err := call(t, r, "ls", nil)
	if err != nil {
		t.Fatalf("ls error = %v", err)
	}
	if got != "(empty directory)" {
		t.Fatalf("ls = %q, want %q", got, "(empty directory)")
	}
}

func TestLs_LimitTruncates(t *testing.T) {
	root := t.TempDir()
	writeFile(t, root, "a.txt", "x")
	writeFile(t, root, "b.txt", "x")
	writeFile(t, root, "c.txt", "x")
	r := mustNew(t, Options{RootDir: root})

	got, err := call(t, r, "ls", lsArgs{Limit: ptr(2)})
	if err != nil {
		t.Fatalf("ls error = %v", err)
	}
	if !strings.HasPrefix(got, "a.txt\nb.txt") || !strings.Contains(got, "truncated") {
		t.Fatalf("ls = %q, want first 2 entries + truncation notice", got)
	}
}

func TestLs_Subdir(t *testing.T) {
	root := t.TempDir()
	writeFile(t, root, "sub/x.txt", "x")
	r := mustNew(t, Options{RootDir: root})

	got, err := call(t, r, "ls", lsArgs{Path: "sub"})
	if err != nil {
		t.Fatalf("ls error = %v", err)
	}
	if got != "x.txt" {
		t.Fatalf("ls = %q, want %q", got, "x.txt")
	}
}

func TestLs_RejectsAbsolutePath(t *testing.T) {
	r := mustNew(t, Options{RootDir: t.TempDir()})
	_, err := call(t, r, "ls", lsArgs{Path: "/etc"})
	if err == nil || !strings.Contains(err.Error(), "absolute") {
		t.Fatalf("ls error = %v, want absolute-path rejection", err)
	}
}
