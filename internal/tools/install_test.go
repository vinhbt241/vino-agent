package tools

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// buildTarGz returns a gzipped tarball of name->content regular files.
func buildTarGz(t *testing.T, files map[string]string) []byte {
	t.Helper()
	var buf bytes.Buffer
	gz := gzip.NewWriter(&buf)
	tw := tar.NewWriter(gz)
	for name, content := range files {
		if err := tw.WriteHeader(&tar.Header{
			Name:     name,
			Mode:     0o755,
			Size:     int64(len(content)),
			Typeflag: tar.TypeReg,
		}); err != nil {
			t.Fatalf("tar header: %v", err)
		}
		if _, err := tw.Write([]byte(content)); err != nil {
			t.Fatalf("tar write: %v", err)
		}
	}
	if err := tw.Close(); err != nil {
		t.Fatalf("tar close: %v", err)
	}
	if err := gz.Close(); err != nil {
		t.Fatalf("gzip close: %v", err)
	}
	return buf.Bytes()
}

func sha256Hex(b []byte) string {
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}

func TestInstall_ExtractsAndMakesExecutable(t *testing.T) {
	archive := buildTarGz(t, map[string]string{"pkg/rg": "#!/bin/sh\necho fake-rg\n"})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Write(archive)
	}))
	defer srv.Close()

	dest := filepath.Join(t.TempDir(), "bin", "rg")
	err := install(context.Background(), "rg", releaseFile{url: srv.URL + "/rg.tar.gz", sha256: sha256Hex(archive)}, dest)
	if err != nil {
		t.Fatalf("install error = %v", err)
	}
	info, err := os.Stat(dest)
	if err != nil {
		t.Fatalf("stat installed binary: %v", err)
	}
	if info.Mode().Perm()&0o111 == 0 {
		t.Fatalf("installed binary mode = %v, want executable", info.Mode())
	}
	b, err := os.ReadFile(dest)
	if err != nil {
		t.Fatalf("read installed binary: %v", err)
	}
	if !strings.Contains(string(b), "fake-rg") {
		t.Fatalf("installed content = %q", b)
	}
}

func TestInstall_ChecksumMismatch(t *testing.T) {
	archive := buildTarGz(t, map[string]string{"pkg/rg": "x"})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Write(archive)
	}))
	defer srv.Close()

	dest := filepath.Join(t.TempDir(), "bin", "rg")
	err := install(context.Background(), "rg", releaseFile{url: srv.URL + "/rg.tar.gz", sha256: strings.Repeat("0", 64)}, dest)
	if err == nil || !strings.Contains(err.Error(), "checksum mismatch") {
		t.Fatalf("install error = %v, want checksum mismatch", err)
	}
	if _, statErr := os.Stat(dest); statErr == nil {
		t.Fatal("dest exists after checksum mismatch, want absent")
	}
}

func TestInstall_HTTPError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNotFound)
	}))
	defer srv.Close()

	err := install(context.Background(), "rg", releaseFile{url: srv.URL + "/rg.tar.gz", sha256: "x"}, filepath.Join(t.TempDir(), "rg"))
	if err == nil || !strings.Contains(err.Error(), "404") {
		t.Fatalf("install error = %v, want download/status error", err)
	}
}

func TestInstall_BinaryNotFoundInArchive(t *testing.T) {
	archive := buildTarGz(t, map[string]string{"pkg/other": "x"})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Write(archive)
	}))
	defer srv.Close()

	err := install(context.Background(), "rg", releaseFile{url: srv.URL + "/rg.tar.gz", sha256: sha256Hex(archive)}, filepath.Join(t.TempDir(), "rg"))
	if err == nil || !strings.Contains(err.Error(), "not found in archive") {
		t.Fatalf("install error = %v, want binary-not-found", err)
	}
}

func TestEnsureBinary_CacheHit(t *testing.T) {
	t.Setenv("PATH", t.TempDir()) // force LookPath to miss
	cache := t.TempDir()
	old := userCacheDir
	userCacheDir = func() (string, error) { return cache, nil }
	t.Cleanup(func() { userCacheDir = old })

	dest := filepath.Join(cache, "vino", "tools", "rg")
	if err := os.MkdirAll(filepath.Dir(dest), 0o755); err != nil {
		t.Fatalf("mkdir cache: %v", err)
	}
	if err := os.WriteFile(dest, []byte("#!/bin/sh\n"), 0o755); err != nil {
		t.Fatalf("write cached binary: %v", err)
	}

	r := mustNew(t, Options{RootDir: t.TempDir()})
	got, err := r.ensureBinary(context.Background(), "rg")
	if err != nil {
		t.Fatalf("ensureBinary error = %v", err)
	}
	if got != dest {
		t.Fatalf("ensureBinary = %q, want %q", got, dest)
	}
}

func TestEnsureBinary_InstallFailureWrapsError(t *testing.T) {
	t.Setenv("PATH", t.TempDir()) // force LookPath to miss
	cache := t.TempDir()
	old := userCacheDir
	userCacheDir = func() (string, error) { return cache, nil }
	t.Cleanup(func() { userCacheDir = old })

	// No cache hit, and the release table is pointed at a failing server.
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer srv.Close()
	oldTable := releaseTable
	releaseTable = func(string) map[string]releaseFile {
		return map[string]releaseFile{
			"linux/amd64": {url: srv.URL + "/rg.tar.gz", sha256: "x"},
		}
	}
	t.Cleanup(func() { releaseTable = oldTable })

	r := mustNew(t, Options{RootDir: t.TempDir()})
	_, err := r.ensureBinary(context.Background(), "rg")
	if err == nil || !strings.Contains(err.Error(), "could not be installed") {
		t.Fatalf("ensureBinary error = %v, want could-not-be-installed", err)
	}
}
