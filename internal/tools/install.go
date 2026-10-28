package tools

import (
	"archive/tar"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
)

// userCacheDir is indirected so tests can point the cache elsewhere.
var userCacheDir = os.UserCacheDir

// releaseFile describes one prebuilt binary release archive.
type releaseFile struct {
	url    string
	sha256 string
}

// releaseTable returns the pinned release files for a tool, keyed by
// "GOOS/GOARCH". Checksums are embedded (see .scratch/tools/spec.md). It is a
// variable so tests can substitute a local release without hitting the network.
var releaseTable = defaultReleaseTable

func defaultReleaseTable(tool string) map[string]releaseFile {
	switch tool {
	case "rg":
		const v = "14.1.1"
		base := "https://github.com/BurntSushi/ripgrep/releases/download/" + v + "/ripgrep-" + v + "-"
		return map[string]releaseFile{
			"linux/amd64":  {url: base + "x86_64-unknown-linux-musl.tar.gz", sha256: "4cf9f2741e6c465ffdb7c26f38056a59e2a2544b51f7cc128ef28337eeae4d8e"},
			"linux/arm64":  {url: base + "aarch64-unknown-linux-gnu.tar.gz", sha256: "c827481c4ff4ea10c9dc7a4022c8de5db34a5737cb74484d62eb94a95841ab2f"},
			"darwin/amd64": {url: base + "x86_64-apple-darwin.tar.gz", sha256: "fc87e78f7cb3fea12d69072e7ef3b21509754717b746368fd40d88963630e2b3"},
			"darwin/arm64": {url: base + "aarch64-apple-darwin.tar.gz", sha256: "24ad76777745fbff131c8fbc466742b011f925bfa4fffa2ded6def23b5b937be"},
		}
	case "fd":
		const v = "10.2.0"
		base := "https://github.com/sharkdp/fd/releases/download/v" + v + "/fd-v" + v + "-"
		return map[string]releaseFile{
			"linux/amd64":  {url: base + "x86_64-unknown-linux-musl.tar.gz", sha256: "d9bfa25ec28624545c222992e1b00673b7c9ca5eb15393c40369f10b28f9c932"},
			"linux/arm64":  {url: base + "aarch64-unknown-linux-gnu.tar.gz", sha256: "6de8be7a3d8ca27954a6d1e22bc327af4cf6fc7622791e68b820197f915c422b"},
			"darwin/amd64": {url: base + "x86_64-apple-darwin.tar.gz", sha256: "991a648a58870230af9547c1ae33e72cb5c5199a622fe5e540e162d6dba82d48"},
			"darwin/arm64": {url: base + "aarch64-apple-darwin.tar.gz", sha256: "ae6327ba8c9a487cd63edd8bddd97da0207887a66d61e067dfe80c1430c5ae36"},
		}
	}
	return nil
}

// ensureBinary returns a path to the named tool binary (rg or fd): PATH first,
// then the cache dir, then a download-and-install into the cache dir.
func (r *Registry) ensureBinary(ctx context.Context, tool string) (string, error) {
	if p, err := exec.LookPath(tool); err == nil {
		return p, nil
	}

	cacheDir, err := userCacheDir()
	if err != nil {
		cacheDir = filepath.Join(os.TempDir(), "vino-tools")
	}
	dest := filepath.Join(cacheDir, "vino", "tools", tool)
	if isExecutable(dest) {
		return dest, nil
	}

	table := releaseTable(tool)
	if table == nil {
		return "", fmt.Errorf("tools: %s is required but has no install definition", tool)
	}
	rf, ok := table[runtime.GOOS+"/"+runtime.GOARCH]
	if !ok {
		return "", fmt.Errorf("tools: %s is required but could not be installed: unsupported platform %s/%s", tool, runtime.GOOS, runtime.GOARCH)
	}
	if err := install(ctx, tool, rf, dest); err != nil {
		return "", fmt.Errorf("tools: %s is required but could not be installed: %w", tool, err)
	}
	return dest, nil
}

// install downloads rf, verifies its checksum, and extracts the binary named
// tool into dest (0755).
func install(ctx context.Context, tool string, rf releaseFile, dest string) error {
	if err := os.MkdirAll(filepath.Dir(dest), 0o755); err != nil {
		return err
	}

	tmp, err := os.CreateTemp(filepath.Dir(dest), tool+".part-*")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	defer os.Remove(tmpName)

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, rf.url, nil)
	if err != nil {
		tmp.Close()
		return err
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		tmp.Close()
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		tmp.Close()
		return fmt.Errorf("download %s: %s", rf.url, resp.Status)
	}

	h := sha256.New()
	if _, err := io.Copy(io.MultiWriter(tmp, h), resp.Body); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	if got := hex.EncodeToString(h.Sum(nil)); got != rf.sha256 {
		return fmt.Errorf("checksum mismatch for %s: got %s want %s", rf.url, got, rf.sha256)
	}

	f, err := os.Open(tmpName)
	if err != nil {
		return err
	}
	defer f.Close()
	gz, err := gzip.NewReader(f)
	if err != nil {
		return err
	}
	defer gz.Close()

	tr := tar.NewReader(gz)
	for {
		hdr, err := tr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
		if hdr.Typeflag != tar.TypeReg || filepath.Base(hdr.Name) != tool {
			continue
		}
		return writeExecutable(dest, tr)
	}
	return fmt.Errorf("binary %q not found in archive", tool)
}

// writeExecutable writes r to dest atomically with mode 0755.
func writeExecutable(dest string, r io.Reader) error {
	tmp, err := os.CreateTemp(filepath.Dir(dest), "bin-*")
	if err != nil {
		return err
	}
	name := tmp.Name()
	defer os.Remove(name)
	if _, err := io.Copy(tmp, r); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Chmod(0o755); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	return os.Rename(name, dest)
}

func isExecutable(p string) bool {
	info, err := os.Stat(p)
	if err != nil {
		return false
	}
	return !info.IsDir() && info.Mode().Perm()&0o111 != 0
}
