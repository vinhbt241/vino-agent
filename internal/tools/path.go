package tools

import (
	"fmt"
	"path/filepath"
	"strings"
)

// resolve maps a tool-supplied relative path onto an absolute path under
// rootDir. Absolute paths and paths escaping rootDir are rejected. The check
// is lexical (filepath.Clean + containment); symlink-hardening is deferred.
func (r *Registry) resolve(rel string) (string, error) {
	if rel == "" {
		return "", fmt.Errorf("tools: path is required")
	}
	if filepath.IsAbs(rel) {
		return "", fmt.Errorf("tools: absolute paths are not allowed: %q", rel)
	}
	p := filepath.Clean(filepath.Join(r.rootDir, rel))
	if !within(r.rootDir, p) {
		return "", fmt.Errorf("tools: path escapes root directory: %q", rel)
	}
	return p, nil
}

// within reports whether p is root itself or a descendant of root.
func within(root, p string) bool {
	rel, err := filepath.Rel(root, p)
	if err != nil {
		return false
	}
	return rel != ".." && !strings.HasPrefix(rel, ".."+string(filepath.Separator))
}

// searchPath resolves an optional tool-supplied path to a path usable as a
// search root for rg/fd, relative to rootDir (or "." for the whole root).
func (r *Registry) searchPath(pathArg string) (string, error) {
	if pathArg == "" {
		return ".", nil
	}
	abs, err := r.resolve(pathArg)
	if err != nil {
		return "", err
	}
	rel, err := filepath.Rel(r.rootDir, abs)
	if err != nil {
		return "", err
	}
	return rel, nil
}
