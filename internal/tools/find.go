package tools

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strconv"
	"strings"

	"vino/internal/agent"
)

const defaultFindLimit = 1000

type findArgs struct {
	Pattern string `json:"pattern"`
	Path    string `json:"path"`
	Limit   *int   `json:"limit"`
}

func findSpec() agent.ToolSpec {
	return agent.ToolSpec{
		Name: "find",
		Description: "Search for files by glob pattern. Returns matching file paths " +
			"relative to the search directory. Respects .gitignore.",
		Parameters: json.RawMessage(`{
			"type": "object",
			"properties": {
				"pattern": {"type": "string", "description": "Glob pattern to match files, e.g. '*.ts', '**/*.json', or 'src/**/*.spec.ts'"},
				"path":    {"type": "string", "description": "Directory to search in (default: workspace root)"},
				"limit":   {"type": "integer", "description": "Maximum number of results (default: 1000)"}
			},
			"required": ["pattern"],
			"additionalProperties": false
		}`),
	}
}

func (r *Registry) find(ctx context.Context, args json.RawMessage) (string, error) {
	var in findArgs
	if err := decodeArgs(args, &in); err != nil {
		return "", fmt.Errorf("tools: find: %w", err)
	}
	if in.Pattern == "" {
		return "", fmt.Errorf("tools: find: pattern is required")
	}
	limit := defaultFindLimit
	if in.Limit != nil {
		if *in.Limit < 0 {
			return "", fmt.Errorf("tools: find: limit must be >= 0")
		}
		if *in.Limit > 0 {
			limit = *in.Limit
		}
	}

	fd, err := r.ensureBinary(ctx, "fd")
	if err != nil {
		return "", err
	}
	searchPath, err := r.searchPath(in.Path)
	if err != nil {
		return "", err
	}

	cmdArgs := []string{"--glob", "--color=never", "--hidden"}
	if !insideGitRepo(r.rootDir, searchPath) {
		cmdArgs = append(cmdArgs, "--no-require-git")
	}
	cmdArgs = append(cmdArgs, "--max-results", strconv.Itoa(limit))

	pattern := in.Pattern
	if strings.Contains(pattern, "/") {
		cmdArgs = append(cmdArgs, "--full-path")
		if !strings.HasPrefix(pattern, "/") && !strings.HasPrefix(pattern, "**/") && pattern != "**" {
			pattern = "**/" + pattern
		}
	}
	cmdArgs = append(cmdArgs, "--", pattern, searchPath)

	cmd := exec.CommandContext(ctx, fd, cmdArgs...)
	cmd.Dir = r.rootDir
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		if ctx.Err() != nil {
			return "", ctx.Err()
		}
		return "", fmt.Errorf("tools: find: %s", strings.TrimSpace(stderr.String()))
	}

	var results []string
	for _, line := range strings.Split(stdout.String(), "\n") {
		line = cleanPath(strings.TrimSpace(line))
		if line != "" {
			results = append(results, line)
		}
	}
	if len(results) == 0 {
		return "No files found matching pattern", nil
	}

	sort.Strings(results)
	res := strings.Join(results, "\n")
	if len(results) >= limit {
		res += fmt.Sprintf("\n%d results limit reached. Use limit=%d for more, or refine pattern", limit, limit*2)
	}
	return res, nil
}

// insideGitRepo reports whether searchDir (under root) is inside a git
// repository, mirroring Pi's fd logic so fd's nested-repo boundary behavior is
// preserved inside repos while --no-require-git is applied outside them.
func insideGitRepo(root, search string) bool {
	dir := search
	if !filepath.IsAbs(dir) {
		dir = filepath.Join(root, dir)
	}
	dir = filepath.Clean(dir)
	for {
		if info, err := os.Stat(filepath.Join(dir, ".git")); err == nil && info.IsDir() {
			return true
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			return false
		}
		dir = parent
	}
}
