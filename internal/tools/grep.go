package tools

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os/exec"
	"strconv"
	"strings"

	"vino/internal/agent"
)

const (
	defaultGrepLimit = 100
	maxOutputLineLen = 200
)

type grepArgs struct {
	Pattern    string `json:"pattern"`
	Path       string `json:"path"`
	Glob       string `json:"glob"`
	IgnoreCase bool   `json:"ignoreCase"`
	Literal    bool   `json:"literal"`
	Context    int    `json:"context"`
	Limit      *int   `json:"limit"`
}

func grepSpec() agent.ToolSpec {
	return agent.ToolSpec{
		Name: "grep",
		Description: "Search file contents for a pattern. Returns matching lines " +
			"with file paths and line numbers. Respects .gitignore. Output is truncated " +
			"to 100 matches.",
		Parameters: json.RawMessage(`{
			"type": "object",
			"properties": {
				"pattern":    {"type": "string", "description": "Search pattern (regex or literal string)"},
				"path":       {"type": "string", "description": "Directory or file to search (default: workspace root)"},
				"glob":       {"type": "string", "description": "Filter files by glob pattern, e.g. '*.ts' or '**/*.spec.ts'"},
				"ignoreCase": {"type": "boolean", "description": "Case-insensitive search (default: false)"},
				"literal":    {"type": "boolean", "description": "Treat pattern as literal string instead of regex (default: false)"},
				"context":    {"type": "integer", "description": "Number of lines to show before and after each match (default: 0)"},
				"limit":      {"type": "integer", "description": "Maximum number of matches to return (default: 100)"}
			},
			"required": ["pattern"],
			"additionalProperties": false
		}`),
	}
}

// rgEvent is one JSON line of `rg --json` output.
type rgEvent struct {
	Type string `json:"type"`
	Data struct {
		Path struct {
			Text string `json:"text"`
		} `json:"path"`
		Lines struct {
			Text string `json:"text"`
		} `json:"lines"`
		LineNumber int `json:"line_number"`
	} `json:"data"`
}

func (r *Registry) grep(ctx context.Context, args json.RawMessage) (string, error) {
	var in grepArgs
	if err := decodeArgs(args, &in); err != nil {
		return "", fmt.Errorf("tools: grep: %w", err)
	}
	if in.Pattern == "" {
		return "", fmt.Errorf("tools: grep: pattern is required")
	}
	limit := defaultGrepLimit
	if in.Limit != nil {
		if *in.Limit < 0 {
			return "", fmt.Errorf("tools: grep: limit must be >= 0")
		}
		if *in.Limit > 0 {
			limit = *in.Limit
		}
	}
	if in.Context < 0 {
		return "", fmt.Errorf("tools: grep: context must be >= 0")
	}

	rg, err := r.ensureBinary(ctx, "rg")
	if err != nil {
		return "", err
	}
	searchPath, err := r.searchPath(in.Path)
	if err != nil {
		return "", err
	}

	cmdArgs := []string{"--no-config", "--json", "--line-number", "--color=never", "--hidden", "--no-require-git"}
	if in.IgnoreCase {
		cmdArgs = append(cmdArgs, "--ignore-case")
	}
	if in.Literal {
		cmdArgs = append(cmdArgs, "--fixed-strings")
	}
	if in.Glob != "" {
		cmdArgs = append(cmdArgs, "--glob", in.Glob)
	}
	if in.Context > 0 {
		cmdArgs = append(cmdArgs, "-C", strconv.Itoa(in.Context))
	}
	cmdArgs = append(cmdArgs, "--", in.Pattern, searchPath)

	cmd := exec.CommandContext(ctx, rg, cmdArgs...)
	cmd.Dir = r.rootDir
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return "", err
	}
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	if err := cmd.Start(); err != nil {
		return "", err
	}

	kill := func() {
		if cmd.Process != nil {
			_ = cmd.Process.Kill()
		}
	}

	scanner := bufio.NewScanner(stdout)
	scanner.Buffer(make([]byte, 0, 64*1024), 4*1024*1024)

	var out []string
	matchCount := 0
	limitReached := false
	var readErr error

readLoop:
	for scanner.Scan() {
		var ev rgEvent
		if err := json.Unmarshal(scanner.Bytes(), &ev); err != nil {
			readErr = fmt.Errorf("tools: grep: decoding rg output: %w", err)
			kill()
			break readLoop
		}
		switch ev.Type {
		case "match":
			if matchCount >= limit {
				limitReached = true
				kill()
				break readLoop
			}
			matchCount++
			out = append(out, formatMatch(&ev))
		case "context":
			out = append(out, formatContext(&ev))
		}
	}
	if readErr == nil {
		if err := scanner.Err(); err != nil {
			readErr = fmt.Errorf("tools: grep: reading rg output: %w", err)
			kill()
		}
	}

	waitErr := cmd.Wait()
	if readErr != nil {
		return "", readErr
	}
	if waitErr != nil && ctx.Err() == nil && !limitReached {
		// rg exits 1 when there are no matches, which is a normal empty result.
		var exitErr *exec.ExitError
		if !(errors.As(waitErr, &exitErr) && exitErr.ExitCode() == 1) {
			return "", fmt.Errorf("tools: grep: %s", strings.TrimSpace(stderr.String()))
		}
	}

	if len(out) == 0 {
		return "No matches found", nil
	}
	res := strings.Join(out, "\n")
	if limitReached {
		res += fmt.Sprintf("\n%d matches limit reached. Use limit=%d for more, or refine pattern", limit, limit*2)
	}
	return res, nil
}

func cleanPath(p string) string {
	return strings.TrimPrefix(p, "./")
}

func truncateLine(s string) string {
	s = strings.TrimRight(s, "\r\n")
	if len(s) <= maxOutputLineLen {
		return s
	}
	return s[:maxOutputLineLen] + "…"
}

func formatMatch(ev *rgEvent) string {
	return fmt.Sprintf("%s:%d:%s", cleanPath(ev.Data.Path.Text), ev.Data.LineNumber, truncateLine(ev.Data.Lines.Text))
}

func formatContext(ev *rgEvent) string {
	return fmt.Sprintf("%s-%d-%s", cleanPath(ev.Data.Path.Text), ev.Data.LineNumber, truncateLine(ev.Data.Lines.Text))
}
