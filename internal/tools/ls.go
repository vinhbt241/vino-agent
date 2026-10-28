package tools

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"

	"vino/internal/agent"
)

const defaultLsLimit = 500

type lsArgs struct {
	Path  string `json:"path"`
	Limit *int   `json:"limit"`
}

func lsSpec() agent.ToolSpec {
	return agent.ToolSpec{
		Name: "ls",
		Description: "List directory contents. Returns entries sorted alphabetically, " +
			"with '/' suffix for directories. Includes dotfiles.",
		Parameters: json.RawMessage(`{
			"type": "object",
			"properties": {
				"path":  {"type": "string", "description": "Directory to list (default: workspace root)"},
				"limit": {"type": "integer", "description": "Maximum number of entries to return (default: 500)"}
			},
			"additionalProperties": false
		}`),
	}
}

func (r *Registry) ls(_ context.Context, args json.RawMessage) (string, error) {
	var in lsArgs
	if err := decodeArgs(args, &in); err != nil {
		return "", fmt.Errorf("tools: ls: %w", err)
	}

	p := r.rootDir
	if in.Path != "" {
		resolved, err := r.resolve(in.Path)
		if err != nil {
			return "", err
		}
		p = resolved
	}

	limit := defaultLsLimit
	if in.Limit != nil {
		if *in.Limit < 0 {
			return "", fmt.Errorf("tools: ls: limit must be >= 0")
		}
		if *in.Limit > 0 {
			limit = *in.Limit
		}
	}

	entries, err := os.ReadDir(p)
	if err != nil {
		return "", err
	}
	if len(entries) == 0 {
		return "(empty directory)", nil
	}

	var b strings.Builder
	truncated := false
	for i, e := range entries {
		if i >= limit {
			truncated = true
			break
		}
		name := e.Name()
		if e.IsDir() {
			name += "/"
		}
		if b.Len() > 0 {
			b.WriteByte('\n')
		}
		b.WriteString(name)
	}
	if truncated {
		fmt.Fprintf(&b, "\n[truncated: %d more entries]", len(entries)-limit)
	}
	return b.String(), nil
}
