package tools

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"

	"vino/internal/agent"
)

const (
	defaultReadLines = 2000
	defaultReadBytes = 50 * 1024
)

type readArgs struct {
	Path   string `json:"path"`
	Offset *int   `json:"offset"`
	Limit  *int   `json:"limit"`
}

func readSpec() agent.ToolSpec {
	return agent.ToolSpec{
		Name: "read",
		Description: "Read the contents of a file. Output is truncated to 2000 " +
			"lines or 50KB (whichever is hit first). Use offset/limit for large files.",
		Parameters: json.RawMessage(`{
			"type": "object",
			"properties": {
				"path":   {"type": "string", "description": "Path to the file to read (relative to the workspace root)"},
				"offset": {"type": "integer", "description": "Line number to start reading from (1-indexed)"},
				"limit":  {"type": "integer", "description": "Maximum number of lines to read"}
			},
			"required": ["path"],
			"additionalProperties": false
		}`),
	}
}

func (r *Registry) read(_ context.Context, args json.RawMessage) (string, error) {
	var in readArgs
	if err := decodeArgs(args, &in); err != nil {
		return "", fmt.Errorf("tools: read: %w", err)
	}
	p, err := r.resolve(in.Path)
	if err != nil {
		return "", err
	}

	offset := 1
	if in.Offset != nil {
		offset = *in.Offset
	}
	if offset < 1 {
		return "", fmt.Errorf("tools: read: offset must be >= 1")
	}
	limit := defaultReadLines
	if in.Limit != nil {
		if *in.Limit < 0 {
			return "", fmt.Errorf("tools: read: limit must be >= 0")
		}
		if *in.Limit > 0 {
			limit = *in.Limit
		}
	}

	f, err := os.Open(p)
	if err != nil {
		return "", err
	}
	defer f.Close()

	scanner := bufio.NewScanner(f)
	scanner.Buffer(make([]byte, 0, 64*1024), 1024*1024)

	var b strings.Builder
	count := 0
	lineNo := 0
	truncated := false
	for scanner.Scan() {
		lineNo++
		if lineNo < offset {
			continue
		}
		if count == limit {
			truncated = true
			break
		}
		line := scanner.Text()
		if b.Len() > 0 && b.Len()+1+len(line) > defaultReadBytes {
			truncated = true
			break
		}
		if b.Len() > 0 {
			b.WriteByte('\n')
		}
		b.WriteString(line)
		count++
	}
	if err := scanner.Err(); err != nil {
		return "", err
	}

	out := b.String()
	if truncated {
		if out != "" {
			out += "\n"
		}
		out += "[truncated: use offset/limit to read more]"
	}
	return out, nil
}
