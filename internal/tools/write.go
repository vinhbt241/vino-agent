package tools

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"

	"vino/internal/agent"
)

type writeArgs struct {
	Path    string `json:"path"`
	Content string `json:"content"`
}

func writeSpec() agent.ToolSpec {
	return agent.ToolSpec{
		Name: "write",
		Description: "Write content to a file. Creates the file if it doesn't " +
			"exist, overwrites if it does. Automatically creates parent directories.",
		Parameters: json.RawMessage(`{
			"type": "object",
			"properties": {
				"path":    {"type": "string", "description": "Path to the file to write (relative to the workspace root)"},
				"content": {"type": "string", "description": "Content to write to the file"}
			},
			"required": ["path", "content"],
			"additionalProperties": false
		}`),
	}
}

func (r *Registry) write(_ context.Context, args json.RawMessage) (string, error) {
	var in writeArgs
	if err := decodeArgs(args, &in); err != nil {
		return "", fmt.Errorf("tools: write: %w", err)
	}
	p, err := r.resolve(in.Path)
	if err != nil {
		return "", err
	}
	if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
		return "", err
	}
	if err := os.WriteFile(p, []byte(in.Content), 0o644); err != nil {
		return "", err
	}
	return fmt.Sprintf("Successfully wrote to %s", in.Path), nil
}
