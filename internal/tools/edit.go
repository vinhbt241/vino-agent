package tools

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"sort"

	"vino/internal/agent"
)

type editReplacement struct {
	OldText string `json:"oldText"`
	NewText string `json:"newText"`
}

type editArgs struct {
	Path  string            `json:"path"`
	Edits []editReplacement `json:"edits"`
}

func editSpec() agent.ToolSpec {
	return agent.ToolSpec{
		Name: "edit",
		Description: "Edit a single file using exact text replacement. Every " +
			"edits[].oldText must match a unique, non-overlapping region of the " +
			"original file. If two changes affect the same block or nearby lines, " +
			"merge them into one edit.",
		Parameters: json.RawMessage(`{
			"type": "object",
			"properties": {
				"path": {"type": "string", "description": "Path to the file to edit (relative to the workspace root)"},
				"edits": {
					"type": "array",
					"description": "One or more targeted replacements, matched against the original file",
					"items": {
						"type": "object",
						"properties": {
							"oldText": {"type": "string", "description": "Exact text for one targeted replacement. Must be unique in the original file."},
							"newText": {"type": "string", "description": "Replacement text for this targeted edit."}
						},
						"required": ["oldText", "newText"],
						"additionalProperties": false
					}
				}
			},
			"required": ["path", "edits"],
			"additionalProperties": false
		}`),
	}
}

func (r *Registry) edit(_ context.Context, args json.RawMessage) (string, error) {
	var in editArgs
	if err := decodeArgs(args, &in); err != nil {
		return "", fmt.Errorf("tools: edit: %w", err)
	}
	if len(in.Edits) == 0 {
		return "", fmt.Errorf("tools: edit: edits is required")
	}
	p, err := r.resolve(in.Path)
	if err != nil {
		return "", err
	}

	orig, err := os.ReadFile(p)
	if err != nil {
		return "", err
	}
	orig = bytes.TrimPrefix(orig, []byte{0xEF, 0xBB, 0xBF}) // strip UTF-8 BOM

	type replacement struct {
		start, end int
		newText    string
	}
	repls := make([]replacement, 0, len(in.Edits))
	for i, e := range in.Edits {
		if e.OldText == "" {
			return "", fmt.Errorf("tools: edit: edits[%d].oldText must be non-empty", i)
		}
		needle := []byte(e.OldText)
		n := bytes.Count(orig, needle)
		if n == 0 {
			return "", fmt.Errorf("tools: edit: edits[%d].oldText not found", i)
		}
		if n > 1 {
			return "", fmt.Errorf("tools: edit: edits[%d].oldText is not unique (%d occurrences)", i, n)
		}
		start := bytes.Index(orig, needle)
		end := start + len(needle)
		for _, rp := range repls {
			if start < rp.end && end > rp.start {
				return "", fmt.Errorf("tools: edit: edits overlap")
			}
		}
		repls = append(repls, replacement{start: start, end: end, newText: e.NewText})
	}

	sort.Slice(repls, func(i, j int) bool { return repls[i].start < repls[j].start })

	var out bytes.Buffer
	prev := 0
	for _, rp := range repls {
		out.Write(orig[prev:rp.start])
		out.WriteString(rp.newText)
		prev = rp.end
	}
	out.Write(orig[prev:])

	if err := os.WriteFile(p, out.Bytes(), 0o644); err != nil {
		return "", err
	}
	return fmt.Sprintf("Successfully edited %s", in.Path), nil
}
