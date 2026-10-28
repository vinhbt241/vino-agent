// Package tools provides the concrete agent.ToolRegistry for the durable
// agent: filesystem and shell tools, mirroring Pi's built-in tool set.
package tools

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"

	"vino/internal/agent"
)

// Options configures a Registry.
type Options struct {
	// RootDir is the directory all path-taking tools are jailed to. Empty
	// means the process working directory.
	RootDir string
	// WorkDir is the working directory for shell tools. Empty means RootDir.
	WorkDir string
}

// Registry is a concrete agent.ToolRegistry. It is safe for concurrent use:
// tools hold no per-call state.
type Registry struct {
	rootDir string
	workDir string

	tools map[string]tool
	names []string
}

// tool is one callable tool: its declaration and its handler.
type tool struct {
	spec agent.ToolSpec
	run  func(ctx context.Context, args json.RawMessage) (string, error)
}

// New builds a Registry from opts. RootDir and WorkDir are resolved to
// absolute, cleaned paths.
func New(opts Options) (*Registry, error) {
	root := opts.RootDir
	if root == "" {
		wd, err := os.Getwd()
		if err != nil {
			return nil, fmt.Errorf("tools: resolving root dir: %w", err)
		}
		root = wd
	}
	rootAbs, err := filepath.Abs(root)
	if err != nil {
		return nil, fmt.Errorf("tools: resolving root dir: %w", err)
	}

	work := opts.WorkDir
	if work == "" {
		work = rootAbs
	} else {
		workAbs, err := filepath.Abs(work)
		if err != nil {
			return nil, fmt.Errorf("tools: resolving work dir: %w", err)
		}
		work = workAbs
	}

	r := &Registry{
		rootDir: filepath.Clean(rootAbs),
		workDir: filepath.Clean(work),
		tools:   make(map[string]tool),
	}
	for _, t := range r.allTools() {
		r.tools[t.spec.Name] = t
		r.names = append(r.names, t.spec.Name)
	}
	sort.Strings(r.names)
	return r, nil
}

// allTools returns the registry's full tool set. Names must be unique.
func (r *Registry) allTools() []tool {
	return []tool{
		{spec: safe(readSpec()), run: r.read},
		{spec: writeSpec(), run: r.write},
		{spec: editSpec(), run: r.edit},
		{spec: safe(lsSpec()), run: r.ls},
		{spec: bashSpec(), run: r.bash},
		{spec: safe(grepSpec()), run: r.grep},
		{spec: safe(findSpec()), run: r.find},
	}
}

// safe marks a read-only tool's spec as replay-safe after a crash. write,
// edit, and bash stay unsafe (the zero value).
func safe(spec agent.ToolSpec) agent.ToolSpec {
	spec.Replay = agent.ReplaySafe
	return spec
}

// Specs implements agent.ToolRegistry. It returns the tool declarations in
// name-sorted order.
func (r *Registry) Specs() []agent.ToolSpec {
	out := make([]agent.ToolSpec, 0, len(r.names))
	for _, name := range r.names {
		out = append(out, r.tools[name].spec)
	}
	return out
}

// Call implements agent.ToolRegistry. It dispatches to the named tool.
func (r *Registry) Call(ctx context.Context, name string, args json.RawMessage) (string, error) {
	t, ok := r.tools[name]
	if !ok {
		return "", fmt.Errorf("tools: unknown tool %q", name)
	}
	return t.run(ctx, args)
}

// decodeArgs unmarshals a tool call's argument JSON. A nil/empty payload is
// treated as an empty object so optional-args tools degrade gracefully.
func decodeArgs(args json.RawMessage, v any) error {
	if len(args) == 0 {
		args = json.RawMessage(`{}`)
	}
	return json.Unmarshal(args, v)
}
