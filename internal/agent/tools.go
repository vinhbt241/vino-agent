package agent

import (
	"context"
	"encoding/json"
)

// ReplayMode declares whether a tool is safe to re-run after a crash
// interrupts its execution.
type ReplayMode string

const (
	// ReplayUnsafe (the zero value) means a crash-interrupted call is never
	// re-run; the model is told it was interrupted instead.
	ReplayUnsafe ReplayMode = ""
	// ReplaySafe means a crash-interrupted call may be re-run because it has
	// no observable side effects beyond its result.
	ReplaySafe ReplayMode = "safe"
)

// ToolSpec describes a tool's callable signature. Parameters is a JSON Schema
// for the tool's arguments.
type ToolSpec struct {
	Name        string
	Description string
	Parameters  json.RawMessage
	Replay      ReplayMode
}

// ToolRegistry exposes the tools available to the agent and executes them.
type ToolRegistry interface {
	Specs() []ToolSpec
	Call(ctx context.Context, name string, args json.RawMessage) (string, error)
}
