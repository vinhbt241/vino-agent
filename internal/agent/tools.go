package agent

import (
	"context"
	"encoding/json"
)

// ToolSpec describes a tool's callable signature. Parameters is a JSON Schema
// for the tool's arguments.
type ToolSpec struct {
	Name        string
	Description string
	Parameters  json.RawMessage
}

// ToolRegistry exposes the tools available to the agent and executes them.
type ToolRegistry interface {
	Specs() []ToolSpec
	Call(ctx context.Context, name string, args json.RawMessage) (string, error)
}
