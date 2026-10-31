package durable

import (
	"context"
	"encoding/json"

	"vino/internal/agent"
)

// ModelRequest is the BeforeModel payload: what the generation step is about
// to send. A hook may replace messages or tools.
type ModelRequest struct {
	Messages []agent.Message
	Tools    []agent.ToolSpec
}

// ToolOutcome is the AfterTool payload: the raw result of reg.Call, before it
// becomes the tool-result entry. Err is the tool's error message ("" on
// success).
type ToolOutcome struct {
	Result string
	Err    string
}

// Hooks is the set of hook points an extension may install. Each hook is a
// transform: it receives the payload and the step's runtime and returns the
// (possibly replaced) payload and an error.
type Hooks struct {
	BeforeModel func(context.Context, *TaskRuntime, ModelRequest) (ModelRequest, error)
	AfterModel  func(context.Context, *TaskRuntime, agent.Response) (agent.Response, error)
	BeforeTool  func(context.Context, *TaskRuntime, agent.ToolCall) (agent.ToolCall, error)
	AfterTool   func(context.Context, *TaskRuntime, ToolOutcome) (ToolOutcome, error)
}

// ToolWrapper decorates the resolved Call for one tool name. next reaches the
// winning provider under that name; a wrapper may transform args before next,
// transform the result after, short-circuit without calling next, or observe.
type ToolWrapper func(ctx context.Context, name string, args json.RawMessage,
	next func(context.Context, string, json.RawMessage) (string, error)) (string, error)
