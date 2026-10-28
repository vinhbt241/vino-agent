package agent

import "encoding/json"

// EventType discriminates the kind of Event.
type EventType string

const (
	EventTextDelta   EventType = "text_delta"
	EventToolCall    EventType = "tool_call_started"
	EventToolResult  EventType = "tool_result"
	EventRunFinished EventType = "run_finished"
)

// Event is a single item in the run's output stream. Exactly one pointer field
// is set, matching Type.
type Event struct {
	Type        EventType
	TextDelta   *TextDelta
	ToolCall    *ToolCallEvent
	ToolResult  *ToolResultEvent
	RunFinished *RunFinishedEvent
}

// TextDelta is a chunk of streamed assistant text.
type TextDelta struct {
	Text string
}

// ToolCallEvent announces that a tool call is about to be executed.
type ToolCallEvent struct {
	ID        string
	Name      string
	Arguments json.RawMessage
}

// ToolResultEvent reports the outcome of a tool call. On failure, Error is set
// and Result may be empty.
type ToolResultEvent struct {
	ID     string
	Name   string
	Result string
	Error  string
}

// RunFinishedEvent marks the end of a run with the final assistant message.
type RunFinishedEvent struct {
	FinalMessage Message
}

// EventSink consumes run events. Returning a non-nil error aborts the run.
type EventSink func(Event) error
