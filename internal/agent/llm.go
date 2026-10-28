package agent

import "context"

// Response is the model's complete response to one Chat call. Text deltas are
// streamed via onDelta; ToolCalls arrive complete (never partially streamed).
type Response struct {
	Content   string
	ToolCalls []ToolCall
}

// LLM is a streaming chat model. tools are the tool declarations the model
// may call; it is nil or empty when no tools are available.
type LLM interface {
	Chat(ctx context.Context, messages []Message, tools []ToolSpec, onDelta func(string) error) (Response, error)
}
