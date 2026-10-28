package agent

import (
	"context"
	"errors"
	"time"
)

// DefaultMaxIterations is the loop's safety cap when RunInput.MaxIterations
// is <= 0.
const DefaultMaxIterations = 50

// ErrMaxIterations is returned when the loop exceeds its iteration cap.
var ErrMaxIterations = errors.New("agent: max iterations exceeded")

// Agent runs a tool-using conversation loop against an LLM. It holds no
// per-run state and is safe for concurrent use as long as its LLM is.
type Agent struct {
	LLM LLM
}

// RunInput is everything needed for one run.
type RunInput struct {
	Model         string
	SystemPrompt  string
	Messages      []Message // prior conversation history (may be empty)
	Tools         ToolRegistry
	MaxIterations int           // <= 0 means DefaultMaxIterations
	ToolTimeout   time.Duration // <= 0 means no timeout
}

// RunResult is the outcome of a completed run.
type RunResult struct {
	FinalMessage Message
	Messages     []Message // full history including tool calls and results
	Iterations   int
}

// Run drives the conversation loop until the model stops requesting tool
// calls, the iteration cap is hit, or the context or sink aborts.
func (a Agent) Run(ctx context.Context, in RunInput, sink EventSink) (RunResult, error) {
	if a.LLM == nil {
		return RunResult{}, errors.New("agent: nil LLM")
	}
	if sink == nil {
		sink = func(Event) error { return nil }
	}
	maxIter := in.MaxIterations
	if maxIter <= 0 {
		maxIter = DefaultMaxIterations
	}

	messages := buildMessages(in)
	tools := toolSpecs(in)

	for i := 0; i < maxIter; i++ {
		if err := ctx.Err(); err != nil {
			return RunResult{}, err
		}

		resp, err := a.LLM.Chat(ctx, messages, tools, func(delta string) error {
			return sink(Event{Type: EventTextDelta, TextDelta: &TextDelta{Text: delta}})
		})
		if err != nil {
			return RunResult{}, err
		}

		assistant := Message{Role: RoleAssistant, Content: resp.Content, ToolCalls: resp.ToolCalls}
		messages = append(messages, assistant)

		if len(resp.ToolCalls) == 0 {
			return a.finish(sink, assistant, messages, i+1)
		}

		for _, tc := range resp.ToolCalls {
			if err := sink(Event{Type: EventToolCall, ToolCall: &ToolCallEvent{ID: tc.ID, Name: tc.Name, Arguments: tc.Arguments}}); err != nil {
				return RunResult{}, err
			}

			result, toolErr := a.callTool(ctx, in, tc)
			messages = append(messages, Message{
				Role:       RoleTool,
				ToolCallID: tc.ID,
				Name:       tc.Name,
				Content:    toolResultContent(result, toolErr),
			})

			ev := &ToolResultEvent{ID: tc.ID, Name: tc.Name, Result: result}
			if toolErr != nil {
				ev.Error = toolErr.Error()
			}
			if err := sink(Event{Type: EventToolResult, ToolResult: ev}); err != nil {
				return RunResult{}, err
			}
		}
	}

	return RunResult{}, ErrMaxIterations
}

func (a Agent) finish(sink EventSink, assistant Message, messages []Message, iterations int) (RunResult, error) {
	res := RunResult{
		FinalMessage: assistant,
		Messages:     append([]Message(nil), messages...),
		Iterations:   iterations,
	}
	if err := sink(Event{Type: EventRunFinished, RunFinished: &RunFinishedEvent{FinalMessage: assistant}}); err != nil {
		return RunResult{}, err
	}
	return res, nil
}

// toolSpecs returns the tool declarations passed to the model, or nil when
// no registry is configured.
func toolSpecs(in RunInput) []ToolSpec {
	if in.Tools == nil {
		return nil
	}
	return in.Tools.Specs()
}

// callTool executes a single tool call, applying the per-run tool timeout.
func (a Agent) callTool(ctx context.Context, in RunInput, tc ToolCall) (string, error) {
	if in.Tools == nil {
		return "", errors.New("agent: tool call requested but no ToolRegistry configured")
	}
	if in.ToolTimeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, in.ToolTimeout)
		defer cancel()
	}
	return in.Tools.Call(ctx, tc.Name, tc.Arguments)
}

// toolResultContent converts a tool outcome into the text fed back to the
// model: the result on success, the error text on failure.
func toolResultContent(result string, err error) string {
	if err != nil {
		return err.Error()
	}
	return result
}
