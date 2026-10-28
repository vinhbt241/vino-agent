package llm

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"strings"

	"vino/internal/agent"
)

// chatRequest is the OpenAI chat-completions request body.
type chatRequest struct {
	Model    string          `json:"model"`
	Messages []openAIMessage `json:"messages"`
	Tools    []openAITool    `json:"tools,omitempty"`
	Stream   bool            `json:"stream"`
}

type openAIMessage struct {
	Role       string           `json:"role"`
	Content    string           `json:"content,omitempty"`
	ToolCalls  []openAIToolCall `json:"tool_calls,omitempty"`
	ToolCallID string           `json:"tool_call_id,omitempty"`
}

// openAITool is one entry in the request's tools array.
type openAITool struct {
	Type     string         `json:"type"`
	Function openAIToolFunc `json:"function"`
}

type openAIToolFunc struct {
	Name        string          `json:"name"`
	Description string          `json:"description,omitempty"`
	Parameters  json.RawMessage `json:"parameters,omitempty"`
}

// openAIToolCall is a tool call inside an assistant message.
type openAIToolCall struct {
	ID       string             `json:"id"`
	Type     string             `json:"type"`
	Function openAIToolCallFunc `json:"function"`
}

// openAIToolCallFunc carries the call's name and JSON-encoded arguments.
type openAIToolCallFunc struct {
	Name      string `json:"name"`
	Arguments string `json:"arguments,omitempty"`
}

// toOpenAIMessages maps the agent's message list to the OpenAI wire shape.
func toOpenAIMessages(messages []agent.Message) []openAIMessage {
	out := make([]openAIMessage, 0, len(messages))
	for _, m := range messages {
		om := openAIMessage{Role: string(m.Role), Content: m.Content}
		switch m.Role {
		case agent.RoleAssistant:
			if len(m.ToolCalls) > 0 {
				om.ToolCalls = make([]openAIToolCall, 0, len(m.ToolCalls))
				for _, tc := range m.ToolCalls {
					om.ToolCalls = append(om.ToolCalls, openAIToolCall{
						ID:   tc.ID,
						Type: "function",
						Function: openAIToolCallFunc{
							Name:      tc.Name,
							Arguments: string(tc.Arguments),
						},
					})
				}
			}
		case agent.RoleTool:
			om.ToolCallID = m.ToolCallID
		}
		out = append(out, om)
	}
	return out
}

// toOpenAITools maps tool specs to the OpenAI tools array.
func toOpenAITools(specs []agent.ToolSpec) []openAITool {
	if len(specs) == 0 {
		return nil
	}
	out := make([]openAITool, 0, len(specs))
	for _, s := range specs {
		out = append(out, openAITool{
			Type: "function",
			Function: openAIToolFunc{
				Name:        s.Name,
				Description: s.Description,
				Parameters:  s.Parameters,
			},
		})
	}
	return out
}

// chatChunk is one SSE chunk of the streamed response.
type chatChunk struct {
	Choices []chatChoice `json:"choices"`
}

type chatChoice struct {
	Delta chatDelta `json:"delta"`
}

type chatDelta struct {
	Content   string          `json:"content"`
	ToolCalls []toolCallDelta `json:"tool_calls"`
}

type toolCallDelta struct {
	Index    int              `json:"index"`
	ID       string           `json:"id"`
	Function toolCallFunction `json:"function"`
}

type toolCallFunction struct {
	Name      string `json:"name"`
	Arguments string `json:"arguments"`
}

// toolCallAccum gathers the streamed fragments of a single tool call.
type toolCallAccum struct {
	id        string
	name      string
	arguments strings.Builder
}

// parseStream reads an SSE stream, streams text deltas through onDelta, and
// returns the complete response with fully accumulated tool calls.
func parseStream(r io.Reader, onDelta func(string) error) (agent.Response, error) {
	var res agent.Response
	var accums []toolCallAccum

	scanner := bufio.NewScanner(r)
	scanner.Buffer(make([]byte, 0, 64*1024), 4*1024*1024)

	for scanner.Scan() {
		line := scanner.Text()
		if !strings.HasPrefix(line, "data:") {
			continue
		}
		data := strings.TrimSpace(strings.TrimPrefix(line, "data:"))
		if data == "[DONE]" {
			break
		}
		var chunk chatChunk
		if err := json.Unmarshal([]byte(data), &chunk); err != nil {
			return agent.Response{}, fmt.Errorf("llm: decoding stream chunk: %w", err)
		}
		if len(chunk.Choices) == 0 {
			continue
		}
		delta := chunk.Choices[0].Delta
		if delta.Content != "" {
			res.Content += delta.Content
			if err := onDelta(delta.Content); err != nil {
				return agent.Response{}, err
			}
		}
		for _, tc := range delta.ToolCalls {
			for len(accums) <= tc.Index {
				accums = append(accums, toolCallAccum{})
			}
			acc := &accums[tc.Index]
			if tc.ID != "" {
				acc.id = tc.ID
			}
			if tc.Function.Name != "" {
				acc.name = tc.Function.Name
			}
			if tc.Function.Arguments != "" {
				acc.arguments.WriteString(tc.Function.Arguments)
			}
		}
	}
	if err := scanner.Err(); err != nil {
		return agent.Response{}, err
	}

	for i := range accums {
		acc := &accums[i]
		tc := agent.ToolCall{ID: acc.id, Name: acc.name}
		if acc.arguments.Len() > 0 {
			tc.Arguments = json.RawMessage(acc.arguments.String())
		}
		res.ToolCalls = append(res.ToolCalls, tc)
	}
	return res, nil
}
