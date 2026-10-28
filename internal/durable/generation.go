package durable

import (
	"context"
	"encoding/json"
	"time"

	"vino/internal/agent"
)

// Entry kinds written by the built-in tasks.
const (
	kindUser       = "user"
	kindAssistant  = "assistant"
	kindTool       = "tool"
	kindGeneration = "vino.generation"
	kindToolTask   = "vino.tool"
)

// generationInput is the generation task's durable input.
type generationInput struct {
	SubmissionID  ID     `json:"submissionId"`
	SystemPrompt  string `json:"systemPrompt,omitempty"`
	MaxIterations int    `json:"maxIterations,omitempty"`
	ToolTimeoutMs int64  `json:"toolTimeoutMs,omitempty"`
}

// generationCheckpoint tracks which phase the generation task is in and how
// many model requests it has made.
type generationCheckpoint struct {
	Phase string `json:"phase"`
	Turns int    `json:"turns"`
}

// generationTask is the built-in task that drives the model loop: send the
// context to the model, then run any requested tool calls as child tool tasks
// and wait for them before the next request.
type generationTask struct{}

func (generationTask) Kind() string { return kindGeneration }
func (generationTask) Version() int { return 1 }
func (generationTask) Initial(input json.RawMessage) json.RawMessage {
	return mustJSON(generationCheckpoint{Phase: "model"})
}

func (g generationTask) Step(ctx context.Context, checkpoint json.RawMessage, rt *TaskRuntime) (NextState, error) {
	var cp generationCheckpoint
	if len(checkpoint) > 0 {
		if err := json.Unmarshal(checkpoint, &cp); err != nil {
			return NextState{}, err
		}
	}
	switch cp.Phase {
	case "tools":
		// Children are terminal (the scheduler only resumes us then). Loop
		// back to the model with the tool results now in the transcript.
		return NextState{Status: TaskRunning, Checkpoint: mustJSON(generationCheckpoint{Phase: "model", Turns: cp.Turns})}, nil
	default:
		return g.stepModel(ctx, rt, cp.Turns)
	}
}

func (g generationTask) stepModel(ctx context.Context, rt *TaskRuntime, turns int) (NextState, error) {
	h := rt.Harness()

	var in generationInput
	if len(rt.Input()) > 0 {
		if err := json.Unmarshal(rt.Input(), &in); err != nil {
			return NextState{}, err
		}
	}

	maxIter := in.MaxIterations
	if maxIter <= 0 {
		maxIter = agent.DefaultMaxIterations
	}
	if turns >= maxIter {
		return NextState{}, agent.ErrMaxIterations
	}

	messages, err := h.buildMessages(ctx, rt.ConversationID(), in.SystemPrompt)
	if err != nil {
		return NextState{}, err
	}

	var specs []agent.ToolSpec
	if h.tools != nil {
		specs = h.tools.Specs()
	}

	onDelta := func(delta string) error {
		return h.emit(in.SubmissionID, agent.Event{Type: agent.EventTextDelta, TextDelta: &agent.TextDelta{Text: delta}})
	}
	resp, err := h.llm.Chat(ctx, messages, specs, onDelta)
	if err != nil {
		return NextState{}, err
	}

	assistantID, err := rt.MintID(ctx)
	if err != nil {
		return NextState{}, err
	}

	if len(resp.ToolCalls) == 0 {
		final := agent.Message{Role: agent.RoleAssistant, Content: resp.Content}
		rt.AppendEntry(&EntryRecord{
			ID:             assistantID,
			ConversationID: rt.ConversationID(),
			Kind:           kindAssistant,
			Model:          []agent.Message{final},
		})
		settleSubmission(ctx, rt, in.SubmissionID, assistantID)
		if err := h.emit(in.SubmissionID, agent.Event{Type: agent.EventRunFinished, RunFinished: &agent.RunFinishedEvent{FinalMessage: final}}); err != nil {
			return NextState{}, err
		}
		h.unregisterSink(in.SubmissionID)
		return NextState{
			Status:  TaskTerminal,
			Outcome: &Outcome{Status: OutcomeCompleted, Result: mustJSON(resp.Content)},
		}, nil
	}

	// Commit the assistant turn (with tool calls) and one child tool task per
	// call, then wait for them.
	assistant := agent.Message{Role: agent.RoleAssistant, Content: resp.Content, ToolCalls: resp.ToolCalls}
	rt.AppendEntry(&EntryRecord{
		ID:             assistantID,
		ConversationID: rt.ConversationID(),
		Kind:           kindAssistant,
		Model:          []agent.Message{assistant},
	})

	children := make([]ID, 0, len(resp.ToolCalls))
	for _, tc := range resp.ToolCalls {
		if err := h.emit(in.SubmissionID, agent.Event{Type: agent.EventToolCall, ToolCall: &agent.ToolCallEvent{ID: tc.ID, Name: tc.Name, Arguments: tc.Arguments}}); err != nil {
			return NextState{}, err
		}
		taskID, err := rt.MintID(ctx)
		if err != nil {
			return NextState{}, err
		}
		raw := mustJSON(toolInput{
			CallID:       tc.ID,
			Name:         tc.Name,
			Arguments:    tc.Arguments,
			ReplaySafe:   replayOf(h.tools, tc.Name) == agent.ReplaySafe,
			SubmissionID: in.SubmissionID,
			TimeoutMs:    in.ToolTimeoutMs,
		})
		owner := rt.TaskID()
		rt.AppendTask(&TaskRecord{
			ID:             taskID,
			ConversationID: rt.ConversationID(),
			Kind:           kindToolTask,
			Version:        toolTaskDef{}.Version(),
			Input:          raw,
			OwnerID:        &owner,
			State:          TaskState{Status: TaskPending, Checkpoint: toolTaskDef{}.Initial(raw)},
		})
		children = append(children, taskID)
	}
	return NextState{
		Status:     TaskWaiting,
		Checkpoint: mustJSON(generationCheckpoint{Phase: "tools", Turns: turns + 1}),
		On:         children,
	}, nil
}

// settleSubmission marks the answered submission done, pointing at the
// assistant entry that answered it.
func settleSubmission(ctx context.Context, rt *TaskRuntime, submissionID ID, answerID ID) {
	if submissionID == 0 {
		return
	}
	sub, err := rt.Submission(ctx, submissionID)
	if err != nil || sub == nil {
		return
	}
	updated := *sub
	updated.Status = SubmissionDone
	updated.AnswerID = &answerID
	rt.AppendSubmission(&updated)
}

// toolInput is the tool task's durable input.
type toolInput struct {
	CallID       string          `json:"callId"`
	Name         string          `json:"name"`
	Arguments    json.RawMessage `json:"arguments"`
	ReplaySafe   bool            `json:"replaySafe"`
	SubmissionID ID              `json:"submissionId,omitempty"`
	TimeoutMs    int64           `json:"timeoutMs,omitempty"`
}

// toolTaskDef is the built-in task that executes one tool call and records its
// result as a transcript entry.
type toolTaskDef struct{}

func (toolTaskDef) Kind() string { return kindToolTask }
func (toolTaskDef) Version() int { return 1 }
func (toolTaskDef) Initial(input json.RawMessage) json.RawMessage {
	return mustJSON(map[string]string{"phase": "run"})
}

func (toolTaskDef) Step(ctx context.Context, checkpoint json.RawMessage, rt *TaskRuntime) (NextState, error) {
	var in toolInput
	if err := json.Unmarshal(rt.Input(), &in); err != nil {
		return NextState{}, err
	}

	if in.TimeoutMs > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, time.Duration(in.TimeoutMs)*time.Millisecond)
		defer cancel()
	}

	entryID, err := rt.MintID(ctx)
	if err != nil {
		return NextState{}, err
	}

	var content, evResult, evErr string
	if rt.Restarts() > 0 && !in.ReplaySafe {
		content = "tool call was interrupted by a crash and is not safe to re-run"
		evResult = content
	} else {
		result, callErr := rt.Harness().tools.Call(ctx, in.Name, in.Arguments)
		if callErr != nil {
			content = callErr.Error()
			evErr = callErr.Error()
		} else {
			content = result
			evResult = result
		}
	}

	rt.AppendEntry(&EntryRecord{
		ID:             entryID,
		ConversationID: rt.ConversationID(),
		Kind:           kindTool,
		Model: []agent.Message{{
			Role:       agent.RoleTool,
			ToolCallID: in.CallID,
			Name:       in.Name,
			Content:    content,
		}},
	})
	if err := rt.Harness().emit(in.SubmissionID, agent.Event{Type: agent.EventToolResult, ToolResult: &agent.ToolResultEvent{ID: in.CallID, Name: in.Name, Result: evResult, Error: evErr}}); err != nil {
		return NextState{}, err
	}
	return NextState{
		Status:  TaskTerminal,
		Outcome: &Outcome{Status: OutcomeCompleted, Result: mustJSON(content)},
	}, nil
}

func replayOf(tools agent.ToolRegistry, name string) agent.ReplayMode {
	if tools == nil {
		return agent.ReplayUnsafe
	}
	for _, s := range tools.Specs() {
		if s.Name == name {
			return s.Replay
		}
	}
	return agent.ReplayUnsafe
}

func builtinDefinitions() []TaskDefinition {
	return []TaskDefinition{generationTask{}, toolTaskDef{}}
}
