package api

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"time"

	"vino/internal/agent"
	"vino/internal/durable"
)

// runRequest is the POST /runs body. Provider and model are fixed at server
// startup (Config.Default), so they are absent here.
type runRequest struct {
	SystemPrompt  string       `json:"systemPrompt"`
	Messages      []runMessage `json:"messages"`
	MaxIterations int          `json:"maxIterations"`
	ToolTimeout   string       `json:"toolTimeout"`
	RequestID     string       `json:"requestId"`
}

// runMessage is one incoming conversation turn. Only user/assistant text
// turns are accepted; tool turns are generated server-side.
type runMessage struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}

func (s *Server) handleRun(w http.ResponseWriter, r *http.Request) {
	var req runRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON body")
		return
	}

	timeout, err := parseTimeout(req.ToolTimeout)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	messages, err := toMessages(req.Messages)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	s.streamSubmission(w, r, func(sink agent.EventSink) (*durable.Submission, error) {
		return s.harness.Start(r.Context(), durable.StartInput{
			RequestID:     req.RequestID,
			SystemPrompt:  req.SystemPrompt,
			Messages:      messages,
			MaxIterations: req.MaxIterations,
			ToolTimeout:   timeout,
		}, sink)
	})
}

// handleResumeConversation appends turns to an existing conversation and
// streams the run exactly like handleRun.
func (s *Server) handleResumeConversation(w http.ResponseWriter, r *http.Request) {
	id, err := parseID(r.PathValue("id"))
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid conversation id")
		return
	}
	if id == durable.RootConversationID {
		writeError(w, http.StatusNotFound, "conversation not found")
		return
	}

	var req runRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON body")
		return
	}
	timeout, err := parseTimeout(req.ToolTimeout)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	messages, err := toMessages(req.Messages)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	c, err := s.harness.Conversation(r.Context(), id)
	if err != nil {
		if errors.Is(err, durable.ErrConversationNotFound) {
			writeError(w, http.StatusNotFound, "conversation not found")
			return
		}
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	s.streamSubmission(w, r, func(sink agent.EventSink) (*durable.Submission, error) {
		return c.SubmitStream(r.Context(), durable.SubmitInput{
			RequestID:     req.RequestID,
			SystemPrompt:  req.SystemPrompt,
			Messages:      messages,
			MaxIterations: req.MaxIterations,
			ToolTimeout:   timeout,
		}, sink)
	})
}

// streamSubmission opens the SSE stream, creates a submission with sink, emits
// the leading run_started, and waits for the run to settle, mapping the
// terminal state to SSE events. It is shared by handleRun and
// handleResumeConversation.
func (s *Server) streamSubmission(w http.ResponseWriter, r *http.Request, create func(sink agent.EventSink) (*durable.Submission, error)) {
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.WriteHeader(http.StatusOK)

	flusher, ok := w.(http.Flusher)
	if !ok {
		return
	}

	finished := false
	var conversationID, submissionID string
	sink := func(ev agent.Event) error {
		if ev.Type == agent.EventRunFinished {
			finished = true
		}
		name, data, err := sseEvent(ev, conversationID)
		if err != nil {
			return err
		}
		return writeSSE(w, flusher, name, data)
	}

	sub, err := create(sink)
	if err != nil {
		_ = writeSSE(w, flusher, "error", map[string]string{"error": err.Error()})
		return
	}

	conversationID = formatID(sub.ConversationID())
	submissionID = formatID(sub.ID())
	if err := writeSSE(w, flusher, "run_started", runStartedDTO{
		ConversationID: conversationID,
		SubmissionID:   submissionID,
		Resumed:        sub.Resumed(),
	}); err != nil {
		return
	}

	rec, err := sub.Wait(r.Context())
	if err != nil {
		_ = writeSSE(w, flusher, "error", map[string]string{"error": err.Error()})
		return
	}

	switch rec.Status {
	case durable.SubmissionDone:
		if !finished {
			// A retry that joined a run which already answered: replay the
			// terminal event from the stored answer.
			msg := s.answerMessage(r.Context(), rec)
			_ = writeSSE(w, flusher, "run_finished", runFinishedDTO{FinalMessage: msg, ConversationID: conversationID})
		}
	case durable.SubmissionUnanswered:
		_ = writeSSE(w, flusher, "error", map[string]string{"error": rec.Reason})
	default:
		_ = writeSSE(w, flusher, "error", map[string]string{"error": "run did not settle"})
	}
}

func (s *Server) answerMessage(ctx context.Context, rec *durable.SubmissionRecord) agent.Message {
	if rec.AnswerID == nil {
		return agent.Message{}
	}
	entry, err := s.harness.Entry(ctx, *rec.AnswerID)
	if err != nil || entry == nil || len(entry.Model) == 0 {
		return agent.Message{}
	}
	return entry.Model[0]
}

// writeError sends a non-streaming JSON error response for requests that fail
// validation before the SSE stream opens.
func writeError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]string{"error": msg})
}

// parseTimeout converts the request's toolTimeout string into a duration.
// Empty means no timeout; negative durations are rejected.
func parseTimeout(s string) (time.Duration, error) {
	if s == "" {
		return 0, nil
	}
	d, err := time.ParseDuration(s)
	if err != nil {
		return 0, fmt.Errorf("toolTimeout: %v", err)
	}
	if d < 0 {
		return 0, fmt.Errorf("toolTimeout: must not be negative")
	}
	return d, nil
}

// toMessages validates the incoming turns and maps them to agent messages.
func toMessages(msgs []runMessage) ([]agent.Message, error) {
	if len(msgs) == 0 {
		return nil, fmt.Errorf("messages: at least one message is required")
	}
	out := make([]agent.Message, 0, len(msgs))
	for _, m := range msgs {
		var role agent.Role
		switch m.Role {
		case "user":
			role = agent.RoleUser
		case "assistant":
			role = agent.RoleAssistant
		default:
			return nil, fmt.Errorf("messages: role %q must be user or assistant", m.Role)
		}
		if m.Content == "" {
			return nil, fmt.Errorf("messages: content is required")
		}
		out = append(out, agent.Message{Role: role, Content: m.Content})
	}
	return out, nil
}

// sseEvent maps an agent.Event to its SSE event name and data payload.
func sseEvent(ev agent.Event, conversationID string) (string, any, error) {
	switch ev.Type {
	case agent.EventTextDelta:
		return "text_delta", textDeltaDTO{Text: ev.TextDelta.Text}, nil
	case agent.EventToolCall:
		return "tool_call_started", toolCallDTO{
			ID:        ev.ToolCall.ID,
			Name:      ev.ToolCall.Name,
			Arguments: ev.ToolCall.Arguments,
		}, nil
	case agent.EventToolResult:
		return "tool_result", toolResultDTO{
			ID:     ev.ToolResult.ID,
			Name:   ev.ToolResult.Name,
			Result: ev.ToolResult.Result,
			Error:  ev.ToolResult.Error,
		}, nil
	case agent.EventRunFinished:
		return "run_finished", runFinishedDTO{FinalMessage: ev.RunFinished.FinalMessage, ConversationID: conversationID}, nil
	default:
		return "", nil, fmt.Errorf("api: unknown event type %q", ev.Type)
	}
}

type textDeltaDTO struct {
	Text string `json:"text"`
}

type toolCallDTO struct {
	ID        string          `json:"id"`
	Name      string          `json:"name"`
	Arguments json.RawMessage `json:"arguments"`
}

type toolResultDTO struct {
	ID     string `json:"id"`
	Name   string `json:"name"`
	Result string `json:"result"`
	Error  string `json:"error,omitempty"`
}

type runFinishedDTO struct {
	FinalMessage   agent.Message `json:"finalMessage"`
	ConversationID string        `json:"conversationId"`
}

type runStartedDTO struct {
	ConversationID string `json:"conversationId"`
	SubmissionID   string `json:"submissionId"`
	Resumed        bool   `json:"resumed"`
}

// writeSSE writes one SSE event frame and flushes it to the client.
func writeSSE(w io.Writer, flusher http.Flusher, name string, data any) error {
	b, err := json.Marshal(data)
	if err != nil {
		return err
	}
	if _, err := fmt.Fprintf(w, "event: %s\ndata: %s\n\n", name, b); err != nil {
		return err
	}
	flusher.Flush()
	return nil
}
