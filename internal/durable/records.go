package durable

import (
	"encoding/json"

	"vino/internal/agent"
)

// ForkRef names where a conversation forked: a conversation and the entry in
// its history the fork is cut at.
type ForkRef struct {
	ConversationID ID `json:"conversationId"`
	At             ID `json:"at"`
}

// OwnerRef records task attribution for a conversation.
type OwnerRef struct {
	ConversationID ID `json:"conversationId"`
	TaskID         ID `json:"taskId"`
}

// ConversationRecord is a transcript scope. Parent (fork) and owner (task
// attribution) are separate edges.
type ConversationRecord struct {
	ID     ID        `json:"id"`
	Parent *ForkRef  `json:"parent,omitempty"`
	Owner  *OwnerRef `json:"owner,omitempty"`
}

// EntryRecord is an immutable transcript record. Model holds the wire
// messages the entry contributes to context; Data holds kind-specific JSON.
type EntryRecord struct {
	ID             ID              `json:"id"`
	ConversationID ID              `json:"conversationId"`
	Kind           string          `json:"kind"`
	Model          []agent.Message `json:"model,omitempty"`
	Data           json.RawMessage `json:"data,omitempty"`
	Head           *ID             `json:"head,omitempty"`
	ByTaskID       *ID             `json:"byTaskId,omitempty"`
}

// TaskStatus is a task's place in its lifecycle.
type TaskStatus string

const (
	TaskPending    TaskStatus = "pending"
	TaskRunning    TaskStatus = "running"
	TaskWaiting    TaskStatus = "waiting"
	TaskCompleting TaskStatus = "completing"
	TaskTerminal   TaskStatus = "terminal"
)

// OutcomeStatus discriminates a terminal outcome.
type OutcomeStatus string

const (
	OutcomeCompleted OutcomeStatus = "completed"
	OutcomeFailed    OutcomeStatus = "failed"
	OutcomeAborted   OutcomeStatus = "aborted"
	OutcomeFaulted   OutcomeStatus = "faulted"
)

// OutcomeError is the error carried by a failed or faulted outcome.
type OutcomeError struct {
	Message string `json:"message"`
}

// Outcome is a task's terminal result.
type Outcome struct {
	Status OutcomeStatus   `json:"status"`
	Result json.RawMessage `json:"result,omitempty"`
	Error  *OutcomeError   `json:"error,omitempty"`
}

// TaskState is the durable, replaceable snapshot of a task. Checkpoint is an
// opaque JSON value owned by the task's definition; On lists tasks a waiting
// task resumes on.
type TaskState struct {
	Status     TaskStatus      `json:"status"`
	Checkpoint json.RawMessage `json:"checkpoint,omitempty"`
	On         []ID            `json:"on,omitempty"`
	Outcome    *Outcome        `json:"outcome,omitempty"`
}

// TaskRecord is a durable state machine attached to one conversation.
type TaskRecord struct {
	ID             ID                         `json:"id"`
	ConversationID ID                         `json:"conversationId"`
	Kind           string                     `json:"kind"`
	Version        int                        `json:"version"`
	Input          json.RawMessage            `json:"input"`
	OwnerID        *ID                        `json:"ownerId,omitempty"`
	Background     bool                       `json:"background"`
	AbortRequested bool                       `json:"abortRequested"`
	Restarts       int                        `json:"restarts"`
	State          TaskState                  `json:"state"`
	Memos          map[string]json.RawMessage `json:"memos,omitempty"`
}

// SubmissionType discriminates a submission record.
type SubmissionType string

const (
	SubmissionInput SubmissionType = "input"
)

// SubmissionStatus is a submission's place in its lifecycle.
type SubmissionStatus string

const (
	SubmissionQueued     SubmissionStatus = "queued"
	SubmissionPlaced     SubmissionStatus = "placed"
	SubmissionDone       SubmissionStatus = "done"
	SubmissionUnanswered SubmissionStatus = "unanswered"
)

// SubmissionRecord backs an awaitable host submission. RequestID makes it
// exactly-once across crash retries.
type SubmissionRecord struct {
	ID             ID               `json:"id"`
	ConversationID ID               `json:"conversationId"`
	RequestID      string           `json:"requestId,omitempty"`
	Type           SubmissionType   `json:"type"`
	Status         SubmissionStatus `json:"status"`
	EntryID        *ID              `json:"entryId,omitempty"`
	AnswerID       *ID              `json:"answerId,omitempty"`
	Reason         string           `json:"reason,omitempty"`
}
