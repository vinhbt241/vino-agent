package api

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strconv"

	"vino/internal/agent"
	"vino/internal/durable"
)

const (
	defaultPageLimit = 50
	maxPageLimit     = 200
	// latestMessageRunes is the preview length cap for a conversation's
	// latest message, measured in runes so UTF-8 is never split.
	latestMessageRunes = 200
)

func formatID(id durable.ID) string {
	return strconv.FormatUint(uint64(id), 10)
}

func parseID(s string) (durable.ID, error) {
	n, err := strconv.ParseUint(s, 10, 64)
	if err != nil {
		return 0, err
	}
	return durable.ID(n), nil
}

func truncateRunes(s string, max int) (string, bool) {
	rs := []rune(s)
	if len(rs) <= max {
		return s, false
	}
	return string(rs[:max]) + "…", true
}

// ---- DTOs ----

type forkRefDTO struct {
	ConversationID string `json:"conversationId"`
	At             string `json:"at"`
}

type ownerRefDTO struct {
	ConversationID string `json:"conversationId"`
	TaskID         string `json:"taskId"`
}

type latestMessageDTO struct {
	Role      string `json:"role"`
	Content   string `json:"content"`
	Truncated bool   `json:"truncated"`
}

type entryDTO struct {
	ID      string        `json:"id"`
	Kind    string        `json:"kind"`
	Message agent.Message `json:"message"`
}

type conversationDTO struct {
	ID            string            `json:"id"`
	Parent        *forkRefDTO       `json:"parent,omitempty"`
	Owner         *ownerRefDTO      `json:"owner,omitempty"`
	LatestMessage *latestMessageDTO `json:"latestMessage,omitempty"`
	Entries       []entryDTO        `json:"entries,omitempty"`
}

type listConversationsDTO struct {
	Conversations []conversationDTO `json:"conversations"`
	NextCursor    *string           `json:"nextCursor"`
}

func toForkRefDTO(ref *durable.ForkRef) *forkRefDTO {
	if ref == nil {
		return nil
	}
	return &forkRefDTO{ConversationID: formatID(ref.ConversationID), At: formatID(ref.At)}
}

func toOwnerRefDTO(ref *durable.OwnerRef) *ownerRefDTO {
	if ref == nil {
		return nil
	}
	return &ownerRefDTO{ConversationID: formatID(ref.ConversationID), TaskID: formatID(ref.TaskID)}
}

func (s *Server) toConversationDTO(ctx context.Context, conv durable.ConversationRecord) conversationDTO {
	dto := conversationDTO{
		ID:     formatID(conv.ID),
		Parent: toForkRefDTO(conv.Parent),
		Owner:  toOwnerRefDTO(conv.Owner),
	}
	c, err := s.harness.Conversation(ctx, conv.ID)
	if err == nil {
		if e, err := c.LatestMessage(ctx); err == nil && e != nil && len(e.Model) > 0 {
			content, truncated := truncateRunes(e.Model[0].Content, latestMessageRunes)
			dto.LatestMessage = &latestMessageDTO{Role: string(e.Model[0].Role), Content: content, Truncated: truncated}
		}
	}
	return dto
}

// ---- handlers ----

func toEntries(entries []durable.EntryRecord) []entryDTO {
	out := make([]entryDTO, 0, len(entries))
	for _, e := range entries {
		dto := entryDTO{ID: formatID(e.ID), Kind: e.Kind}
		if len(e.Model) > 0 {
			dto.Message = e.Model[0]
		}
		out = append(out, dto)
	}
	return out
}

func (s *Server) handleGetConversation(w http.ResponseWriter, r *http.Request) {
	id, err := parseID(r.PathValue("id"))
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid conversation id")
		return
	}
	if id == durable.RootConversationID {
		writeError(w, http.StatusNotFound, "conversation not found")
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

	rec, err := c.Record(r.Context())
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	entries, err := c.Entries(r.Context())
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	writeJSON(w, http.StatusOK, conversationDTO{
		ID:      formatID(rec.ID),
		Parent:  toForkRefDTO(rec.Parent),
		Owner:   toOwnerRefDTO(rec.Owner),
		Entries: toEntries(entries),
	})
}

func (s *Server) handleListConversations(w http.ResponseWriter, r *http.Request) {
	limit, err := parseLimit(r.URL.Query().Get("limit"))
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	var cursor *durable.Cursor
	if v := r.URL.Query().Get("cursor"); v != "" {
		id, err := parseID(v)
		if err != nil {
			writeError(w, http.StatusBadRequest, "cursor: invalid conversation id")
			return
		}
		cursor = &durable.Cursor{LastID: id}
	}

	order := durable.OrderDescending
	switch r.URL.Query().Get("order") {
	case "", "desc":
	case "asc":
		order = durable.OrderAscending
	default:
		writeError(w, http.StatusBadRequest, "order: must be asc or desc")
		return
	}

	page, err := s.harness.ScanConversations(r.Context(), durable.ConversationQuery{Order: order, ExcludeRoot: true}, limit, cursor)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	items := make([]conversationDTO, 0, len(page.Items))
	for _, conv := range page.Items {
		items = append(items, s.toConversationDTO(r.Context(), conv))
	}

	var next *string
	if page.Next != nil {
		n := formatID(page.Next.LastID)
		next = &n
	}
	writeJSON(w, http.StatusOK, listConversationsDTO{Conversations: items, NextCursor: next})
}

func parseLimit(v string) (int, error) {
	if v == "" {
		return defaultPageLimit, nil
	}
	n, err := strconv.Atoi(v)
	if err != nil || n < 1 {
		return 0, fmt.Errorf("limit: must be a positive integer")
	}
	if n > maxPageLimit {
		n = maxPageLimit
	}
	return n, nil
}
