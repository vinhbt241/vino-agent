package api

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"sync"

	"vino/internal/agent"
	"vino/internal/durable"
)

// ---- multiplayer DTOs ----

type agentConfigDTO struct {
	Model        string   `json:"model,omitempty"`
	SystemPrompt string   `json:"systemPrompt,omitempty"`
	Tools        []string `json:"tools,omitempty"`
	Extensions   []string `json:"extensions,omitempty"`
}

type conversationStateDTO struct {
	ID     string         `json:"id"`
	Parent *forkRefDTO    `json:"parent,omitempty"`
	Owner  *ownerRefDTO   `json:"owner,omitempty"`
	Agent  agentConfigDTO `json:"agent"`
}

type documentDTO struct {
	ID    string          `json:"id"`
	Name  string          `json:"name"`
	Value json.RawMessage `json:"value,omitempty"`
}

type submissionDTO struct {
	ID        string `json:"id"`
	RequestID string `json:"requestId,omitempty"`
	Type      string `json:"type"`
	Status    string `json:"status"`
	EntryID   string `json:"entryId,omitempty"`
	AnswerID  string `json:"answerId,omitempty"`
	Reason    string `json:"reason,omitempty"`
}

type viewDTO struct {
	Seq          string               `json:"seq"`
	Conversation conversationStateDTO `json:"conversation"`
	Entries      []entryDTO           `json:"entries"`
	Documents    []documentDTO        `json:"documents"`
	Submissions  []submissionDTO      `json:"submissions"`
}

type writeDTO struct {
	Type   string `json:"type"`
	Record any    `json:"record"`
}

type commitOpDTO struct {
	Seq    string     `json:"seq"`
	Writes []writeDTO `json:"writes"`
}

// ---- record -> DTO mapping ----

func formatSeq(seq durable.Seq) string {
	return strconv.FormatUint(uint64(seq), 10)
}

func toEntryDTO(e durable.EntryRecord) entryDTO {
	dto := entryDTO{ID: formatID(e.ID), Kind: e.Kind}
	if len(e.Model) > 0 {
		dto.Message = e.Model[0]
	}
	return dto
}

func toAgentConfigDTO(cfg durable.AgentConfig) agentConfigDTO {
	return agentConfigDTO{
		Model:        cfg.Model,
		SystemPrompt: cfg.SystemPrompt,
		Tools:        cfg.Tools,
		Extensions:   cfg.Extensions,
	}
}

func toConversationStateDTO(rec durable.ConversationRecord) conversationStateDTO {
	return conversationStateDTO{
		ID:     formatID(rec.ID),
		Parent: toForkRefDTO(rec.Parent),
		Owner:  toOwnerRefDTO(rec.Owner),
		Agent:  toAgentConfigDTO(rec.Agent),
	}
}

func toDocumentDTO(rec durable.DocumentRecord) documentDTO {
	return documentDTO{ID: formatID(rec.ID), Name: rec.Name, Value: rec.Value}
}

func toSubmissionDTO(rec durable.SubmissionRecord) submissionDTO {
	dto := submissionDTO{
		ID:        formatID(rec.ID),
		RequestID: rec.RequestID,
		Type:      string(rec.Type),
		Status:    string(rec.Status),
		Reason:    rec.Reason,
	}
	if rec.EntryID != nil {
		dto.EntryID = formatID(*rec.EntryID)
	}
	if rec.AnswerID != nil {
		dto.AnswerID = formatID(*rec.AnswerID)
	}
	return dto
}

func toViewDTO(v durable.View) viewDTO {
	entries := make([]entryDTO, 0, len(v.Entries))
	for _, e := range v.Entries {
		entries = append(entries, toEntryDTO(e))
	}
	docs := make([]documentDTO, 0, len(v.Documents))
	for _, d := range v.Documents {
		docs = append(docs, toDocumentDTO(d))
	}
	subs := make([]submissionDTO, 0, len(v.Submissions))
	for _, s := range v.Submissions {
		subs = append(subs, toSubmissionDTO(s))
	}
	return viewDTO{
		Seq:          formatSeq(v.Seq),
		Conversation: toConversationStateDTO(v.Conversation),
		Entries:      entries,
		Documents:    docs,
		Submissions:  subs,
	}
}

func toWriteDTO(w durable.StorageWrite) (writeDTO, error) {
	switch w.Type {
	case durable.WriteConversation:
		rec, ok := w.Value.(*durable.ConversationRecord)
		if !ok {
			return writeDTO{}, fmt.Errorf("api: write conversation: unexpected value type %T", w.Value)
		}
		return writeDTO{Type: string(w.Type), Record: toConversationStateDTO(*rec)}, nil
	case durable.WriteEntry:
		rec, ok := w.Value.(*durable.EntryRecord)
		if !ok {
			return writeDTO{}, fmt.Errorf("api: write entry: unexpected value type %T", w.Value)
		}
		return writeDTO{Type: string(w.Type), Record: toEntryDTO(*rec)}, nil
	case durable.WriteDocument:
		rec, ok := w.Value.(*durable.DocumentRecord)
		if !ok {
			return writeDTO{}, fmt.Errorf("api: write document: unexpected value type %T", w.Value)
		}
		return writeDTO{Type: string(w.Type), Record: toDocumentDTO(*rec)}, nil
	case durable.WriteSubmission:
		rec, ok := w.Value.(*durable.SubmissionRecord)
		if !ok {
			return writeDTO{}, fmt.Errorf("api: write submission: unexpected value type %T", w.Value)
		}
		return writeDTO{Type: string(w.Type), Record: toSubmissionDTO(*rec)}, nil
	default:
		return writeDTO{}, fmt.Errorf("api: watch: unsupported write type %q", w.Type)
	}
}

func toCommitOpDTO(op durable.CommitOp) (commitOpDTO, error) {
	writes := make([]writeDTO, 0, len(op.Writes))
	for _, w := range op.Writes {
		dto, err := toWriteDTO(w)
		if err != nil {
			return commitOpDTO{}, err
		}
		writes = append(writes, dto)
	}
	return commitOpDTO{Seq: formatSeq(op.Seq), Writes: writes}, nil
}

// sseQueueCap bounds a watch/events stream's pending-frame queue. It must
// exceed durable's commit catch-up window (1024) so catch-up is never dropped.
const sseQueueCap = 2048

var errSSEQueueFull = errors.New("sse: client too slow")

// sseWriter serializes every SSE frame write onto one goroutine. Producers
// (the durable watch drain and event emit paths) only enqueue; the writer
// goroutine is the sole writer to the ResponseWriter, so concurrent frames
// from several run goroutines never race on the response.
type sseWriter struct {
	w     http.ResponseWriter
	flush http.Flusher

	mu   sync.Mutex
	q    [][]byte
	sig  chan struct{}
	done chan struct{}
	once sync.Once
	wg   sync.WaitGroup
}

// newSSEWriter sets the shared SSE headers and returns a writer, or nil when
// the ResponseWriter does not support flushing.
func newSSEWriter(w http.ResponseWriter) *sseWriter {
	flush, ok := w.(http.Flusher)
	if !ok {
		return nil
	}
	sseHeaders(w)
	return &sseWriter{w: w, flush: flush, sig: make(chan struct{}, 1), done: make(chan struct{})}
}

// start launches the writer goroutine and flushes the 200 so the client has
// an open stream even before the first frame.
func (s *sseWriter) start() {
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		defer s.once.Do(func() { close(s.done) })
		s.flush.Flush()
		for {
			select {
			case <-s.done:
				// Flush anything enqueued before stop() was called, so the
				// final frame is never dropped when done and a signal are
				// both ready.
				s.drain()
				return
			case <-s.sig:
				s.drain()
			}
		}
	}()
}

// stop closes the writer and waits for it to finish, so the handler returns
// only after the response writer is no longer being written from this stream.
func (s *sseWriter) stop() {
	s.once.Do(func() { close(s.done) })
	s.wg.Wait()
}

// drain writes every queued frame and flushes. It is only called by the
// writer goroutine.
func (s *sseWriter) drain() {
	s.mu.Lock()
	frames := s.q
	s.q = nil
	s.mu.Unlock()
	for _, b := range frames {
		if _, err := s.w.Write(b); err != nil {
			return
		}
		if s.flush != nil {
			s.flush.Flush()
		}
	}
}

// send marshals and enqueues one frame. It is non-blocking: a full queue
// reports errSSEQueueFull so the durable layer detaches a too-slow client
// instead of stalling commits or the run.
func (s *sseWriter) send(name string, data any) error {
	b, err := json.Marshal(data)
	if err != nil {
		return err
	}
	frame := fmt.Sprintf("event: %s\ndata: %s\n\n", name, b)
	s.mu.Lock()
	if len(s.q) >= sseQueueCap {
		s.mu.Unlock()
		return errSSEQueueFull
	}
	s.q = append(s.q, []byte(frame))
	s.mu.Unlock()
	select {
	case s.sig <- struct{}{}:
	default:
	}
	return nil
}

// ---- handlers ----

// conversationHandle parses and resolves the {id} path value, rejecting the
// internal root conversation. It writes the error response and returns false
// when the request cannot proceed.
func (s *Server) conversationHandle(w http.ResponseWriter, r *http.Request) (*durable.Conversation, bool) {
	id, err := parseID(r.PathValue("id"))
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid conversation id")
		return nil, false
	}
	if id == durable.RootConversationID {
		writeError(w, http.StatusNotFound, "conversation not found")
		return nil, false
	}
	c, err := s.harness.Conversation(r.Context(), id)
	if err != nil {
		if errors.Is(err, durable.ErrConversationNotFound) {
			writeError(w, http.StatusNotFound, "conversation not found")
			return nil, false
		}
		writeError(w, http.StatusInternalServerError, err.Error())
		return nil, false
	}
	return c, true
}

// handleViewConversation returns the conversation's committed snapshot.
func (s *Server) handleViewConversation(w http.ResponseWriter, r *http.Request) {
	c, ok := s.conversationHandle(w, r)
	if !ok {
		return
	}
	v, err := c.View(r.Context())
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, toViewDTO(v))
}

// handleWatchConversation streams committed changes for the conversation,
// catching up from ?after= then following live commits.
func (s *Server) handleWatchConversation(w http.ResponseWriter, r *http.Request) {
	c, ok := s.conversationHandle(w, r)
	if !ok {
		return
	}
	after, err := parseAfter(r.URL.Query().Get("after"))
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	sw := newSSEWriter(w)
	if sw == nil {
		writeError(w, http.StatusInternalServerError, "streaming unsupported")
		return
	}

	// Catch-up is delivered synchronously inside Watch, before start() flushes
	// the 200, so an ErrWatchSeqExpired can still answer as a clean JSON 409.
	cancel, err := c.Watch(r.Context(), after, func(op durable.CommitOp) error {
		dto, err := toCommitOpDTO(op)
		if err != nil {
			return err
		}
		return sw.send("commit", dto)
	})
	if err != nil {
		if errors.Is(err, durable.ErrWatchSeqExpired) {
			writeError(w, http.StatusConflict, "watch seq expired; re-read /view")
			return
		}
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	sw.start()
	defer sw.stop()
	<-r.Context().Done()
	cancel()
}

// handleWatchConversationEvents streams live run events for the conversation.
func (s *Server) handleWatchConversationEvents(w http.ResponseWriter, r *http.Request) {
	c, ok := s.conversationHandle(w, r)
	if !ok {
		return
	}

	sw := newSSEWriter(w)
	if sw == nil {
		writeError(w, http.StatusInternalServerError, "streaming unsupported")
		return
	}

	conversationID := formatID(c.ID())
	cancel, err := c.WatchEvents(r.Context(), func(ev agent.Event) error {
		name, data, err := sseEvent(ev, conversationID)
		if err != nil {
			return err
		}
		return sw.send(name, data)
	})
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	sw.start()
	defer sw.stop()
	<-r.Context().Done()
	cancel()
}

// parseAfter parses the required ?after= commit sequence.
func parseAfter(s string) (durable.Seq, error) {
	if s == "" {
		return 0, fmt.Errorf("after is required; read /view first")
	}
	n, err := strconv.ParseUint(s, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("after: must be a non-negative integer")
	}
	return durable.Seq(n), nil
}
