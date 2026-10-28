package api

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"vino/internal/agent"
)

func doGet(t *testing.T, s *Server, path string) *httptest.ResponseRecorder {
	t.Helper()
	rr := httptest.NewRecorder()
	s.ServeHTTP(rr, httptest.NewRequest(http.MethodGet, path, nil))
	return rr
}

func TestListConversations(t *testing.T) {
	llm := &fakeLLM{steps: []fakeStep{
		{resp: agent.Response{Content: "first answer"}},
		{resp: agent.Response{Content: "second answer"}},
	}}
	s := newTestServer(t, llm, &fakeRegistry{})
	doRun(t, s, `{"messages":[{"role":"user","content":"first"}]}`)
	doRun(t, s, `{"messages":[{"role":"user","content":"second"}]}`)

	rr := doGet(t, s, "/conversations")
	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rr.Code)
	}
	var body struct {
		Conversations []struct {
			ID            string `json:"id"`
			LatestMessage *struct {
				Role      string `json:"role"`
				Content   string `json:"content"`
				Truncated bool   `json:"truncated"`
			} `json:"latestMessage"`
		} `json:"conversations"`
		NextCursor *string `json:"nextCursor"`
	}
	if err := json.Unmarshal(rr.Body.Bytes(), &body); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if len(body.Conversations) != 2 {
		t.Fatalf("len(conversations) = %d, want 2 (root excluded)", len(body.Conversations))
	}
	if body.NextCursor != nil {
		t.Fatalf("nextCursor = %v, want null", body.NextCursor)
	}
	for _, c := range body.Conversations {
		if c.ID == "1" {
			t.Fatal("root conversation leaked into the list")
		}
	}
	// Newest first.
	if body.Conversations[0].ID <= body.Conversations[1].ID {
		t.Fatalf("order = [%s %s], want descending", body.Conversations[0].ID, body.Conversations[1].ID)
	}
	lm := body.Conversations[0].LatestMessage
	if lm == nil || lm.Role != "assistant" || lm.Content != "second answer" || lm.Truncated {
		t.Fatalf("latestMessage = %+v, want assistant \"second answer\" untruncated", lm)
	}
}

func TestListConversations_TruncatesLatestMessage(t *testing.T) {
	long := strings.Repeat("a", 250)
	llm := &fakeLLM{steps: []fakeStep{{resp: agent.Response{Content: long}}}}
	s := newTestServer(t, llm, &fakeRegistry{})
	doRun(t, s, `{"messages":[{"role":"user","content":"hi"}]}`)

	rr := doGet(t, s, "/conversations")
	var body struct {
		Conversations []struct {
			LatestMessage struct {
				Content   string `json:"content"`
				Truncated bool   `json:"truncated"`
			} `json:"latestMessage"`
		} `json:"conversations"`
	}
	if err := json.Unmarshal(rr.Body.Bytes(), &body); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if len(body.Conversations) != 1 {
		t.Fatalf("len(conversations) = %d, want 1", len(body.Conversations))
	}
	lm := body.Conversations[0].LatestMessage
	if !lm.Truncated {
		t.Fatalf("truncated = false, want true")
	}
	if len([]rune(lm.Content)) != 201 || !strings.HasSuffix(lm.Content, "…") {
		t.Fatalf("content = %q, want 200 runes + ellipsis", lm.Content)
	}
	if !strings.HasPrefix(lm.Content, strings.Repeat("a", 200)) {
		t.Fatalf("content does not start with the first 200 runes")
	}
}

func TestListConversations_Pagination(t *testing.T) {
	llm := &fakeLLM{steps: []fakeStep{
		{resp: agent.Response{Content: "a"}},
		{resp: agent.Response{Content: "b"}},
		{resp: agent.Response{Content: "c"}},
	}}
	s := newTestServer(t, llm, &fakeRegistry{})
	doRun(t, s, `{"messages":[{"role":"user","content":"one"}]}`)
	doRun(t, s, `{"messages":[{"role":"user","content":"two"}]}`)
	doRun(t, s, `{"messages":[{"role":"user","content":"three"}]}`)

	rr1 := doGet(t, s, "/conversations?limit=2")
	var page1 struct {
		Conversations []struct {
			ID string `json:"id"`
		} `json:"conversations"`
		NextCursor *string `json:"nextCursor"`
	}
	if err := json.Unmarshal(rr1.Body.Bytes(), &page1); err != nil {
		t.Fatalf("unmarshal page1: %v", err)
	}
	if len(page1.Conversations) != 2 || page1.NextCursor == nil {
		t.Fatalf("page1 = %+v, want 2 items and a cursor", page1)
	}

	rr2 := doGet(t, s, "/conversations?limit=2&cursor="+*page1.NextCursor)
	var page2 struct {
		Conversations []struct {
			ID string `json:"id"`
		} `json:"conversations"`
		NextCursor *string `json:"nextCursor"`
	}
	if err := json.Unmarshal(rr2.Body.Bytes(), &page2); err != nil {
		t.Fatalf("unmarshal page2: %v", err)
	}
	if len(page2.Conversations) != 1 || page2.NextCursor != nil {
		t.Fatalf("page2 = %+v, want 1 item and no cursor", page2)
	}
	if page1.Conversations[1].ID != *page1.NextCursor {
		t.Fatalf("cursor = %q, want last item id %q", *page1.NextCursor, page1.Conversations[1].ID)
	}
}

func runAndGetConversationID(t *testing.T, s *Server, body string) string {
	t.Helper()
	rr := doRun(t, s, body)
	frames := parseSSE(rr.Body.String())
	if len(frames) == 0 || frames[0].name != "run_started" {
		t.Fatalf("frames = %#v, want leading run_started", frames)
	}
	var started struct {
		ConversationID string `json:"conversationId"`
	}
	if err := json.Unmarshal([]byte(frames[0].data), &started); err != nil {
		t.Fatalf("unmarshal run_started: %v", err)
	}
	return started.ConversationID
}

func TestGetConversation(t *testing.T) {
	llm := &fakeLLM{steps: []fakeStep{{resp: agent.Response{Content: "answer"}}}}
	s := newTestServer(t, llm, &fakeRegistry{})
	id := runAndGetConversationID(t, s, `{"messages":[{"role":"user","content":"hi"}]}`)

	rr := doGet(t, s, "/conversations/"+id)
	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rr.Code)
	}
	var body struct {
		ID      string `json:"id"`
		Parent  any    `json:"parent"`
		Owner   any    `json:"owner"`
		Entries []struct {
			ID      string `json:"id"`
			Kind    string `json:"kind"`
			Message struct {
				Role    string `json:"role"`
				Content string `json:"content"`
			} `json:"message"`
		} `json:"entries"`
	}
	if err := json.Unmarshal(rr.Body.Bytes(), &body); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if body.ID != id {
		t.Fatalf("id = %q, want %q", body.ID, id)
	}
	if body.Parent != nil || body.Owner != nil {
		t.Fatalf("parent/owner = %v/%v, want absent", body.Parent, body.Owner)
	}
	if len(body.Entries) != 2 {
		t.Fatalf("len(entries) = %d, want 2: %#v", len(body.Entries), body.Entries)
	}
	if body.Entries[0].Kind != "user" || body.Entries[0].Message.Content != "hi" {
		t.Fatalf("entries[0] = %#v, want user hi", body.Entries[0])
	}
	if body.Entries[1].Kind != "assistant" || body.Entries[1].Message.Content != "answer" {
		t.Fatalf("entries[1] = %#v, want assistant answer", body.Entries[1])
	}
}

func TestGetConversation_NotFound(t *testing.T) {
	s := newTestServer(t, &fakeLLM{}, &fakeRegistry{})

	for _, id := range []string{"1", "999999"} {
		rr := doGet(t, s, "/conversations/"+id)
		if rr.Code != http.StatusNotFound {
			t.Fatalf("GET /conversations/%s status = %d, want 404", id, rr.Code)
		}
	}

	rr := doGet(t, s, "/conversations/abc")
	if rr.Code != http.StatusBadRequest {
		t.Fatalf("GET /conversations/abc status = %d, want 400", rr.Code)
	}
}

func doResume(t *testing.T, s *Server, id string, body string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(http.MethodPost, "/conversations/"+id+"/runs", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rr := httptest.NewRecorder()
	s.ServeHTTP(rr, req)
	return rr
}

func TestResumeConversation(t *testing.T) {
	llm := &fakeLLM{steps: []fakeStep{
		{resp: agent.Response{Content: "first answer"}},
		{resp: agent.Response{Content: "second answer"}},
	}}
	s := newTestServer(t, llm, &fakeRegistry{})
	id := runAndGetConversationID(t, s, `{"messages":[{"role":"user","content":"hi"}]}`)

	rr := doResume(t, s, id, `{"messages":[{"role":"user","content":"continue"}]}`)
	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rr.Code)
	}
	frames := parseSSE(rr.Body.String())
	if len(frames) != 2 || frames[0].name != "run_started" || frames[1].name != "run_finished" {
		t.Fatalf("frames = %#v, want [run_started run_finished]", frames)
	}
	var started struct {
		ConversationID string `json:"conversationId"`
		Resumed        bool   `json:"resumed"`
	}
	if err := json.Unmarshal([]byte(frames[0].data), &started); err != nil {
		t.Fatalf("unmarshal run_started: %v", err)
	}
	if started.ConversationID != id || started.Resumed {
		t.Fatalf("run_started = %+v, want conversationId %q resumed false", started, id)
	}
	if !strings.Contains(frames[1].data, "second answer") {
		t.Fatalf("run_finished = %q, want second answer", frames[1].data)
	}

	// The resumed run must have seen the prior history.
	if len(llm.calls) != 2 {
		t.Fatalf("llm.calls = %d, want 2", len(llm.calls))
	}
	msgs := llm.calls[1]
	if len(msgs) != 3 {
		t.Fatalf("resumed context = %d messages, want 3: %#v", len(msgs), msgs)
	}
	if msgs[0].Role != agent.RoleUser || msgs[0].Content != "hi" {
		t.Fatalf("msgs[0] = %#v, want user hi", msgs[0])
	}
	if msgs[1].Role != agent.RoleAssistant || msgs[1].Content != "first answer" {
		t.Fatalf("msgs[1] = %#v, want assistant first answer", msgs[1])
	}
	if msgs[2].Role != agent.RoleUser || msgs[2].Content != "continue" {
		t.Fatalf("msgs[2] = %#v, want user continue", msgs[2])
	}

	// The transcript now has all four turns.
	get := doGet(t, s, "/conversations/"+id)
	var body struct {
		Entries []struct {
			Kind    string `json:"kind"`
			Message struct {
				Content string `json:"content"`
			} `json:"message"`
		} `json:"entries"`
	}
	if err := json.Unmarshal(get.Body.Bytes(), &body); err != nil {
		t.Fatalf("unmarshal get: %v", err)
	}
	if len(body.Entries) != 4 {
		t.Fatalf("len(entries) = %d, want 4: %#v", len(body.Entries), body.Entries)
	}
	if body.Entries[2].Kind != "user" || body.Entries[2].Message.Content != "continue" {
		t.Fatalf("entries[2] = %#v, want user continue", body.Entries[2])
	}
	if body.Entries[3].Kind != "assistant" || body.Entries[3].Message.Content != "second answer" {
		t.Fatalf("entries[3] = %#v, want assistant second answer", body.Entries[3])
	}
}

func TestResumeConversation_SystemPromptOverride(t *testing.T) {
	llm := &fakeLLM{steps: []fakeStep{
		{resp: agent.Response{Content: "a"}},
		{resp: agent.Response{Content: "b"}},
	}}
	s := newTestServer(t, llm, &fakeRegistry{})
	id := runAndGetConversationID(t, s, `{"messages":[{"role":"user","content":"hi"}]}`)

	doResume(t, s, id, `{"systemPrompt":"override","messages":[{"role":"user","content":"go"}]}`)

	msgs := llm.calls[1]
	if len(msgs) == 0 || msgs[0].Role != agent.RoleSystem || msgs[0].Content != "override" {
		t.Fatalf("msgs[0] = %#v, want system override", msgs[0])
	}
}

func TestResumeConversation_NotFoundAndValidation(t *testing.T) {
	s := newTestServer(t, &fakeLLM{}, &fakeRegistry{})

	for _, id := range []string{"1", "999999"} {
		rr := doResume(t, s, id, `{"messages":[{"role":"user","content":"x"}]}`)
		if rr.Code != http.StatusNotFound {
			t.Fatalf("POST /conversations/%s/runs status = %d, want 404", id, rr.Code)
		}
	}

	if rr := doResume(t, s, "abc", `{"messages":[{"role":"user","content":"x"}]}`); rr.Code != http.StatusBadRequest {
		t.Fatalf("POST /conversations/abc/runs status = %d, want 400", rr.Code)
	}

	// A valid conversation but an invalid body must 400.
	id := runAndGetConversationID(t, s, `{"messages":[{"role":"user","content":"hi"}]}`)
	for _, body := range []string{
		`{"messages":[]}`,
		`{"messages":[{"role":"system","content":"x"}]}`,
		`{"messages":[{"role":"user","content":""}]}`,
	} {
		if rr := doResume(t, s, id, body); rr.Code != http.StatusBadRequest {
			t.Fatalf("resume body %q status = %d, want 400", body, rr.Code)
		}
	}
}

func TestListConversations_Validation(t *testing.T) {
	s := newTestServer(t, &fakeLLM{}, &fakeRegistry{})
	for _, path := range []string{
		"/conversations?limit=abc",
		"/conversations?limit=0",
		"/conversations?order=up",
		"/conversations?cursor=notanumber",
	} {
		t.Run(path, func(t *testing.T) {
			rr := doGet(t, s, path)
			if rr.Code != http.StatusBadRequest {
				t.Fatalf("status = %d, want 400", rr.Code)
			}
		})
	}
}
