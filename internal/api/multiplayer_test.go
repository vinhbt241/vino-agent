package api

import (
	"bufio"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"vino/internal/agent"
	"vino/internal/durable"
)

// commitFrame is the test-side shape of a /watch "commit" SSE frame.
type commitFrame struct {
	Seq    string `json:"seq"`
	Writes []struct {
		Type   string          `json:"type"`
		Record json.RawMessage `json:"record"`
	} `json:"writes"`
}

func readSSE(t *testing.T, body io.Reader) <-chan sseFrame {
	t.Helper()
	ch := make(chan sseFrame, 16)
	go func() {
		defer close(ch)
		sc := bufio.NewScanner(body)
		sc.Buffer(make([]byte, 0, 64*1024), 1<<20)
		var name, data string
		for sc.Scan() {
			line := sc.Text()
			switch {
			case line == "":
				if name != "" || data != "" {
					ch <- sseFrame{name: name, data: data}
					name, data = "", ""
				}
			case strings.HasPrefix(line, "event: "):
				name = strings.TrimPrefix(line, "event: ")
			case strings.HasPrefix(line, "data: "):
				data = strings.TrimPrefix(line, "data: ")
			}
		}
	}()
	return ch
}

func nextFrame(t *testing.T, ch <-chan sseFrame) sseFrame {
	t.Helper()
	select {
	case f, ok := <-ch:
		if !ok {
			t.Fatal("SSE stream closed unexpectedly")
		}
		return f
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for SSE frame")
		return sseFrame{}
	}
}

func startConversation(t *testing.T, s *Server, content string) (*durable.Submission, durable.ID) {
	t.Helper()
	sub, err := s.harness.Start(context.Background(), durable.StartInput{
		Messages: []agent.Message{{Role: agent.RoleUser, Content: content}},
	}, nil)
	if err != nil {
		t.Fatalf("harness.Start: %v", err)
	}
	return sub, sub.ConversationID()
}

func TestViewConversation(t *testing.T) {
	s := newTestServer(t, &fakeLLM{}, &fakeRegistry{})
	_, convID := startConversation(t, s, "hi")
	id := formatID(convID)

	rr := httptest.NewRecorder()
	s.ServeHTTP(rr, httptest.NewRequest(http.MethodGet, "/conversations/"+id+"/view", nil))

	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rr.Code)
	}
	var v viewDTO
	if err := json.Unmarshal(rr.Body.Bytes(), &v); err != nil {
		t.Fatalf("unmarshal view: %v", err)
	}
	if v.Conversation.ID != id {
		t.Fatalf("conversation.id = %q, want %q", v.Conversation.ID, id)
	}
	if len(v.Entries) != 1 || v.Entries[0].Message.Content != "hi" {
		t.Fatalf("entries = %#v, want one user entry \"hi\"", v.Entries)
	}
	if len(v.Submissions) != 1 {
		t.Fatalf("submissions = %d, want 1", len(v.Submissions))
	}
	if v.Seq == "" || v.Seq == "0" {
		t.Fatalf("seq = %q, want a positive sequence", v.Seq)
	}
}

func TestViewConversationNotFound(t *testing.T) {
	s := newTestServer(t, &fakeLLM{}, &fakeRegistry{})

	rr := httptest.NewRecorder()
	s.ServeHTTP(rr, httptest.NewRequest(http.MethodGet, "/conversations/999999/view", nil))

	if rr.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want 404", rr.Code)
	}
}

func TestWatchConversationCatchesUp(t *testing.T) {
	s := newTestServer(t, &fakeLLM{}, &fakeRegistry{})
	_, convID := startConversation(t, s, "first")
	id := formatID(convID)

	ctx := context.Background()
	c, err := s.harness.Conversation(ctx, convID)
	if err != nil {
		t.Fatal(err)
	}
	v, err := c.View(ctx)
	if err != nil {
		t.Fatal(err)
	}
	// A second commit to catch up from v.Seq.
	if _, err := c.Submit(ctx, durable.SubmitInput{Content: "second"}); err != nil {
		t.Fatal(err)
	}

	ts := httptest.NewServer(s)
	defer ts.Close()
	reqCtx, cancel := context.WithCancel(context.Background())
	defer cancel()
	req, err := http.NewRequestWithContext(reqCtx, http.MethodGet, ts.URL+"/conversations/"+id+"/watch?after="+formatSeq(v.Seq), nil)
	if err != nil {
		t.Fatal(err)
	}
	resp, err := ts.Client().Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}

	f := nextFrame(t, readSSE(t, resp.Body))
	if f.name != "commit" {
		t.Fatalf("event = %q, want commit", f.name)
	}
	var cf commitFrame
	if err := json.Unmarshal([]byte(f.data), &cf); err != nil {
		t.Fatalf("unmarshal commit frame: %v", err)
	}
	var entry entryDTO
	found := false
	for _, w := range cf.Writes {
		if w.Type == "entry" {
			if err := json.Unmarshal(w.Record, &entry); err != nil {
				t.Fatalf("unmarshal entry record: %v", err)
			}
			found = true
		}
	}
	if !found {
		t.Fatalf("writes = %#v, want an entry write", cf.Writes)
	}
	if entry.Message.Content != "second" {
		t.Fatalf("entry content = %q, want second", entry.Message.Content)
	}
}

func TestWatchConversationStreamsLiveCommit(t *testing.T) {
	s := newTestServer(t, &fakeLLM{}, &fakeRegistry{})
	_, convID := startConversation(t, s, "first")
	id := formatID(convID)

	ctx := context.Background()
	c, err := s.harness.Conversation(ctx, convID)
	if err != nil {
		t.Fatal(err)
	}
	v, err := c.View(ctx)
	if err != nil {
		t.Fatal(err)
	}

	ts := httptest.NewServer(s)
	defer ts.Close()
	reqCtx, cancel := context.WithCancel(context.Background())
	defer cancel()
	req, err := http.NewRequestWithContext(reqCtx, http.MethodGet, ts.URL+"/conversations/"+id+"/watch?after="+formatSeq(v.Seq), nil)
	if err != nil {
		t.Fatal(err)
	}
	resp, err := ts.Client().Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	frames := readSSE(t, resp.Body)

	// Commit after the watch is registered (Do returned only after the
	// handler flushed its 200, which happens after Watch registers).
	if _, err := c.Submit(ctx, durable.SubmitInput{Content: "live"}); err != nil {
		t.Fatal(err)
	}

	f := nextFrame(t, frames)
	if f.name != "commit" {
		t.Fatalf("event = %q, want commit", f.name)
	}
	var cf commitFrame
	if err := json.Unmarshal([]byte(f.data), &cf); err != nil {
		t.Fatalf("unmarshal commit frame: %v", err)
	}
	var entry entryDTO
	for _, w := range cf.Writes {
		if w.Type == "entry" {
			if err := json.Unmarshal(w.Record, &entry); err != nil {
				t.Fatalf("unmarshal entry record: %v", err)
			}
		}
	}
	if entry.Message.Content != "live" {
		t.Fatalf("entry content = %q, want live", entry.Message.Content)
	}
}

func TestWatchConversationExpiredSeq(t *testing.T) {
	s := newTestServer(t, &fakeLLM{}, &fakeRegistry{})
	_, convID := startConversation(t, s, "first")
	id := formatID(convID)

	ts := httptest.NewServer(s)
	defer ts.Close()
	// after=0 predates the retained window (which starts at the harness's
	// first commit), so Watch must refuse and tell the client to re-view.
	resp, err := ts.Client().Get(ts.URL + "/conversations/" + id + "/watch?after=0")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusConflict {
		t.Fatalf("status = %d, want 409", resp.StatusCode)
	}
	body := new(strings.Builder)
	if _, err := io.Copy(body, resp.Body); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(body.String(), "re-read /view") {
		t.Fatalf("body = %q, want re-read /view guidance", body.String())
	}
}

func TestWatchConversationBadAfter(t *testing.T) {
	s := newTestServer(t, &fakeLLM{}, &fakeRegistry{})
	_, convID := startConversation(t, s, "first")
	id := formatID(convID)

	ts := httptest.NewServer(s)
	defer ts.Close()
	resp, err := ts.Client().Get(ts.URL + "/conversations/" + id + "/watch?after=xyz")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", resp.StatusCode)
	}
}

func TestWatchConversationMissingAfter(t *testing.T) {
	s := newTestServer(t, &fakeLLM{}, &fakeRegistry{})
	_, convID := startConversation(t, s, "first")
	id := formatID(convID)

	ts := httptest.NewServer(s)
	defer ts.Close()
	resp, err := ts.Client().Get(ts.URL + "/conversations/" + id + "/watch")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", resp.StatusCode)
	}
}

func TestEventsConversationFansOut(t *testing.T) {
	llm := &fakeLLM{steps: []fakeStep{{deltas: []string{"Hel", "lo"}, resp: agent.Response{Content: "Hello"}}}}
	s := newTestServer(t, llm, &fakeRegistry{})
	_, convID := startConversation(t, s, "go")
	id := formatID(convID)

	ts := httptest.NewServer(s)
	defer ts.Close()
	reqCtx, cancel := context.WithCancel(context.Background())
	defer cancel()
	req, err := http.NewRequestWithContext(reqCtx, http.MethodGet, ts.URL+"/conversations/"+id+"/events", nil)
	if err != nil {
		t.Fatal(err)
	}
	resp, err := ts.Client().Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	frames := readSSE(t, resp.Body)

	// Run the conversation after the event stream is registered.
	ctx := context.Background()
	c, err := s.harness.Conversation(ctx, convID)
	if err != nil {
		t.Fatal(err)
	}
	sub, err := c.Submit(ctx, durable.SubmitInput{Content: "run"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := sub.Wait(ctx); err != nil {
		t.Fatal(err)
	}

	var got []string
	for i := 0; i < 3; i++ {
		got = append(got, nextFrame(t, frames).name)
	}
	if got[0] != "text_delta" || got[1] != "text_delta" || got[2] != "run_finished" {
		t.Fatalf("event names = %#v, want [text_delta text_delta run_finished]", got)
	}
}
