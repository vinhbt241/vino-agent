package durable

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"vino/internal/agent"
)

func TestBeforeModelHookTransformsContext(t *testing.T) {
	ctx := context.Background()
	base := &fakeRegistry{specs: []agent.ToolSpec{{Name: "keep"}, {Name: "drop"}}}
	llm := &fakeLLM{steps: []llmStep{{resp: agent.Response{Content: "ok"}}}}
	ext := Extension{
		Name: "x",
		Hooks: Hooks{
			BeforeModel: func(_ context.Context, _ *TaskRuntime, req ModelRequest) (ModelRequest, error) {
				req.Messages = append(req.Messages, agent.Message{Role: agent.RoleUser, Content: "injected from hook"})
				var keep []agent.ToolSpec
				for _, s := range req.Tools {
					if s.Name != "drop" {
						keep = append(keep, s)
					}
				}
				req.Tools = keep
				return req, nil
			},
		},
	}
	h, err := Open(ctx, NewMemoryStorage(), HarnessOptions{LLM: llm, Tools: base, Extensions: []Extension{ext}})
	if err != nil {
		t.Fatal(err)
	}
	root := h.Root()
	if err := root.Configure(ctx, AgentConfig{Extensions: []string{"x"}}); err != nil {
		t.Fatal(err)
	}
	sub, err := root.Submit(ctx, SubmitInput{Content: "hi"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := sub.Wait(ctx); err != nil {
		t.Fatal(err)
	}

	if len(llm.messages) != 1 || len(llm.messages[0]) == 0 {
		t.Fatalf("messages = %#v, want one Chat call", llm.messages)
	}
	got := llm.messages[0]
	if got[len(got)-1].Role != agent.RoleUser || got[len(got)-1].Content != "injected from hook" {
		t.Fatalf("last message = %#v, want injected hook message", got[len(got)-1])
	}
	names := map[string]bool{}
	for _, s := range llm.specs[0] {
		names[s.Name] = true
	}
	if !names["keep"] || names["drop"] {
		t.Fatalf("spec names = %v, want keep only (drop removed by hook)", names)
	}
}

func TestAfterModelHookTransformsResponse(t *testing.T) {
	ctx := context.Background()
	base := &fakeRegistry{specs: []agent.ToolSpec{{Name: "read"}}, results: map[string]string{"read": "r"}}
	llm := &fakeLLM{steps: []llmStep{
		{resp: agent.Response{Content: "raw", ToolCalls: []agent.ToolCall{{ID: "c1", Name: "read", Arguments: json.RawMessage(`{"raw":true}`)}}}},
		{resp: agent.Response{Content: "done"}},
	}}
	ext := Extension{
		Name: "x",
		Hooks: Hooks{
			AfterModel: func(_ context.Context, _ *TaskRuntime, resp agent.Response) (agent.Response, error) {
				resp.Content = "rewritten"
				if len(resp.ToolCalls) > 0 {
					resp.ToolCalls[0].Arguments = json.RawMessage(`{"rewritten":true}`)
				}
				return resp, nil
			},
		},
	}
	h, err := Open(ctx, NewMemoryStorage(), HarnessOptions{LLM: llm, Tools: base, Extensions: []Extension{ext}})
	if err != nil {
		t.Fatal(err)
	}
	root := h.Root()
	if err := root.Configure(ctx, AgentConfig{Extensions: []string{"x"}}); err != nil {
		t.Fatal(err)
	}
	sub, err := root.Submit(ctx, SubmitInput{Content: "hi"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := sub.Wait(ctx); err != nil {
		t.Fatal(err)
	}

	entries, err := root.Entries(ctx)
	if err != nil {
		t.Fatal(err)
	}
	var got agent.Message
	for _, e := range entries {
		if e.Kind == kindAssistant && len(e.Model) > 0 && len(e.Model[0].ToolCalls) > 0 {
			got = e.Model[0]
		}
	}
	if got.Content != "rewritten" {
		t.Fatalf("assistant content = %q, want rewritten", got.Content)
	}
	if len(got.ToolCalls) != 1 || string(got.ToolCalls[0].Arguments) != `{"rewritten":true}` {
		t.Fatalf("assistant tool call args = %v, want rewritten", got.ToolCalls)
	}
	if len(base.args) != 1 || string(base.args[0]) != `{"rewritten":true}` {
		t.Fatalf("executed args = %v, want rewritten", base.args)
	}
}

func TestAfterModelHookErrorFaultsRun(t *testing.T) {
	ctx := context.Background()
	llm := &fakeLLM{steps: []llmStep{{resp: agent.Response{Content: "ok"}}}}
	ext := Extension{
		Name: "x",
		Hooks: Hooks{
			AfterModel: func(context.Context, *TaskRuntime, agent.Response) (agent.Response, error) {
				return agent.Response{}, errors.New("hook boom")
			},
		},
	}
	h, err := Open(ctx, NewMemoryStorage(), HarnessOptions{LLM: llm, Extensions: []Extension{ext}})
	if err != nil {
		t.Fatal(err)
	}
	root := h.Root()
	if err := root.Configure(ctx, AgentConfig{Extensions: []string{"x"}}); err != nil {
		t.Fatal(err)
	}
	sub, err := root.Submit(ctx, SubmitInput{Content: "hi"})
	if err != nil {
		t.Fatal(err)
	}
	rec, err := sub.Wait(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if rec.Status != SubmissionUnanswered {
		t.Fatalf("submission status = %s, want unanswered", rec.Status)
	}
	if rec.Reason != "hook boom" {
		t.Fatalf("submission reason = %q, want hook boom", rec.Reason)
	}
}

func TestBeforeToolHookTransformsArgs(t *testing.T) {
	ctx := context.Background()
	base := &fakeRegistry{specs: []agent.ToolSpec{{Name: "read"}}, results: map[string]string{"read": "r"}}
	llm := &fakeLLM{steps: []llmStep{
		{resp: agent.Response{ToolCalls: []agent.ToolCall{{ID: "c1", Name: "read", Arguments: json.RawMessage(`{"a":1}`)}}}},
		{resp: agent.Response{Content: "done"}},
	}}
	ext := Extension{
		Name: "x",
		Hooks: Hooks{
			BeforeTool: func(_ context.Context, _ *TaskRuntime, call agent.ToolCall) (agent.ToolCall, error) {
				call.Arguments = json.RawMessage(`{"a":2}`)
				return call, nil
			},
		},
	}
	h, err := Open(ctx, NewMemoryStorage(), HarnessOptions{LLM: llm, Tools: base, Extensions: []Extension{ext}})
	if err != nil {
		t.Fatal(err)
	}
	root := h.Root()
	if err := root.Configure(ctx, AgentConfig{Extensions: []string{"x"}}); err != nil {
		t.Fatal(err)
	}
	sub, err := root.Submit(ctx, SubmitInput{Content: "hi"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := sub.Wait(ctx); err != nil {
		t.Fatal(err)
	}
	if len(base.args) != 1 || string(base.args[0]) != `{"a":2}` {
		t.Fatalf("executed args = %v, want a=2 (transformed by hook)", base.args)
	}
}

func TestAfterToolHookTransformsResult(t *testing.T) {
	ctx := context.Background()
	base := &fakeRegistry{specs: []agent.ToolSpec{{Name: "read"}}, results: map[string]string{"read": "secret"}}
	llm := &fakeLLM{steps: []llmStep{
		{resp: agent.Response{ToolCalls: []agent.ToolCall{{ID: "c1", Name: "read", Arguments: json.RawMessage(`{}`)}}}},
		{resp: agent.Response{Content: "done"}},
	}}
	ext := Extension{
		Name: "x",
		Hooks: Hooks{
			AfterTool: func(_ context.Context, _ *TaskRuntime, o ToolOutcome) (ToolOutcome, error) {
				o.Result = "sanitized"
				return o, nil
			},
		},
	}
	h, err := Open(ctx, NewMemoryStorage(), HarnessOptions{LLM: llm, Tools: base, Extensions: []Extension{ext}})
	if err != nil {
		t.Fatal(err)
	}
	root := h.Root()
	if err := root.Configure(ctx, AgentConfig{Extensions: []string{"x"}}); err != nil {
		t.Fatal(err)
	}
	sub, err := root.Submit(ctx, SubmitInput{Content: "hi"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := sub.Wait(ctx); err != nil {
		t.Fatal(err)
	}

	entries, err := root.Entries(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if got := toolEntryContent(t, entries); got != "sanitized" {
		t.Fatalf("tool entry content = %q, want sanitized", got)
	}
	if len(llm.messages) != 2 {
		t.Fatalf("chat calls = %d, want 2", len(llm.messages))
	}
	found := false
	for _, m := range llm.messages[1] {
		if m.Role == agent.RoleTool && m.Content == "sanitized" {
			found = true
		}
	}
	if !found {
		t.Fatalf("second call messages = %#v, want sanitized tool result", llm.messages[1])
	}
}

func TestToolHookErrorSurfacesAsToolResult(t *testing.T) {
	ctx := context.Background()
	base := &fakeRegistry{specs: []agent.ToolSpec{{Name: "read"}}, results: map[string]string{"read": "r"}}
	llm := &fakeLLM{steps: []llmStep{
		{resp: agent.Response{ToolCalls: []agent.ToolCall{{ID: "c1", Name: "read", Arguments: json.RawMessage(`{}`)}}}},
		{resp: agent.Response{Content: "done"}},
	}}
	ext := Extension{
		Name: "x",
		Hooks: Hooks{
			BeforeTool: func(context.Context, *TaskRuntime, agent.ToolCall) (agent.ToolCall, error) {
				return agent.ToolCall{}, errors.New("hook veto")
			},
		},
	}
	h, err := Open(ctx, NewMemoryStorage(), HarnessOptions{LLM: llm, Tools: base, Extensions: []Extension{ext}})
	if err != nil {
		t.Fatal(err)
	}
	root := h.Root()
	if err := root.Configure(ctx, AgentConfig{Extensions: []string{"x"}}); err != nil {
		t.Fatal(err)
	}
	sub, err := root.Submit(ctx, SubmitInput{Content: "hi"})
	if err != nil {
		t.Fatal(err)
	}
	rec, err := sub.Wait(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if rec.Status != SubmissionDone {
		t.Fatalf("submission status = %s, want done (tool hook error is a tool result, not a run fault)", rec.Status)
	}
	entries, err := root.Entries(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if got := toolEntryContent(t, entries); got != "hook veto" {
		t.Fatalf("tool entry content = %q, want hook veto", got)
	}
	if len(base.calls) != 0 {
		t.Fatalf("base calls = %v, want none (hook vetoed before execution)", base.calls)
	}
}

func TestHookOrdering(t *testing.T) {
	ctx := context.Background()
	llm := &fakeLLM{steps: []llmStep{{resp: agent.Response{Content: "ok"}}}}
	mk := func(s string) Extension {
		return Extension{
			Name: s,
			Hooks: Hooks{
				BeforeModel: func(_ context.Context, _ *TaskRuntime, req ModelRequest) (ModelRequest, error) {
					req.Messages = append(req.Messages, agent.Message{Role: agent.RoleUser, Content: s})
					return req, nil
				},
			},
		}
	}
	h, err := Open(ctx, NewMemoryStorage(), HarnessOptions{LLM: llm, Extensions: []Extension{mk("A"), mk("B")}})
	if err != nil {
		t.Fatal(err)
	}
	root := h.Root()
	if err := root.Configure(ctx, AgentConfig{Extensions: []string{"A", "B"}}); err != nil {
		t.Fatal(err)
	}
	sub, err := root.Submit(ctx, SubmitInput{Content: "hi"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := sub.Wait(ctx); err != nil {
		t.Fatal(err)
	}
	got := llm.messages[0]
	if len(got) < 2 || got[len(got)-2].Content != "A" || got[len(got)-1].Content != "B" {
		t.Fatalf("tail messages = %#v, want A then B in activation order", got)
	}
}

func TestHookMemoPersistsAcrossSteps(t *testing.T) {
	ctx := context.Background()
	base := &fakeRegistry{specs: []agent.ToolSpec{{Name: "read"}}, results: map[string]string{"read": "r"}}
	llm := &fakeLLM{steps: []llmStep{
		{resp: agent.Response{ToolCalls: []agent.ToolCall{{ID: "c1", Name: "read", Arguments: json.RawMessage(`{}`)}}}},
		{resp: agent.Response{Content: "done"}},
	}}
	var reads []json.RawMessage
	ext := Extension{
		Name: "x",
		Hooks: Hooks{
			BeforeModel: func(_ context.Context, rt *TaskRuntime, req ModelRequest) (ModelRequest, error) {
				reads = append(reads, rt.MemoValue("greeted"))
				if rt.MemoValue("greeted") == nil {
					if _, err := rt.Memo("greeted", json.RawMessage(`1`)); err != nil {
						return req, err
					}
					req.Messages = append(req.Messages, agent.Message{Role: agent.RoleUser, Content: "first"})
				} else {
					req.Messages = append(req.Messages, agent.Message{Role: agent.RoleUser, Content: "second"})
				}
				return req, nil
			},
		},
	}
	h, err := Open(ctx, NewMemoryStorage(), HarnessOptions{LLM: llm, Tools: base, Extensions: []Extension{ext}})
	if err != nil {
		t.Fatal(err)
	}
	root := h.Root()
	if err := root.Configure(ctx, AgentConfig{Extensions: []string{"x"}}); err != nil {
		t.Fatal(err)
	}
	sub, err := root.Submit(ctx, SubmitInput{Content: "hi"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := sub.Wait(ctx); err != nil {
		t.Fatal(err)
	}
	if len(reads) != 2 {
		t.Fatalf("hook invocations = %d, want 2", len(reads))
	}
	if reads[0] != nil {
		t.Fatalf("first read = %s, want nil (no memo yet)", reads[0])
	}
	if string(reads[1]) != `1` {
		t.Fatalf("second read = %s, want 1 (memo committed by step 1)", reads[1])
	}
}

func TestHookStagedMemoDiscardedOnFault(t *testing.T) {
	ctx := context.Background()
	llm := &fakeLLM{steps: []llmStep{{resp: agent.Response{Content: "ok"}}}}
	ext := Extension{
		Name: "x",
		Hooks: Hooks{
			BeforeModel: func(_ context.Context, rt *TaskRuntime, req ModelRequest) (ModelRequest, error) {
				if _, err := rt.Memo("k", json.RawMessage(`"v"`)); err != nil {
					return req, err
				}
				return req, errors.New("boom")
			},
		},
	}
	h, err := Open(ctx, NewMemoryStorage(), HarnessOptions{LLM: llm, Extensions: []Extension{ext}})
	if err != nil {
		t.Fatal(err)
	}
	root := h.Root()
	if err := root.Configure(ctx, AgentConfig{Extensions: []string{"x"}}); err != nil {
		t.Fatal(err)
	}
	sub, err := root.Submit(ctx, SubmitInput{Content: "hi"})
	if err != nil {
		t.Fatal(err)
	}
	rec, err := sub.Wait(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if rec.Status != SubmissionUnanswered {
		t.Fatalf("submission status = %s, want unanswered", rec.Status)
	}
	page, err := h.session.storage.ScanTasks(ctx, TaskQuery{}, 0, nil)
	if err != nil {
		t.Fatal(err)
	}
	for _, task := range page.Items {
		if task.Kind == kindGeneration && len(task.Memos) != 0 {
			t.Fatalf("generation task memos = %v, want empty (staged memo discarded with the faulted step)", task.Memos)
		}
	}
}

func TestWrapToolWrapsBaseTool(t *testing.T) {
	ctx := context.Background()
	base := &fakeRegistry{specs: []agent.ToolSpec{{Name: "read"}}, results: map[string]string{"read": "raw"}}
	llm := &fakeLLM{steps: []llmStep{
		{resp: agent.Response{ToolCalls: []agent.ToolCall{{ID: "c1", Name: "read", Arguments: json.RawMessage(`{}`)}}}},
		{resp: agent.Response{Content: "done"}},
	}}
	ext := Extension{
		Name: "x",
		WrapTool: map[string]ToolWrapper{
			"read": func(ctx context.Context, name string, args json.RawMessage, next func(context.Context, string, json.RawMessage) (string, error)) (string, error) {
				r, err := next(ctx, name, args)
				if err != nil {
					return "", err
				}
				return "wrapped:" + r, nil
			},
		},
	}
	h, err := Open(ctx, NewMemoryStorage(), HarnessOptions{LLM: llm, Tools: base, Extensions: []Extension{ext}})
	if err != nil {
		t.Fatal(err)
	}
	root := h.Root()
	if err := root.Configure(ctx, AgentConfig{Extensions: []string{"x"}}); err != nil {
		t.Fatal(err)
	}
	sub, err := root.Submit(ctx, SubmitInput{Content: "hi"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := sub.Wait(ctx); err != nil {
		t.Fatal(err)
	}
	entries, err := root.Entries(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if got := toolEntryContent(t, entries); got != "wrapped:raw" {
		t.Fatalf("tool entry content = %q, want wrapped:raw", got)
	}
	if len(base.calls) != 1 || base.calls[0] != "read" {
		t.Fatalf("base calls = %v, want [read] (wrapper reached the base provider)", base.calls)
	}
}

func TestWrapToolWrapsShadowWinner(t *testing.T) {
	ctx := context.Background()
	base := &fakeRegistry{specs: []agent.ToolSpec{{Name: "read"}}, results: map[string]string{"read": "base result"}}
	shadow := &fakeRegistry{specs: []agent.ToolSpec{{Name: "read"}}, results: map[string]string{"read": "shadow result"}}
	llm := &fakeLLM{steps: []llmStep{
		{resp: agent.Response{ToolCalls: []agent.ToolCall{{ID: "c1", Name: "read", Arguments: json.RawMessage(`{}`)}}}},
		{resp: agent.Response{Content: "done"}},
	}}
	wrapExt := Extension{
		Name: "wrap",
		WrapTool: map[string]ToolWrapper{
			"read": func(ctx context.Context, name string, args json.RawMessage, next func(context.Context, string, json.RawMessage) (string, error)) (string, error) {
				r, err := next(ctx, name, args)
				if err != nil {
					return "", err
				}
				return "wrapped:" + r, nil
			},
		},
	}
	h, err := Open(ctx, NewMemoryStorage(), HarnessOptions{LLM: llm, Tools: base, Extensions: []Extension{
		{Name: "shadow", Tools: shadow},
		wrapExt,
	}})
	if err != nil {
		t.Fatal(err)
	}
	root := h.Root()
	if err := root.Configure(ctx, AgentConfig{Extensions: []string{"shadow", "wrap"}}); err != nil {
		t.Fatal(err)
	}
	sub, err := root.Submit(ctx, SubmitInput{Content: "hi"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := sub.Wait(ctx); err != nil {
		t.Fatal(err)
	}
	entries, err := root.Entries(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if got := toolEntryContent(t, entries); got != "wrapped:shadow result" {
		t.Fatalf("tool entry content = %q, want wrapped:shadow result (wrapper around the shadow winner)", got)
	}
	if len(shadow.calls) != 1 || len(base.calls) != 0 {
		t.Fatalf("shadow calls = %v base calls = %v, want shadow only", shadow.calls, base.calls)
	}
}

func TestWrapToolNestsInActivationOrder(t *testing.T) {
	ctx := context.Background()
	base := &fakeRegistry{specs: []agent.ToolSpec{{Name: "read"}}, results: map[string]string{"read": "raw"}}
	llm := &fakeLLM{steps: []llmStep{
		{resp: agent.Response{ToolCalls: []agent.ToolCall{{ID: "c1", Name: "read", Arguments: json.RawMessage(`{}`)}}}},
		{resp: agent.Response{Content: "done"}},
	}}
	wrap := func(tag string) ToolWrapper {
		return func(ctx context.Context, name string, args json.RawMessage, next func(context.Context, string, json.RawMessage) (string, error)) (string, error) {
			r, err := next(ctx, name, args)
			if err != nil {
				return "", err
			}
			return tag + "(" + r + ")", nil
		}
	}
	h, err := Open(ctx, NewMemoryStorage(), HarnessOptions{LLM: llm, Tools: base, Extensions: []Extension{
		{Name: "A", WrapTool: map[string]ToolWrapper{"read": wrap("A")}},
		{Name: "B", WrapTool: map[string]ToolWrapper{"read": wrap("B")}},
	}})
	if err != nil {
		t.Fatal(err)
	}
	root := h.Root()
	if err := root.Configure(ctx, AgentConfig{Extensions: []string{"A", "B"}}); err != nil {
		t.Fatal(err)
	}
	sub, err := root.Submit(ctx, SubmitInput{Content: "hi"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := sub.Wait(ctx); err != nil {
		t.Fatal(err)
	}
	entries, err := root.Entries(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if got := toolEntryContent(t, entries); got != "A(B(raw))" {
		t.Fatalf("tool entry content = %q, want A(B(raw)) (earliest-activated wrapper outermost)", got)
	}
}

func TestWrapToolDoesNotChangeSpecs(t *testing.T) {
	ctx := context.Background()
	base := &fakeRegistry{specs: []agent.ToolSpec{{Name: "read", Description: "base desc"}}, results: map[string]string{"read": "r"}}
	llm := &fakeLLM{steps: []llmStep{
		{resp: agent.Response{ToolCalls: []agent.ToolCall{{ID: "c1", Name: "read", Arguments: json.RawMessage(`{}`)}}}},
		{resp: agent.Response{Content: "done"}},
	}}
	ext := Extension{
		Name: "x",
		WrapTool: map[string]ToolWrapper{
			"read": func(ctx context.Context, name string, args json.RawMessage, next func(context.Context, string, json.RawMessage) (string, error)) (string, error) {
				return next(ctx, name, args)
			},
		},
	}
	h, err := Open(ctx, NewMemoryStorage(), HarnessOptions{LLM: llm, Tools: base, Extensions: []Extension{ext}})
	if err != nil {
		t.Fatal(err)
	}
	root := h.Root()
	if err := root.Configure(ctx, AgentConfig{Extensions: []string{"x"}}); err != nil {
		t.Fatal(err)
	}
	sub, err := root.Submit(ctx, SubmitInput{Content: "hi"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := sub.Wait(ctx); err != nil {
		t.Fatal(err)
	}
	if len(llm.specs) < 1 || len(llm.specs[0]) != 1 {
		t.Fatalf("specs = %#v, want a first call with one spec", llm.specs)
	}
	if s := llm.specs[0][0]; s.Name != "read" || s.Description != "base desc" {
		t.Fatalf("spec = %+v, want the winner's spec unchanged", s)
	}
}
