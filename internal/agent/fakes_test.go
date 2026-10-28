package agent

import (
	"context"
	"encoding/json"
	"errors"
)

// fakeStep is one scripted LLM turn: optional streamed deltas, a complete
// response, or an error.
type fakeStep struct {
	deltas []string
	resp   Response
	err    error
}

// fakeLLM is a scripted LLM for tests. It records every message list it is
// given and pops one fakeStep per Chat call.
type fakeLLM struct {
	steps []fakeStep
	calls [][]Message
}

func (f *fakeLLM) Chat(_ context.Context, msgs []Message, onDelta func(string) error) (Response, error) {
	f.calls = append(f.calls, append([]Message(nil), msgs...))
	if len(f.steps) == 0 {
		return Response{}, errors.New("fakeLLM: no more scripted steps")
	}
	step := f.steps[0]
	f.steps = f.steps[1:]
	if step.err != nil {
		return Response{}, step.err
	}
	for _, d := range step.deltas {
		if err := onDelta(d); err != nil {
			return Response{}, err
		}
	}
	return step.resp, nil
}

type fakeToolResult struct {
	result string
	err    error
}

type fakeCall struct {
	name string
	args json.RawMessage
}

// fakeRegistry is a scripted ToolRegistry for tests. It records every call
// and returns the scripted result/error for each tool name.
type fakeRegistry struct {
	specs   []ToolSpec
	results map[string]fakeToolResult
	calls   []fakeCall
}

func (f *fakeRegistry) Specs() []ToolSpec { return f.specs }

func (f *fakeRegistry) Call(_ context.Context, name string, args json.RawMessage) (string, error) {
	f.calls = append(f.calls, fakeCall{name: name, args: args})
	r := f.results[name]
	return r.result, r.err
}
