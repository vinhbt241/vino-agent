package api

import (
	"context"
	"encoding/json"
	"errors"

	"vino/internal/agent"
)

// fakeStep is one scripted LLM turn.
type fakeStep struct {
	deltas []string
	resp   agent.Response
	err    error
}

// fakeLLM is a scripted LLM. It records every message list it is given.
type fakeLLM struct {
	steps []fakeStep
	calls [][]agent.Message
}

func (f *fakeLLM) Chat(_ context.Context, msgs []agent.Message, _ []agent.ToolSpec, onDelta func(string) error) (agent.Response, error) {
	f.calls = append(f.calls, append([]agent.Message(nil), msgs...))
	if len(f.steps) == 0 {
		return agent.Response{}, errors.New("fakeLLM: no more scripted steps")
	}
	step := f.steps[0]
	f.steps = f.steps[1:]
	if step.err != nil {
		return agent.Response{}, step.err
	}
	for _, d := range step.deltas {
		if err := onDelta(d); err != nil {
			return agent.Response{}, err
		}
	}
	return step.resp, nil
}

// fakeRegistry is a scripted ToolRegistry. Every call returns the same result.
type fakeRegistry struct {
	result string
	err    error
}

func (f *fakeRegistry) Specs() []agent.ToolSpec { return nil }

func (f *fakeRegistry) Call(_ context.Context, _ string, _ json.RawMessage) (string, error) {
	return f.result, f.err
}
