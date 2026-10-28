package api

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"vino/internal/agent"
	"vino/internal/durable"
)

// newTestServer wires fake LLM and tools into a Server over an in-memory
// durable harness, so tests exercise the real run path without a JSONL file.
func newTestServer(t *testing.T, llm agent.LLM, reg agent.ToolRegistry) *Server {
	t.Helper()
	h, err := durable.Open(context.Background(), durable.NewMemoryStorage(), durable.HarnessOptions{LLM: llm, Tools: reg})
	if err != nil {
		t.Fatal(err)
	}
	return newServer(h)
}

// openHarness opens a harness over the JSONL log at path, as a server restart
// would.
func openHarness(t *testing.T, path string, llm agent.LLM, reg agent.ToolRegistry) *durable.Harness {
	t.Helper()
	store, err := durable.OpenJSONL(path, false)
	if err != nil {
		t.Fatal(err)
	}
	h, err := durable.Open(context.Background(), store, durable.HarnessOptions{LLM: llm, Tools: reg})
	if err != nil {
		t.Fatal(err)
	}
	return h
}

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

// blockingRegistry blocks its first Call until released, to simulate a tool
// whose process is killed mid-execution. Its one tool is replay-safe.
type blockingRegistry struct {
	started chan struct{}
	release chan struct{}
}

func (b *blockingRegistry) Specs() []agent.ToolSpec {
	return []agent.ToolSpec{{Name: "search", Replay: agent.ReplaySafe}}
}

func (b *blockingRegistry) Call(_ context.Context, _ string, _ json.RawMessage) (string, error) {
	close(b.started)
	<-b.release
	return "hit", nil
}
