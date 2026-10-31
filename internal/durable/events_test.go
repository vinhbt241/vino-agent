package durable

import (
	"errors"
	"testing"

	"vino/internal/agent"
)

func textEvent(t string) agent.Event {
	return agent.Event{Type: agent.EventTextDelta, TextDelta: &agent.TextDelta{Text: t}}
}

func collect(out *[]string) agent.EventSink {
	return func(ev agent.Event) error {
		if ev.TextDelta != nil {
			*out = append(*out, ev.TextDelta.Text)
		}
		return nil
	}
}

func TestEventFanoutEmitsToSubmitAndWatch(t *testing.T) {
	f := newEventFanout()
	var submit, watch []string
	f.RegisterSubmit(10, 1, collect(&submit))
	f.Subscribe(10, collect(&watch))

	f.Emit(10, textEvent("a"))

	if len(submit) != 1 || submit[0] != "a" {
		t.Fatalf("submit = %v, want [a]", submit)
	}
	if len(watch) != 1 || watch[0] != "a" {
		t.Fatalf("watch = %v, want [a]", watch)
	}
}

func TestEventFanoutConversationScoped(t *testing.T) {
	f := newEventFanout()
	var a, b []string
	f.Subscribe(1, collect(&a))
	f.Subscribe(2, collect(&b))

	f.Emit(1, textEvent("x"))

	if len(a) != 1 || a[0] != "x" {
		t.Fatalf("conversation 1 = %v, want [x]", a)
	}
	if len(b) != 0 {
		t.Fatalf("conversation 2 = %v, want none (fan-out is conversation-scoped)", b)
	}
}

func TestEventFanoutSinkErrorDetaches(t *testing.T) {
	f := newEventFanout()
	var good []string
	badCalls := 0
	bad := func(ev agent.Event) error {
		badCalls++
		return errors.New("sink failed")
	}
	f.Subscribe(1, bad)
	f.Subscribe(1, collect(&good))

	f.Emit(1, textEvent("a"))
	f.Emit(1, textEvent("b"))

	if badCalls != 1 {
		t.Fatalf("bad sink called %d times, want 1 (detached after error)", badCalls)
	}
	if len(good) != 2 {
		t.Fatalf("good sink = %v, want [a b] (one sink error never aborts fan-out)", good)
	}
}

func TestEventFanoutCancelIdempotent(t *testing.T) {
	f := newEventFanout()
	var out []string
	cancel := f.Subscribe(1, collect(&out))
	cancel()
	cancel() // second cancel is a no-op
	f.Emit(1, textEvent("a"))
	if len(out) != 0 {
		t.Fatalf("out = %v, want none after cancel", out)
	}
}

func TestEventFanoutUnregisterSubmitOnlyThatSubmission(t *testing.T) {
	f := newEventFanout()
	var one, two []string
	f.RegisterSubmit(10, 1, collect(&one))
	f.RegisterSubmit(10, 2, collect(&two))
	f.UnregisterSubmit(10, 1)
	f.Emit(10, textEvent("a"))
	if len(one) != 0 {
		t.Fatalf("submission 1 = %v, want none after unregister", one)
	}
	if len(two) != 1 {
		t.Fatalf("submission 2 = %v, want [a]", two)
	}
}

func TestEventFanoutNilSinkIgnored(t *testing.T) {
	f := newEventFanout()
	f.RegisterSubmit(10, 1, nil) // must not panic
	var out []string
	f.Subscribe(10, collect(&out))
	f.Emit(10, textEvent("a"))
	if len(out) != 1 {
		t.Fatalf("out = %v, want [a]", out)
	}
}

func TestEventFanoutNilSubscribeReturnsNoopCancel(t *testing.T) {
	f := newEventFanout()
	cancel := f.Subscribe(1, nil)
	if cancel == nil {
		t.Fatal("Subscribe(nil) returned nil cancel, want no-op cancel")
	}
	cancel() // must not panic
}
