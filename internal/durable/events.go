package durable

import (
	"sync"

	"vino/internal/agent"
)

// EventFanout delivers one conversation's live run events to every subscriber
// of that conversation: the submit-stream sinks (keyed by the durable
// submission ID, so a crash-retried request re-attaches its sink to the same
// run) and the WatchEvents subscribers (keyed by a local counter). The two
// namespaces are disjoint. A sink error detaches that subscriber and never
// aborts the run: a durable run must complete and persist a consistent
// transcript even when nobody is watching.
type EventFanout struct {
	mu sync.Mutex
	// submitSinks streams one run's events to the client that started it,
	// keyed by submission ID (a storage-minted ID).
	submitSinks map[ID]map[uint64]agent.EventSink
	// watchSinks holds WatchEvents subscribers, keyed by a local counter — a
	// separate namespace from storage-minted submission IDs.
	watchSinks map[ID]map[uint64]agent.EventSink
	watchSeq   uint64
}

// newEventFanout returns an empty fan-out.
func newEventFanout() *EventFanout {
	return &EventFanout{
		submitSinks: make(map[ID]map[uint64]agent.EventSink),
		watchSinks:  make(map[ID]map[uint64]agent.EventSink),
	}
}

// RegisterSubmit attaches sink to conversationID under submissionID. A later
// registration with the same submission ID replaces the sink (the crash-retry
// resume path). A nil sink is ignored.
func (f *EventFanout) RegisterSubmit(conversationID, submissionID ID, sink agent.EventSink) {
	if sink == nil {
		return
	}
	f.mu.Lock()
	if f.submitSinks[conversationID] == nil {
		f.submitSinks[conversationID] = make(map[uint64]agent.EventSink)
	}
	f.submitSinks[conversationID][uint64(submissionID)] = sink
	f.mu.Unlock()
}

// UnregisterSubmit removes the submit sink registered for submissionID under
// conversationID. Removing a missing sink is a no-op.
func (f *EventFanout) UnregisterSubmit(conversationID, submissionID ID) {
	f.mu.Lock()
	if subs := f.submitSinks[conversationID]; subs != nil {
		delete(subs, uint64(submissionID))
		if len(subs) == 0 {
			delete(f.submitSinks, conversationID)
		}
	}
	f.mu.Unlock()
}

// Subscribe registers fn as a conversation-scoped WatchEvents subscriber and
// returns an idempotent cancel. A nil fn returns a no-op cancel.
func (f *EventFanout) Subscribe(conversationID ID, fn agent.EventSink) func() {
	if fn == nil {
		return func() {}
	}
	f.mu.Lock()
	f.watchSeq++
	id := f.watchSeq
	if f.watchSinks[conversationID] == nil {
		f.watchSinks[conversationID] = make(map[uint64]agent.EventSink)
	}
	f.watchSinks[conversationID][id] = fn
	f.mu.Unlock()
	return func() {
		f.removeWatch(conversationID, id)
	}
}

// removeWatch detaches one watch subscriber. Idempotent.
func (f *EventFanout) removeWatch(conversationID ID, id uint64) {
	f.mu.Lock()
	if subs := f.watchSinks[conversationID]; subs != nil {
		delete(subs, id)
		if len(subs) == 0 {
			delete(f.watchSinks, conversationID)
		}
	}
	f.mu.Unlock()
}

// Emit delivers ev to every subscriber of conversationID — both submit sinks
// and WatchEvents subscribers — outside the lock, so a slow sink never stalls
// registration. A sink error (e.g. the client disconnected) detaches that
// subscriber and does not abort the run.
func (f *EventFanout) Emit(conversationID ID, ev agent.Event) {
	f.mu.Lock()
	submit := make(map[uint64]agent.EventSink, len(f.submitSinks[conversationID]))
	for id, sink := range f.submitSinks[conversationID] {
		submit[id] = sink
	}
	watch := make(map[uint64]agent.EventSink, len(f.watchSinks[conversationID]))
	for id, sink := range f.watchSinks[conversationID] {
		watch[id] = sink
	}
	f.mu.Unlock()

	for id, sink := range submit {
		if err := sink(ev); err != nil {
			f.UnregisterSubmit(conversationID, ID(id))
		}
	}
	for id, sink := range watch {
		if err := sink(ev); err != nil {
			f.removeWatch(conversationID, id)
		}
	}
}
