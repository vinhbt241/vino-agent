package durable

import (
	"context"
	"testing"
)

// TestSubmissionSettlementIdempotent pins the "settled once" rule at the
// settlement seam: the first terminal status wins, a later settle is a no-op,
// and a settle for a missing submission is not an error. The done path is
// applied inside a commit (as the scheduler does); the unanswered path is
// settled in its own commit (as abort/cut-off/fault do).
func TestSubmissionSettlementIdempotent(t *testing.T) {
	ctx := context.Background()
	st := NewMemoryStorage()
	s := settlement{session: OpenSession(st)}

	convID := mint(t, st, ctx)
	subID := mint(t, st, ctx)
	answerID := mint(t, st, ctx)
	write(t, st, ctx, []StorageWrite{
		{Type: WriteConversation, Value: &ConversationRecord{ID: convID}},
		{Type: WriteSubmission, Value: &SubmissionRecord{ID: subID, ConversationID: convID, Type: SubmissionInput, Status: SubmissionPlaced}},
	})

	// done first, then unanswered: done, its answer, and an empty reason win.
	if err := s.session.Commit(ctx, func(tx *Tx) error {
		return s.ApplyDone(ctx, tx, subID, answerID)
	}); err != nil {
		t.Fatal(err)
	}
	if err := s.session.Commit(ctx, func(tx *Tx) error {
		return s.ApplyUnanswered(ctx, tx, subID, "aborted")
	}); err != nil {
		t.Fatal(err)
	}
	sub, err := st.Submission(ctx, subID)
	if err != nil {
		t.Fatal(err)
	}
	if sub.Status != SubmissionDone {
		t.Fatalf("status = %s, want done", sub.Status)
	}
	if sub.AnswerID == nil || *sub.AnswerID != answerID {
		t.Fatalf("answerID = %v, want %d", sub.AnswerID, answerID)
	}
	if sub.Reason != "" {
		t.Fatalf("reason = %q, want empty", sub.Reason)
	}

	// unanswered first, then done: unanswered and its reason win.
	subID2 := mint(t, st, ctx)
	write(t, st, ctx, []StorageWrite{
		{Type: WriteSubmission, Value: &SubmissionRecord{ID: subID2, ConversationID: convID, Type: SubmissionInput, Status: SubmissionPlaced}},
	})
	gotConv, err := s.SettleUnanswered(ctx, subID2, "aborted")
	if err != nil {
		t.Fatal(err)
	}
	if gotConv != convID {
		t.Fatalf("SettleUnanswered conversation = %d, want %d", gotConv, convID)
	}
	if err := s.session.Commit(ctx, func(tx *Tx) error {
		return s.ApplyDone(ctx, tx, subID2, answerID)
	}); err != nil {
		t.Fatal(err)
	}
	sub2, err := st.Submission(ctx, subID2)
	if err != nil {
		t.Fatal(err)
	}
	if sub2.Status != SubmissionUnanswered {
		t.Fatalf("status = %s, want unanswered", sub2.Status)
	}
	if sub2.Reason != "aborted" {
		t.Fatalf("reason = %q, want %q", sub2.Reason, "aborted")
	}
	if sub2.AnswerID != nil {
		t.Fatalf("answerID = %v, want nil", sub2.AnswerID)
	}

	// A settle for a missing submission is a no-op, not an error.
	if err := s.session.Commit(ctx, func(tx *Tx) error {
		return s.ApplyDone(ctx, tx, ID(999999), answerID)
	}); err != nil {
		t.Fatalf("settle missing submission: %v", err)
	}
}
