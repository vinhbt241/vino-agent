package durable

import "context"

// SettleIntent is the terminal transition a task step asks the scheduler to
// apply atomically with the step's own commit. Only the "done" transition
// rides the step's return: unanswered settles own their commit (abort,
// cut-off, fault).
type SettleIntent struct {
	SubmissionID ID
	AnswerID     ID
}

// settlement owns a submission's terminal transition: placed -> done (with an
// answer) or placed -> unanswered (with a reason). The "settled once" rule —
// the first terminal status wins, a later settle is a no-op — lives in one
// apply helper, so every settle path re-checks under the commit lock and a
// concurrent abort always wins over a late answer. See GLOSSARY.md
// "Submission settlement".
type settlement struct {
	session *Session
}

// ApplyDone writes the done transition into tx, idempotently. The scheduler
// calls it from the generation step's terminal commit so the answer entry, the
// task outcome, and the submission settle in one atomic commit.
func (s settlement) ApplyDone(ctx context.Context, tx *Tx, submissionID, answerID ID) error {
	_, err := s.apply(ctx, tx, submissionID, func(sub *SubmissionRecord) {
		sub.Status = SubmissionDone
		sub.AnswerID = &answerID
	})
	return err
}

// ApplyUnanswered writes the unanswered transition into tx, idempotently.
// Callers that already hold a commit closure (Conversation.Abort) call this
// directly.
func (s settlement) ApplyUnanswered(ctx context.Context, tx *Tx, submissionID ID, reason string) error {
	_, err := s.apply(ctx, tx, submissionID, func(sub *SubmissionRecord) {
		sub.Status = SubmissionUnanswered
		sub.Reason = reason
	})
	return err
}

// SettleUnanswered settles a submission unanswered in its own commit and
// returns the conversation it belongs to, for callers that don't already hold
// a commit (abort, cut-off, fault).
func (s settlement) SettleUnanswered(ctx context.Context, submissionID ID, reason string) (ID, error) {
	var convID ID
	err := s.session.Commit(ctx, func(tx *Tx) error {
		var err error
		convID, err = s.apply(ctx, tx, submissionID, func(sub *SubmissionRecord) {
			sub.Status = SubmissionUnanswered
			sub.Reason = reason
		})
		return err
	})
	return convID, err
}

// apply reads the submission under the commit lock, applies mutate only when
// the submission is still placed (not yet settled), and stages the write. A
// missing submission is a no-op: a settle for a submission that was never
// placed is not an error.
func (s settlement) apply(ctx context.Context, tx *Tx, submissionID ID, mutate func(*SubmissionRecord)) (ID, error) {
	sub, err := tx.Submission(ctx, submissionID)
	if err != nil || sub == nil {
		return 0, err
	}
	if sub.Status != SubmissionDone && sub.Status != SubmissionUnanswered {
		mutate(sub)
		tx.WriteSubmission(sub)
	}
	return sub.ConversationID, nil
}
