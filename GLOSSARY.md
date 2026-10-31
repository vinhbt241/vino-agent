# Glossary

## Submission

A host request the agent answers, tracked as a `SubmissionRecord` and
deduplicated by `RequestID`. Lifecycle: `queued → placed → done | unanswered`.

## Submission settlement

The idempotent transition of a submission from `placed` to a terminal status:
`done` (with an answer entry) or `unanswered` (with a reason). *Settled once*:
the first terminal status wins; a later settle is a no-op. Owned by the
`settlement` module in `internal/durable`, applied under the session commit
lock so a concurrent abort always wins over a late answer.
