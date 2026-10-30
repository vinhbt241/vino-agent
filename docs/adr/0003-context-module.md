# ADR-0003: "What the model sees" is assembled in one context module

**Status:** Accepted

**Date:** 2025-10-31

## Context

Assembling the model's prompt required holding two files in your head at once:
`harness.go` (`buildMessages`, `ancestorEntries`, `rawEntriesUpTo`) and
`compaction.go` (`latestCompaction`, `liveTail`). Fork knowledge (the ancestor
walk) and compaction knowledge (the summary + live tail) leaked across module
boundaries, and there was no single interface to test the assembled context
against.

## Decision

`internal/durable/context.go` owns everything that decides what the model sees.

- `contextMessages(ctx, conversationID, systemPrompt)` is the single entry
  point: system prompt, then fork ancestors' raw history, then the
  conversation's own compaction summary and live tail, then schema assembly.
- `ancestorEntries` and `rawEntriesUpTo` (fork knowledge) and
  `latestCompaction` and `liveTail` (compaction knowledge) are internal to this
  module.
- `contextMessages` is the only caller of `assembleToolTurns` (from
  `transcript.go`); the schema is applied once, unavoidably, at the seam.

## Consequences

- The model context is testable through one interface (`contextMessages`);
  `TestContextMessagesAssemblesFullContext` covers system prompt, ancestors,
  summary, tail, and schema assembly in one call.
- Fork and compaction knowledge no longer leak: `harness.go` keeps no context
  assembly, and `compaction.go` keeps only the `Compact` write operation, which
  reuses the module's `liveTail`.
- `Compact` still builds its own summarization prompt (instruction + previous
  summary + live tail) because that prompt is a write input, not the model's
  context; the two share `liveTail` and `assembleToolTurns`.
