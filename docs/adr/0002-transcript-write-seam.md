# ADR-0002: The tool-call transcript schema is owned by one write seam

**Status:** Accepted

**Date:** 2025-10-31

## Context

The durable loop must satisfy the provider's tool-call schema: every assistant
`tool_calls` message is immediately followed by exactly one tool message per
call. Before this change the invariant was maintained by hand across three
writer paths (`toolTaskDef.Step`, `toolRestartLimitResult`, `abortToolResult`),
each carrying its own placeholder string, with a fourth placeholder and a
bolted-on `sanitizeToolCalls` patching the result at read time in two call
sites (`buildMessages` and `Compact`). The cancel-mid-tool-call corruption
(58b39cf) had to touch six files because the invariant lived in the read path
as a convention, not in the write path as a guarantee.

## Decision

`internal/durable/transcript.go` is the single owner of the tool-call schema.

- Every assistant entry is written through `appendAssistantEntry`; every tool
  entry through `appendToolResultEntry`. These are the only construction sites.
- The placeholder result strings (`abortedToolResult`, `cancelledToolResult`,
  `crashInterruptedToolResult`) live in this one module.
- Context assembly goes through `assembleToolTurns`, which reattaches each tool
  result to its assistant `tool_calls` by `ToolCallID`, fills missing results,
  and drops orphan tool messages. It replaces `sanitizeToolCalls` and is the
  only place the entry→message schema is enforced.
- `toolTurnSchemaHolds` is the single predicate the invariant is tested against.

## Consequences

- The schema's vocabulary and shape have one home, so the cancel-mid-tool-call
  class of bug is fixed in one module instead of six files.
- `assembleToolTurns` is turn-aware: a concurrent submit can interleave its user
  entry between an assistant `tool_calls` and its tool results (entry IDs are
  monotonic), and the tool results are still pulled back next to their assistant
  turn. The previous `sanitizeToolCalls` produced a malformed context in this
  case (placeholder plus orphaned real results).
- The write path and the read path are no longer two copies of the invariant to
  keep in sync; the write seam and the assembly function share one module and
  one schema predicate.
