# ADR-0001: A single durable tool-using loop

**Status:** Accepted

**Date:** 2025-10-30

## Context

The repo once shipped two implementations of the tool-using loop: `internal/agent.Agent.Run`
(a non-durable, in-memory kernel) and `internal/durable`'s `generationTask` (the durable,
crash-recovering loop the HTTP server actually drives). The two copies diverged: hardening
for transcript-corruption-on-cancel, partial-answer persistence, and empty-content model
panics landed only in the durable copy. `Agent.Run` had no production caller — its only
consumer was its own test file.

## Decision

There is exactly one tool-using loop, and it lives in the durable harness's `generationTask`
(`internal/durable/generation.go`). The `internal/agent` package owns only the shared types
and interfaces the loop and its collaborators need: `Message`, `Event`, `LLM`, `ToolRegistry`,
`ToolSpec`, `Role`, `ReplayMode`, `DefaultMaxIterations`, `ErrMaxIterations`.

The dead kernel (`Agent`, `RunInput`, `RunResult`, and their helpers) was deleted.

## Consequences

- Loop semantics (iteration cap, tool timeout, cancellation, transcript invariants) are fixed
  in one place, not two.
- A future in-process, non-durable run must wrap the durable harness rather than reintroduce
  a second loop. An architecture review that suggests a parallel loop should be treated as
  re-litigating this ADR.
