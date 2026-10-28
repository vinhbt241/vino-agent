// Package durable implements the load-bearing core of a durable agent
// harness: a small Storage interface with memory and JSONL backends, a
// Session mutation line, durable task state machines with crash recovery,
// submissions with exactly-once request IDs, and a thin Harness facade.
package durable

// Seq is a strictly increasing commit sequence. Gaps are permitted.
type Seq uint64

// ID is a record identifier minted from one global numeric namespace shared
// by every record kind. Kind brands are enforced by convention, not type.
type ID uint64

// RootConversationID is the conversation every harness creates on first use.
const RootConversationID ID = 1
