// Package agent defines the message, event, and interface types shared by the
// durable harness (internal/durable), the provider adapter (internal/llm), and
// the concrete tool set (internal/tools). The tool-using loop itself lives in
// the durable harness's generation task; this package owns no loop.
package agent

import "errors"

// DefaultMaxIterations is the generation loop's safety cap when a run's
// MaxIterations is <= 0.
const DefaultMaxIterations = 50

// ErrMaxIterations is returned when the generation loop exceeds its iteration
// cap.
var ErrMaxIterations = errors.New("agent: max iterations exceeded")
