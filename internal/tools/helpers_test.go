package tools

import (
	"context"
	"encoding/json"
	"testing"
)

func mustNew(t *testing.T, opts Options) *Registry {
	t.Helper()
	r, err := New(opts)
	if err != nil {
		t.Fatalf("New(%+v) error = %v, want nil", opts, err)
	}
	return r
}

// call invokes the named tool with args marshalled to JSON. A nil args is
// passed through as an empty payload.
func call(t *testing.T, r *Registry, name string, args any) (string, error) {
	t.Helper()
	var raw []byte
	if args != nil {
		b, err := json.Marshal(args)
		if err != nil {
			t.Fatalf("marshal %s args: %v", name, err)
		}
		raw = b
	}
	return r.Call(context.Background(), name, raw)
}
