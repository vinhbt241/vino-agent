package tools

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os/exec"
	"strings"
	"time"

	"vino/internal/agent"
)

type bashArgs struct {
	Command string `json:"command"`
	Timeout *int   `json:"timeout"`
}

func bashSpec() agent.ToolSpec {
	return agent.ToolSpec{
		Name: "bash",
		Description: "Execute a shell command in the working directory. Returns " +
			"combined stdout and stderr. Optionally provide a timeout in seconds.",
		Parameters: json.RawMessage(`{
			"type": "object",
			"properties": {
				"command": {"type": "string", "description": "Shell command to execute"},
				"timeout": {"type": "integer", "description": "Timeout in seconds (optional, no default timeout)"}
			},
			"required": ["command"],
			"additionalProperties": false
		}`),
	}
}

func (r *Registry) bash(ctx context.Context, args json.RawMessage) (string, error) {
	var in bashArgs
	if err := decodeArgs(args, &in); err != nil {
		return "", fmt.Errorf("tools: bash: %w", err)
	}
	if in.Command == "" {
		return "", fmt.Errorf("tools: bash: command is required")
	}

	runCtx := ctx
	var cancel context.CancelFunc
	if in.Timeout != nil {
		if *in.Timeout < 0 {
			return "", fmt.Errorf("tools: bash: timeout must be >= 0")
		}
		if *in.Timeout > 0 {
			runCtx, cancel = context.WithTimeout(ctx, time.Duration(*in.Timeout)*time.Second)
			defer cancel()
		}
	}

	cmd := exec.CommandContext(runCtx, "sh", "-c", in.Command)
	cmd.Dir = r.workDir
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	if err := cmd.Run(); err != nil {
		if runCtx.Err() != nil {
			return "", runCtx.Err()
		}
		var exitErr *exec.ExitError
		if errors.As(err, &exitErr) {
			return "", shellExitError(exitErr.ExitCode(), stdout.String(), stderr.String())
		}
		return "", err
	}

	out := stdout.String()
	if se := stderr.String(); se != "" {
		if out != "" {
			out += "\n"
		}
		out += se
	}
	return out, nil
}

// shellExitError formats a non-zero exit as a tool error the kernel relays to
// the model: the exit status plus the combined output.
func shellExitError(code int, stdout, stderr string) error {
	combined := stdout
	if stderr != "" {
		if combined != "" {
			combined += "\n"
		}
		combined += stderr
	}
	combined = strings.TrimSpace(combined)
	if combined == "" {
		return fmt.Errorf("shell: exit status %d", code)
	}
	return fmt.Errorf("shell: exit status %d\n%s", code, combined)
}
