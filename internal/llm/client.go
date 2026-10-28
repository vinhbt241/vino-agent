package llm

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"vino/internal/agent"
)

// Config configures a Client. APIKey, BaseURL, and Model are required.
type Config struct {
	APIKey  string // provider auth token
	BaseURL string // e.g. https://api.openai.com/v1
	Model   string // model identifier sent to the provider

	// HTTPClient is the transport used for requests. nil means http.DefaultClient.
	HTTPClient *http.Client
	// Timeout bounds each Chat call. 0 means no timeout.
	Timeout time.Duration
}

// Client is an OpenAI-compatible streaming chat client implementing agent.LLM.
type Client struct {
	apiKey  string
	baseURL string
	model   string
	http    *http.Client
	timeout time.Duration
}

// New validates cfg and returns a ready Client.
func New(cfg Config) (*Client, error) {
	switch {
	case cfg.APIKey == "":
		return nil, errors.New("llm: APIKey is required")
	case cfg.BaseURL == "":
		return nil, errors.New("llm: BaseURL is required")
	case cfg.Model == "":
		return nil, errors.New("llm: Model is required")
	}
	httpClient := cfg.HTTPClient
	if httpClient == nil {
		httpClient = http.DefaultClient
	}
	return &Client{
		apiKey:  cfg.APIKey,
		baseURL: strings.TrimRight(cfg.BaseURL, "/"),
		model:   cfg.Model,
		http:    httpClient,
		timeout: cfg.Timeout,
	}, nil
}

// Chat implements agent.LLM. It streams assistant text through onDelta and
// returns tool calls complete once the response finishes.
func (c *Client) Chat(ctx context.Context, messages []agent.Message, tools []agent.ToolSpec, onDelta func(string) error) (agent.Response, error) {
	if onDelta == nil {
		onDelta = func(string) error { return nil }
	}

	req := chatRequest{
		Model:    c.model,
		Messages: toOpenAIMessages(messages),
		Tools:    toOpenAITools(tools),
		Stream:   true,
	}
	body, err := json.Marshal(req)
	if err != nil {
		return agent.Response{}, fmt.Errorf("llm: encoding request: %w", err)
	}

	callCtx := ctx
	var cancel context.CancelFunc
	if c.timeout > 0 {
		callCtx, cancel = context.WithTimeout(ctx, c.timeout)
		defer cancel()
	}

	httpReq, err := http.NewRequestWithContext(callCtx, http.MethodPost, c.baseURL+"/chat/completions", bytes.NewReader(body))
	if err != nil {
		return agent.Response{}, fmt.Errorf("llm: building request: %w", err)
	}
	httpReq.Header.Set("Content-Type", "application/json")
	httpReq.Header.Set("Authorization", "Bearer "+c.apiKey)

	resp, err := c.http.Do(httpReq)
	if err != nil {
		return agent.Response{}, err
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return agent.Response{}, statusError(resp)
	}

	return parseStream(resp.Body, onDelta)
}

// statusError builds an error from a non-2xx response, including a truncated
// body for diagnostics.
func statusError(resp *http.Response) error {
	const maxBody = 512
	b, _ := io.ReadAll(io.LimitReader(resp.Body, maxBody))
	return fmt.Errorf("llm: %s: %s", resp.Status, strings.TrimSpace(string(b)))
}
