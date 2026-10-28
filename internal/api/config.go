// Package api implements the durable-agent HTTP server: config loading,
// routing, and the SSE run endpoint. It wires the kernel (internal/agent) to
// a real provider (internal/llm) and the concrete tool set (internal/tools).
package api

import (
	"encoding/json"
	"fmt"
	"os"
	"strings"
)

const defaultAddr = "127.0.0.1:8080"

// Config is the server's startup configuration.
type Config struct {
	Addr        string              `json:"addr"`
	Default     string              `json:"default"`
	Providers   map[string]Provider `json:"providers"`
	Tools       ToolsConfig         `json:"tools"`
	DurablePath string              `json:"durablePath"`
}

// Provider is one named LLM provider. APIKey may be empty in the file and
// filled from the environment at load time.
type Provider struct {
	APIKey  string `json:"apiKey"`
	BaseURL string `json:"baseURL"`
	Model   string `json:"model"`
}

// ToolsConfig configures the tool registry's filesystem jail.
type ToolsConfig struct {
	RootDir string `json:"rootDir"`
	WorkDir string `json:"workDir"`
}

// LoadConfig reads path, merges per-provider env overrides
// (VINO_PROVIDER_<NAME>_API_KEY / _BASE_URL / _MODEL), defaults Addr, and
// validates that the default provider exists and every provider is complete.
func LoadConfig(path string) (Config, error) {
	var cfg Config
	data, err := os.ReadFile(path)
	if err != nil {
		return Config{}, fmt.Errorf("api: reading config: %w", err)
	}
	if err := json.Unmarshal(data, &cfg); err != nil {
		return Config{}, fmt.Errorf("api: decoding config: %w", err)
	}
	applyEnv(&cfg)
	if cfg.Addr == "" {
		cfg.Addr = defaultAddr
	}
	if cfg.DurablePath == "" {
		cfg.DurablePath = "vino.jsonl"
	}
	if err := cfg.validate(); err != nil {
		return Config{}, err
	}
	return cfg, nil
}

// applyEnv merges provider overrides from the environment. An unset or empty
// env var leaves the file value untouched.
func applyEnv(cfg *Config) {
	for name, p := range cfg.Providers {
		prefix := "VINO_PROVIDER_" + strings.ToUpper(name) + "_"
		if v := os.Getenv(prefix + "API_KEY"); v != "" {
			p.APIKey = v
		}
		if v := os.Getenv(prefix + "BASE_URL"); v != "" {
			p.BaseURL = v
		}
		if v := os.Getenv(prefix + "MODEL"); v != "" {
			p.Model = v
		}
		cfg.Providers[name] = p
	}
}

func (c Config) validate() error {
	if c.Default == "" {
		return fmt.Errorf("api: config: default provider is required")
	}
	if _, ok := c.Providers[c.Default]; !ok {
		return fmt.Errorf("api: config: default provider %q not found in providers", c.Default)
	}
	for name, p := range c.Providers {
		switch {
		case p.APIKey == "":
			return fmt.Errorf("api: config: provider %q: apiKey is required", name)
		case p.BaseURL == "":
			return fmt.Errorf("api: config: provider %q: baseURL is required", name)
		case p.Model == "":
			return fmt.Errorf("api: config: provider %q: model is required", name)
		}
	}
	return nil
}
