package api

import (
	"os"
	"path/filepath"
	"testing"
)

func writeConfig(t *testing.T, content string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "vino.json")
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}
	return path
}

func TestLoadConfig_ValidFile(t *testing.T) {
	path := writeConfig(t, `{
		"addr": "127.0.0.1:9000",
		"default": "openai",
		"providers": {
			"openai": {"apiKey": "k", "baseURL": "https://api.openai.com/v1", "model": "gpt-4o"}
		},
		"tools": {"rootDir": "/tmp/root", "workDir": "/tmp/work"}
	}`)

	cfg, err := LoadConfig(path)
	if err != nil {
		t.Fatalf("LoadConfig() error = %v, want nil", err)
	}
	if cfg.Addr != "127.0.0.1:9000" {
		t.Fatalf("Addr = %q, want 127.0.0.1:9000", cfg.Addr)
	}
	if cfg.Default != "openai" {
		t.Fatalf("Default = %q, want openai", cfg.Default)
	}
	p := cfg.Providers["openai"]
	if p.APIKey != "k" || p.BaseURL != "https://api.openai.com/v1" || p.Model != "gpt-4o" {
		t.Fatalf("Providers[openai] = %#v, want key/baseURL/model parsed", p)
	}
	if cfg.Tools.RootDir != "/tmp/root" || cfg.Tools.WorkDir != "/tmp/work" {
		t.Fatalf("Tools = %#v, want rootDir/workDir parsed", cfg.Tools)
	}
}

func TestLoadConfig_DefaultsAddr(t *testing.T) {
	path := writeConfig(t, `{
		"default": "openai",
		"providers": {"openai": {"apiKey": "k", "baseURL": "b", "model": "m"}}
	}`)

	cfg, err := LoadConfig(path)
	if err != nil {
		t.Fatalf("LoadConfig() error = %v, want nil", err)
	}
	if cfg.Addr != "127.0.0.1:8080" {
		t.Fatalf("Addr = %q, want default 127.0.0.1:8080", cfg.Addr)
	}
}

func TestLoadConfig_EnvOverridesProvider(t *testing.T) {
	t.Setenv("VINO_PROVIDER_OPENAI_API_KEY", "envkey")
	t.Setenv("VINO_PROVIDER_OPENAI_BASE_URL", "http://localhost:1234/v1")
	t.Setenv("VINO_PROVIDER_OPENAI_MODEL", "env-model")

	path := writeConfig(t, `{
		"default": "openai",
		"providers": {"openai": {"apiKey": "filekey", "baseURL": "https://x", "model": "m"}}
	}`)

	cfg, err := LoadConfig(path)
	if err != nil {
		t.Fatalf("LoadConfig() error = %v, want nil", err)
	}
	p := cfg.Providers["openai"]
	if p.APIKey != "envkey" {
		t.Fatalf("APIKey = %q, want envkey", p.APIKey)
	}
	if p.BaseURL != "http://localhost:1234/v1" {
		t.Fatalf("BaseURL = %q, want env override", p.BaseURL)
	}
	if p.Model != "env-model" {
		t.Fatalf("Model = %q, want env-model", p.Model)
	}
}

func TestLoadConfig_EmptyEnvDoesNotOverride(t *testing.T) {
	t.Setenv("VINO_PROVIDER_OPENAI_API_KEY", "")

	path := writeConfig(t, `{
		"default": "openai",
		"providers": {"openai": {"apiKey": "filekey", "baseURL": "b", "model": "m"}}
	}`)

	cfg, err := LoadConfig(path)
	if err != nil {
		t.Fatalf("LoadConfig() error = %v, want nil", err)
	}
	if cfg.Providers["openai"].APIKey != "filekey" {
		t.Fatalf("APIKey = %q, want file value (empty env ignored)", cfg.Providers["openai"].APIKey)
	}
}

func TestLoadConfig_Errors(t *testing.T) {
	tests := []struct {
		name    string
		content string
	}{
		{"missing default", `{"providers": {"openai": {"apiKey": "k", "baseURL": "b", "model": "m"}}}`},
		{"default not found", `{"default": "x", "providers": {"openai": {"apiKey": "k", "baseURL": "b", "model": "m"}}}`},
		{"keyless provider", `{"default": "openai", "providers": {"openai": {"baseURL": "b", "model": "m"}}}`},
		{"missing baseURL", `{"default": "openai", "providers": {"openai": {"apiKey": "k", "model": "m"}}}`},
		{"missing model", `{"default": "openai", "providers": {"openai": {"apiKey": "k", "baseURL": "b"}}}`},
		{"invalid json", `{`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			path := writeConfig(t, tt.content)
			if _, err := LoadConfig(path); err == nil {
				t.Fatal("LoadConfig() error = nil, want error")
			}
		})
	}
}

func TestLoadConfig_MissingFile(t *testing.T) {
	if _, err := LoadConfig(filepath.Join(t.TempDir(), "nope.json")); err == nil {
		t.Fatal("LoadConfig() error = nil, want error")
	}
}

func TestLoadConfig_CORS(t *testing.T) {
	path := writeConfig(t, `{
		"default": "openai",
		"providers": {"openai": {"apiKey": "k", "baseURL": "b", "model": "m"}},
		"cors": {"origins": ["http://localhost:5173"], "allowCredentials": true, "maxAge": 60}
	}`)

	cfg, err := LoadConfig(path)
	if err != nil {
		t.Fatalf("LoadConfig() error = %v, want nil", err)
	}
	if len(cfg.CORS.Origins) != 1 || cfg.CORS.Origins[0] != "http://localhost:5173" {
		t.Fatalf("CORS.Origins = %#v, want [http://localhost:5173]", cfg.CORS.Origins)
	}
	if !cfg.CORS.AllowCredentials {
		t.Fatal("CORS.AllowCredentials = false, want true")
	}
	if cfg.CORS.MaxAge != 60 {
		t.Fatalf("CORS.MaxAge = %d, want 60", cfg.CORS.MaxAge)
	}
}

func TestLoadConfig_CORSStarWithCredentialsRejected(t *testing.T) {
	path := writeConfig(t, `{
		"default": "openai",
		"providers": {"openai": {"apiKey": "k", "baseURL": "b", "model": "m"}},
		"cors": {"origins": ["*"], "allowCredentials": true}
	}`)

	if _, err := LoadConfig(path); err == nil {
		t.Fatal("LoadConfig() error = nil, want error")
	}
}
