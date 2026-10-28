package api

import (
	"os"
	"path/filepath"
	"testing"
)

func writeDotenv(t *testing.T, content string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), ".env")
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatalf("write .env: %v", err)
	}
	return path
}

func unsetEnv(t *testing.T, keys ...string) {
	t.Helper()
	for _, k := range keys {
		if err := os.Unsetenv(k); err != nil {
			t.Errorf("unsetenv %s: %v", k, err)
		}
	}
}

func TestLoadDotenv_SetsVars(t *testing.T) {
	path := writeDotenv(t, `
# full-line comment
VINO_TEST_A=plain
VINO_TEST_B="quoted value"
VINO_TEST_C='single quoted'
export VINO_TEST_D=exported
VINO_TEST_E=value # inline comment
`)
	defer unsetEnv(t, "VINO_TEST_A", "VINO_TEST_B", "VINO_TEST_C", "VINO_TEST_D", "VINO_TEST_E")

	if err := LoadDotenv(path); err != nil {
		t.Fatalf("LoadDotenv() error = %v, want nil", err)
	}

	want := map[string]string{
		"VINO_TEST_A": "plain",
		"VINO_TEST_B": "quoted value",
		"VINO_TEST_C": "single quoted",
		"VINO_TEST_D": "exported",
		"VINO_TEST_E": "value",
	}
	for k, v := range want {
		if got := os.Getenv(k); got != v {
			t.Errorf("os.Getenv(%q) = %q, want %q", k, got, v)
		}
	}
}

func TestLoadDotenv_DoesNotOverrideExistingEnv(t *testing.T) {
	t.Setenv("VINO_TEST_EXISTING", "real")

	path := writeDotenv(t, "VINO_TEST_EXISTING=from-dotenv\n")
	if err := LoadDotenv(path); err != nil {
		t.Fatalf("LoadDotenv() error = %v, want nil", err)
	}

	if got := os.Getenv("VINO_TEST_EXISTING"); got != "real" {
		t.Fatalf("os.Getenv(VINO_TEST_EXISTING) = %q, want real (env wins over .env)", got)
	}
}

func TestLoadDotenv_MissingFile(t *testing.T) {
	if err := LoadDotenv(filepath.Join(t.TempDir(), ".env")); err != nil {
		t.Fatalf("LoadDotenv() error = %v, want nil (missing .env is optional)", err)
	}
}

func TestLoadDotenv_Malformed(t *testing.T) {
	tests := []struct {
		name    string
		content string
	}{
		{"no equals", "VINO_TEST_A\n"},
		{"empty key", "=value\n"},
		{"unterminated quote", "VINO_TEST_A=\"oops\n"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			path := writeDotenv(t, tt.content)
			if err := LoadDotenv(path); err == nil {
				t.Fatal("LoadDotenv() error = nil, want error")
			}
		})
	}
}

func TestLoadDotenv_FeedsConfig(t *testing.T) {
	envPath := writeDotenv(t, "VINO_PROVIDER_OPENAI_API_KEY=dotenv-key\n")
	cfgPath := writeConfig(t, `{"default":"openai","providers":{"openai":{"baseURL":"b","model":"m"}}}`)
	defer unsetEnv(t, "VINO_PROVIDER_OPENAI_API_KEY")

	if err := LoadDotenv(envPath); err != nil {
		t.Fatalf("LoadDotenv() error = %v, want nil", err)
	}
	cfg, err := LoadConfig(cfgPath)
	if err != nil {
		t.Fatalf("LoadConfig() error = %v, want nil", err)
	}
	if got := cfg.Providers["openai"].APIKey; got != "dotenv-key" {
		t.Fatalf("APIKey = %q, want dotenv-key", got)
	}
}
