package api

import (
	"bufio"
	"fmt"
	"os"
	"strings"
)

// LoadDotenv parses a .env file and exports each KEY=VALUE pair into the
// process environment, so later config loading (which reads os.Getenv) can
// see it. A missing file is not an error — .env is optional. Variables
// already present in the real environment are left untouched: the environment
// wins over the file.
//
// Supported syntax: full-line `#` comments, blank lines, optional `export `
// prefix, single- or double-quoted values, and inline ` #` comments on
// unquoted values.
func LoadDotenv(path string) error {
	f, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return fmt.Errorf("api: opening .env: %w", err)
	}
	defer f.Close()

	scanner := bufio.NewScanner(f)
	lineNo := 0
	for scanner.Scan() {
		lineNo++
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		line = strings.TrimPrefix(line, "export ")
		key, value, ok := strings.Cut(line, "=")
		if !ok {
			return fmt.Errorf("api: .env:%d: expected KEY=VALUE", lineNo)
		}
		key = strings.TrimSpace(key)
		if key == "" {
			return fmt.Errorf("api: .env:%d: empty key", lineNo)
		}
		value, err = parseDotenvValue(value)
		if err != nil {
			return fmt.Errorf("api: .env:%d: %w", lineNo, err)
		}
		// The real environment wins over the file.
		if _, exists := os.LookupEnv(key); !exists {
			if err := os.Setenv(key, value); err != nil {
				return fmt.Errorf("api: .env:%d: %w", lineNo, err)
			}
		}
	}
	if err := scanner.Err(); err != nil {
		return fmt.Errorf("api: reading .env: %w", err)
	}
	return nil
}

// parseDotenvValue returns the value of a KEY=value line's right-hand side.
// Quoted values keep their literal contents; unquoted values are trimmed and
// may have a trailing inline comment stripped.
func parseDotenvValue(raw string) (string, error) {
	s := strings.TrimSpace(raw)
	if s == "" {
		return "", nil
	}
	if s[0] == '"' || s[0] == '\'' {
		quote := s[0]
		end := strings.IndexByte(s[1:], quote)
		if end < 0 {
			return "", fmt.Errorf("unterminated quote")
		}
		return s[1 : end+1], nil
	}
	if i := strings.Index(s, " #"); i >= 0 {
		s = strings.TrimSpace(s[:i])
	}
	return s, nil
}
