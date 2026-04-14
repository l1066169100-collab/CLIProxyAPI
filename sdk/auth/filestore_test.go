package auth

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
)

func TestExtractAccessToken(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		metadata map[string]any
		expected string
	}{
		{
			"antigravity top-level access_token",
			map[string]any{"access_token": "tok-abc"},
			"tok-abc",
		},
		{
			"gemini nested token.access_token",
			map[string]any{
				"token": map[string]any{"access_token": "tok-nested"},
			},
			"tok-nested",
		},
		{
			"top-level takes precedence over nested",
			map[string]any{
				"access_token": "tok-top",
				"token":        map[string]any{"access_token": "tok-nested"},
			},
			"tok-top",
		},
		{
			"empty metadata",
			map[string]any{},
			"",
		},
		{
			"whitespace-only access_token",
			map[string]any{"access_token": "   "},
			"",
		},
		{
			"wrong type access_token",
			map[string]any{"access_token": 12345},
			"",
		},
		{
			"token is not a map",
			map[string]any{"token": "not-a-map"},
			"",
		},
		{
			"nested whitespace-only",
			map[string]any{
				"token": map[string]any{"access_token": "  "},
			},
			"",
		},
		{
			"fallback to nested when top-level empty",
			map[string]any{
				"access_token": "",
				"token":        map[string]any{"access_token": "tok-fallback"},
			},
			"tok-fallback",
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got := extractAccessToken(tt.metadata)
			if got != tt.expected {
				t.Errorf("extractAccessToken() = %q, want %q", got, tt.expected)
			}
		})
	}
}

func TestShouldIgnoreAuthPath(t *testing.T) {
	t.Parallel()

	baseDir := "/tmp/auth"
	tests := []struct {
		path string
		want bool
	}{
		{"/tmp/auth/auth-cleaner-state.json", true},
		{"/tmp/auth/reports/auth-cleaner/report-1.json", true},
		{"/tmp/auth/backups/auth-cleaner/20260414/foo.json", true},
		{"/tmp/auth/codex-user.json", false},
		{"/tmp/auth/team/codex-user.json", false},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.path, func(t *testing.T) {
			t.Parallel()
			if got := ShouldIgnoreAuthPath(baseDir, tt.path); got != tt.want {
				t.Fatalf("ShouldIgnoreAuthPath(%q) = %v, want %v", tt.path, got, tt.want)
			}
		})
	}
}

func TestFileTokenStoreListSkipsAuthCleanerArtifacts(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	store := NewFileTokenStore()
	store.SetBaseDir(dir)

	writeJSON := func(rel string, payload map[string]any) {
		t.Helper()
		full := filepath.Join(dir, filepath.FromSlash(rel))
		if err := os.MkdirAll(filepath.Dir(full), 0o755); err != nil {
			t.Fatalf("mkdir %s: %v", rel, err)
		}
		data, err := json.Marshal(payload)
		if err != nil {
			t.Fatalf("marshal %s: %v", rel, err)
		}
		if err := os.WriteFile(full, data, 0o644); err != nil {
			t.Fatalf("write %s: %v", rel, err)
		}
	}

	writeJSON("real.json", map[string]any{"type": "codex", "email": "real@example.com"})
	writeJSON("auth-cleaner-state.json", map[string]any{"version": 1})
	writeJSON("reports/auth-cleaner/report-1.json", map[string]any{"summary": map[string]any{"available": 33}})
	writeJSON("backups/auth-cleaner/20260414/backup.json", map[string]any{"type": "codex", "email": "backup@example.com"})
	writeJSON("sub2.json", map[string]any{"accounts": []any{map[string]any{"name": "x"}}})

	entries, err := store.List(context.Background())
	if err != nil {
		t.Fatalf("List() error: %v", err)
	}
	if len(entries) != 1 {
		t.Fatalf("expected 1 auth entry, got %d", len(entries))
	}
	if entries[0].FileName != "real.json" {
		t.Fatalf("expected real.json, got %s", entries[0].FileName)
	}
}
