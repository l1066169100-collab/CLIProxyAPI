package managementui

import (
	"strings"
	"testing"
)

func TestInjectAuthCleanerUIUsesVersionedScriptURL(t *testing.T) {
	html := []byte("<html><body><h1>ok</h1></body></html>")
	out := string(InjectAuthCleanerUI(html))

	if !strings.Contains(out, AuthCleanerUIScriptPath+"?v=") {
		t.Fatalf("expected versioned script url in injected html, got: %s", out)
	}
	if strings.Count(out, AuthCleanerUIScriptPath) != 1 {
		t.Fatalf("expected a single auth cleaner script injection, got: %s", out)
	}
}
