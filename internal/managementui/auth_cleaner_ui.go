package managementui

import (
	"crypto/sha256"
	_ "embed"
	"encoding/hex"
	"strings"
)

const (
	AuthCleanerUIScriptPath = "/management-auth-cleaner.js"
	authCleanerUIMarker     = "data-cpa-auth-cleaner-ui"
)

//go:embed auth_cleaner_ui.js
var authCleanerUIScript string
var authCleanerUIScriptURL string

func AuthCleanerUIScript() []byte {
	return []byte(authCleanerUIScript)
}

func AuthCleanerUIScriptURL() string {
	if authCleanerUIScriptURL != "" {
		return authCleanerUIScriptURL
	}
	sum := sha256.Sum256([]byte(authCleanerUIScript))
	authCleanerUIScriptURL = AuthCleanerUIScriptPath + "?v=" + hex.EncodeToString(sum[:6])
	return authCleanerUIScriptURL
}

func InjectAuthCleanerUI(html []byte) []byte {
	if len(html) == 0 {
		return html
	}

	content := string(html)
	if strings.Contains(content, authCleanerUIMarker) || strings.Contains(content, AuthCleanerUIScriptPath) {
		return html
	}

	snippet := "\n<script " + authCleanerUIMarker + ` src="` + AuthCleanerUIScriptURL() + `"></script>` + "\n"
	lower := strings.ToLower(content)
	if idx := strings.LastIndex(lower, "</body>"); idx >= 0 {
		return []byte(content[:idx] + snippet + content[idx:])
	}
	return []byte(content + snippet)
}
