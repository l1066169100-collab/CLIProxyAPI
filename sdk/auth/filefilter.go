package auth

import (
	"io/fs"
	"path/filepath"
	"strings"
)

const authCleanerStateFileName = "auth-cleaner-state.json"

func shouldSkipAuthDir(baseDir, path string) bool {
	rel := normalizeAuthRelativePath(baseDir, path)
	switch rel {
	case "reports/auth-cleaner", "backups/auth-cleaner":
		return true
	default:
		return false
	}
}

// ShouldIgnoreAuthPath returns true when a path under auth-dir is a known
// auth-cleaner artifact rather than a real auth file.
func ShouldIgnoreAuthPath(baseDir, path string) bool {
	rel := normalizeAuthRelativePath(baseDir, path)
	switch {
	case rel == authCleanerStateFileName:
		return true
	case strings.HasPrefix(rel, "reports/auth-cleaner/"):
		return true
	case strings.HasPrefix(rel, "backups/auth-cleaner/"):
		return true
	default:
		return false
	}
}

// ShouldSkipAuthWalkEntry decides whether a WalkDir entry should be ignored.
// For matching directories the caller should stop descending into that subtree.
func ShouldSkipAuthWalkEntry(baseDir, path string, d fs.DirEntry) bool {
	if d == nil {
		return false
	}
	if d.IsDir() {
		return shouldSkipAuthDir(baseDir, path)
	}
	return ShouldIgnoreAuthPath(baseDir, path)
}

// LooksLikeAuthMetadata keeps file-based auth scanning aligned with the
// synthesizer by requiring a top-level provider type.
func LooksLikeAuthMetadata(metadata map[string]any) bool {
	if len(metadata) == 0 {
		return false
	}
	value, _ := metadata["type"].(string)
	return strings.TrimSpace(value) != ""
}

func normalizeAuthRelativePath(baseDir, path string) string {
	base := strings.TrimSpace(baseDir)
	target := strings.TrimSpace(path)
	if target == "" {
		return ""
	}
	if base != "" {
		if rel, err := filepath.Rel(base, target); err == nil && rel != "" {
			target = rel
		}
	}
	target = filepath.Clean(filepath.ToSlash(target))
	target = strings.TrimPrefix(target, "./")
	target = strings.TrimPrefix(target, "/")
	if target == "." {
		return ""
	}
	return strings.ToLower(target)
}
