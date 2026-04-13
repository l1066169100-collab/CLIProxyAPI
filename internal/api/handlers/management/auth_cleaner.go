package management

import (
	"errors"
	"fmt"
	"net/http"
	"os"
	"strconv"
	"strings"

	"github.com/gin-gonic/gin"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/authcleaner"
)

func (h *Handler) cleanerService() *authcleaner.Service {
	if h == nil {
		return nil
	}
	return h.cleaner
}

func (h *Handler) GetAuthCleanerStatus(c *gin.Context) {
	svc := h.cleanerService()
	if svc == nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "auth cleaner unavailable"})
		return
	}
	c.JSON(http.StatusOK, svc.Status())
}

func (h *Handler) GetAuthCleanerConfig(c *gin.Context) {
	svc := h.cleanerService()
	if svc == nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "auth cleaner unavailable"})
		return
	}
	c.JSON(http.StatusOK, gin.H{"auth-cleaner": svc.EffectiveConfig()})
}

func (h *Handler) RunAuthCleaner(c *gin.Context) {
	svc := h.cleanerService()
	if svc == nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "auth cleaner unavailable"})
		return
	}
	dryRun := parseDryRunFlag(c)
	report, err := svc.RunNow(c.Request.Context(), dryRun)
	if err != nil {
		switch {
		case errors.Is(err, authcleaner.ErrAlreadyRunning):
			c.JSON(http.StatusConflict, gin.H{"error": "auth cleaner already running"})
		case strings.Contains(strings.ToLower(err.Error()), "disabled"):
			c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		default:
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		}
		return
	}
	c.JSON(http.StatusOK, gin.H{
		"status":       "ok",
		"run_id":       report.RunID,
		"generated_at": report.GeneratedAt,
		"dry_run":      report.DryRun,
		"report_path":  report.ReportPath,
		"summary":      report.Summary,
	})
}

func (h *Handler) GetAuthCleanerState(c *gin.Context) {
	svc := h.cleanerService()
	if svc == nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "auth cleaner unavailable"})
		return
	}
	data, err := svc.LoadState()
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.Data(http.StatusOK, "application/json; charset=utf-8", data)
}

func (h *Handler) ListAuthCleanerReports(c *gin.Context) {
	svc := h.cleanerService()
	if svc == nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "auth cleaner unavailable"})
		return
	}
	limit := 20
	if raw := strings.TrimSpace(c.Query("limit")); raw != "" {
		if parsed, err := strconv.Atoi(raw); err == nil && parsed > 0 {
			if parsed > 200 {
				parsed = 200
			}
			limit = parsed
		}
	}
	reports, err := svc.ListReports(limit)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"reports": reports})
}

func (h *Handler) GetAuthCleanerReport(c *gin.Context) {
	svc := h.cleanerService()
	if svc == nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "auth cleaner unavailable"})
		return
	}
	name := strings.TrimSpace(c.Param("name"))
	data, err := svc.LoadReport(name)
	if err != nil {
		if os.IsNotExist(err) {
			c.JSON(http.StatusNotFound, gin.H{"error": fmt.Sprintf("report not found: %s", name)})
			return
		}
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.Data(http.StatusOK, "application/json; charset=utf-8", data)
}

func parseDryRunFlag(c *gin.Context) bool {
	if c == nil {
		return false
	}
	if raw := strings.TrimSpace(c.Query("dry_run")); raw != "" {
		switch strings.ToLower(raw) {
		case "1", "true", "yes", "on":
			return true
		case "0", "false", "no", "off":
			return false
		}
	}
	var body struct {
		DryRun *bool `json:"dry_run"`
	}
	if c.Request != nil && c.Request.Body != nil {
		_ = c.ShouldBindJSON(&body)
	}
	if body.DryRun != nil {
		return *body.DryRun
	}
	return false
}
