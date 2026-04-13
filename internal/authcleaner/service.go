package authcleaner

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	sdkauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/auth"
	coreauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
	log "github.com/sirupsen/logrus"
)

var ErrAlreadyRunning = errors.New("auth cleaner already running")

const (
	defaultIntervalSeconds           = 60
	defaultTimeoutSeconds            = 20
	defaultAPICallURL                = "https://chatgpt.com/backend-api/wham/usage"
	defaultAPICallMethod             = "GET"
	defaultAPICallUserAgent          = "Mozilla/5.0 CLIProxyAPI auth-cleaner/1.0"
	defaultAPICallProviders          = "codex,openai,chatgpt"
	defaultAPICallMaxPerRun          = 9
	defaultAPICallSleepMinSeconds    = 5
	defaultAPICallSleepMaxSeconds    = 10
	defaultRevivalWaitDays           = 7
	defaultRevivalProbeIntervalHours = 12
	defaultRetentionKeepReports      = 200
	defaultRetentionReportMaxAgeDays = 7
	defaultRetentionBackupMaxAgeDays = 14
	idleDisabledPoll                 = 1 * time.Minute
)

type Service struct {
	cfgMu          sync.RWMutex
	cfg            *config.Config
	configFilePath string
	authManager    *coreauth.Manager
	tokenStore     coreauth.Store
	wakeCh         chan struct{}
	cancel         context.CancelFunc
	started        atomic.Bool
	running        atomic.Bool
	statusMu       sync.RWMutex
	status         Status
}

type Status struct {
	Enabled         bool            `json:"enabled"`
	Running         bool            `json:"running"`
	IntervalSeconds int             `json:"interval_seconds"`
	LastStartedAt   time.Time       `json:"last_started_at,omitempty"`
	LastFinishedAt  time.Time       `json:"last_finished_at,omitempty"`
	NextRunAt       time.Time       `json:"next_run_at,omitempty"`
	LastError       string          `json:"last_error,omitempty"`
	LastReportPath  string          `json:"last_report_path,omitempty"`
	LastDryRun      bool            `json:"last_dry_run"`
	LastSummary     *RunSummary     `json:"last_summary,omitempty"`
	Config          EffectiveConfig `json:"config"`
}

type EffectiveConfig struct {
	Enabled                   bool    `json:"enabled"`
	IntervalSeconds           int     `json:"interval_seconds"`
	TimeoutSeconds            int     `json:"timeout_seconds"`
	EnableAPICallCheck        bool    `json:"enable_api_call_check"`
	APICallURL                string  `json:"api_call_url"`
	APICallMethod             string  `json:"api_call_method"`
	APICallAccountID          string  `json:"api_call_account_id,omitempty"`
	APICallUserAgent          string  `json:"api_call_user_agent"`
	APICallBody               string  `json:"api_call_body,omitempty"`
	APICallProviders          string  `json:"api_call_providers"`
	APICallMaxPerRun          int     `json:"api_call_max_per_run"`
	APICallSleepMinSeconds    float64 `json:"api_call_sleep_min_seconds"`
	APICallSleepMaxSeconds    float64 `json:"api_call_sleep_max_seconds"`
	RevivalWaitDays           int     `json:"revival_wait_days"`
	RevivalProbeIntervalHours int     `json:"revival_probe_interval_hours"`
	StateFile                 string  `json:"state_file"`
	ReportDir                 string  `json:"report_dir"`
	BackupDir                 string  `json:"backup_dir"`
	RetentionKeepReports      int     `json:"retention_keep_reports"`
	RetentionReportMaxAgeDays int     `json:"retention_report_max_age_days"`
	RetentionBackupMaxAgeDays int     `json:"retention_backup_max_age_days"`
}

type cleanerOptions struct {
	EffectiveConfig
	ProxyURL string
}

type RunReportMeta struct {
	Name        string      `json:"name"`
	RunID       string      `json:"run_id,omitempty"`
	DryRun      bool        `json:"dry_run"`
	GeneratedAt time.Time   `json:"generated_at,omitempty"`
	ModTime     time.Time   `json:"mod_time"`
	Size        int64       `json:"size"`
	Summary     *RunSummary `json:"summary,omitempty"`
}

func NewService(cfg *config.Config, configFilePath string, manager *coreauth.Manager, tokenStore coreauth.Store) *Service {
	if tokenStore == nil {
		tokenStore = sdkauth.GetTokenStore()
	}
	return &Service{
		cfg:            cfg,
		configFilePath: strings.TrimSpace(configFilePath),
		authManager:    manager,
		tokenStore:     tokenStore,
		wakeCh:         make(chan struct{}, 1),
	}
}

func (s *Service) Start(parent context.Context) {
	if s == nil {
		return
	}
	if parent == nil {
		parent = context.Background()
	}
	if !s.started.CompareAndSwap(false, true) {
		return
	}
	ctx, cancel := context.WithCancel(parent)
	s.cancel = cancel
	s.wake()
	go s.loop(ctx)
}

func (s *Service) Stop() {
	if s == nil {
		return
	}
	if cancel := s.cancel; cancel != nil {
		cancel()
	}
}

func (s *Service) SetConfig(cfg *config.Config, configFilePath string) {
	if s == nil {
		return
	}
	s.cfgMu.Lock()
	s.cfg = cfg
	if trimmed := strings.TrimSpace(configFilePath); trimmed != "" {
		s.configFilePath = trimmed
	}
	s.cfgMu.Unlock()
	s.wake()
}

func (s *Service) SetAuthManager(manager *coreauth.Manager) {
	if s == nil {
		return
	}
	s.cfgMu.Lock()
	s.authManager = manager
	s.cfgMu.Unlock()
	s.wake()
}

func (s *Service) SetTokenStore(store coreauth.Store) {
	if s == nil || store == nil {
		return
	}
	s.cfgMu.Lock()
	s.tokenStore = store
	s.cfgMu.Unlock()
}

func (s *Service) EffectiveConfig() EffectiveConfig {
	if s == nil {
		return EffectiveConfig{}
	}
	return s.snapshotOptions().EffectiveConfig
}

func (s *Service) Status() Status {
	if s == nil {
		return Status{}
	}
	s.statusMu.RLock()
	status := s.status
	s.statusMu.RUnlock()
	status.Running = s.running.Load()
	status.Config = s.snapshotOptions().EffectiveConfig
	status.Enabled = status.Config.Enabled
	status.IntervalSeconds = status.Config.IntervalSeconds
	if status.LastSummary != nil {
		copySummary := *status.LastSummary
		status.LastSummary = &copySummary
	}
	return status
}

func (s *Service) RunNow(ctx context.Context, dryRun bool) (*RunReport, error) {
	if s == nil {
		return nil, fmt.Errorf("auth cleaner unavailable")
	}
	if !s.running.CompareAndSwap(false, true) {
		return nil, ErrAlreadyRunning
	}
	defer s.running.Store(false)
	if ctx == nil {
		ctx = context.Background()
	}

	opts := s.snapshotOptions()
	if !opts.Enabled && !dryRun {
		return nil, fmt.Errorf("auth cleaner disabled")
	}
	if s.snapshotManager() == nil {
		return nil, fmt.Errorf("core auth manager unavailable")
	}

	startedAt := time.Now().UTC()
	s.statusMu.Lock()
	s.status.LastStartedAt = startedAt
	s.status.LastDryRun = dryRun
	s.status.LastError = ""
	s.status.Enabled = opts.Enabled
	s.status.IntervalSeconds = opts.IntervalSeconds
	s.statusMu.Unlock()

	report, err := s.run(ctx, opts, dryRun)
	finishedAt := time.Now().UTC()

	s.statusMu.Lock()
	s.status.LastFinishedAt = finishedAt
	if report != nil {
		s.status.LastReportPath = report.ReportPath
		s.status.LastSummary = report.Summary.Clone()
	}
	if err != nil {
		s.status.LastError = err.Error()
	}
	s.statusMu.Unlock()

	if err != nil {
		return report, err
	}
	return report, nil
}

func (s *Service) ListReports(limit int) ([]RunReportMeta, error) {
	if s == nil {
		return nil, fmt.Errorf("auth cleaner unavailable")
	}
	opts := s.snapshotOptions()
	entries, err := os.ReadDir(opts.ReportDir)
	if err != nil {
		if os.IsNotExist(err) {
			return []RunReportMeta{}, nil
		}
		return nil, err
	}
	items := make([]RunReportMeta, 0)
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		name := entry.Name()
		if !strings.HasPrefix(name, "report-") || !strings.HasSuffix(strings.ToLower(name), ".json") {
			continue
		}
		full := filepath.Join(opts.ReportDir, name)
		info, errInfo := entry.Info()
		if errInfo != nil {
			continue
		}
		meta := RunReportMeta{Name: name, ModTime: info.ModTime().UTC(), Size: info.Size()}
		if data, errRead := os.ReadFile(full); errRead == nil {
			var payload struct {
				RunID       string     `json:"run_id"`
				DryRun      bool       `json:"dry_run"`
				GeneratedAt time.Time  `json:"generated_at"`
				Summary     RunSummary `json:"summary"`
			}
			if errUnmarshal := json.Unmarshal(data, &payload); errUnmarshal == nil {
				meta.RunID = payload.RunID
				meta.DryRun = payload.DryRun
				meta.GeneratedAt = payload.GeneratedAt
				summary := payload.Summary
				meta.Summary = &summary
			}
		}
		items = append(items, meta)
	}
	sort.Slice(items, func(i, j int) bool {
		if items[i].ModTime.Equal(items[j].ModTime) {
			return items[i].Name > items[j].Name
		}
		return items[i].ModTime.After(items[j].ModTime)
	})
	if limit > 0 && len(items) > limit {
		items = items[:limit]
	}
	return items, nil
}

func (s *Service) LoadReport(name string) ([]byte, error) {
	if s == nil {
		return nil, fmt.Errorf("auth cleaner unavailable")
	}
	name = sanitizeFileName(name)
	if name == "" {
		return nil, fmt.Errorf("invalid report name")
	}
	opts := s.snapshotOptions()
	return os.ReadFile(filepath.Join(opts.ReportDir, name))
}

func (s *Service) LoadState() ([]byte, error) {
	if s == nil {
		return nil, fmt.Errorf("auth cleaner unavailable")
	}
	opts := s.snapshotOptions()
	data, err := os.ReadFile(opts.StateFile)
	if err == nil {
		return data, nil
	}
	if os.IsNotExist(err) {
		state := defaultState()
		return json.MarshalIndent(state, "", "  ")
	}
	return nil, err
}

func (s *Service) ForceRevivalNow(names []string) (int, error) {
	if s == nil {
		return 0, fmt.Errorf("auth cleaner unavailable")
	}

	opts := s.snapshotOptions()
	state, err := loadStateFile(opts.StateFile)
	if err != nil {
		return 0, err
	}

	selected := make(map[string]struct{})
	for _, name := range names {
		trimmed := strings.TrimSpace(name)
		if trimmed == "" {
			continue
		}
		selected[trimmed] = struct{}{}
	}

	now := formatISOTime(time.Now().UTC())
	forced := 0
	for name, entry := range state.QuotaAccounts {
		if entry == nil {
			continue
		}
		if len(selected) > 0 {
			if _, ok := selected[name]; !ok {
				continue
			}
		}
		entry.NextRevivalCheckAt = now
		forced++
	}

	if forced == 0 {
		return 0, nil
	}
	if err := saveStateFile(opts.StateFile, state); err != nil {
		return 0, err
	}
	s.wake()
	return forced, nil
}

func (s *Service) loop(ctx context.Context) {
	nextDelay := time.Duration(0)
	timer := time.NewTimer(0)
	defer timer.Stop()
	for {
		opts := s.snapshotOptions()
		if !opts.Enabled || s.snapshotManager() == nil {
			nextDelay = idleDisabledPoll
		} else if nextDelay < 0 {
			nextDelay = 0
		}
		nextRun := time.Time{}
		if !nextDelayIsInfinite(nextDelay) {
			nextRun = time.Now().UTC().Add(nextDelay)
		}
		s.statusMu.Lock()
		s.status.Enabled = opts.Enabled
		s.status.IntervalSeconds = opts.IntervalSeconds
		s.status.NextRunAt = nextRun
		s.statusMu.Unlock()

		resetTimer(timer, nextDelay)
		select {
		case <-ctx.Done():
			return
		case <-s.wakeCh:
			nextDelay = 0
			continue
		case <-timer.C:
		}

		opts = s.snapshotOptions()
		if !opts.Enabled || s.snapshotManager() == nil {
			nextDelay = idleDisabledPoll
			continue
		}
		if _, err := s.RunNow(ctx, false); err != nil && !errors.Is(err, ErrAlreadyRunning) {
			log.WithError(err).Warn("auth cleaner run failed")
		}
		nextDelay = time.Duration(opts.IntervalSeconds) * time.Second
	}
}

func resetTimer(timer *time.Timer, d time.Duration) {
	if timer == nil {
		return
	}
	if d < 0 {
		d = 0
	}
	if !timer.Stop() {
		select {
		case <-timer.C:
		default:
		}
	}
	timer.Reset(d)
}

func nextDelayIsInfinite(d time.Duration) bool {
	return d <= 0
}

func (s *Service) snapshotManager() *coreauth.Manager {
	s.cfgMu.RLock()
	defer s.cfgMu.RUnlock()
	return s.authManager
}

func (s *Service) snapshotOptions() cleanerOptions {
	s.cfgMu.RLock()
	defer s.cfgMu.RUnlock()
	return normalizeOptions(s.cfg, s.configFilePath)
}

func (s *Service) snapshotTokenStore() coreauth.Store {
	s.cfgMu.RLock()
	defer s.cfgMu.RUnlock()
	if s.tokenStore != nil {
		return s.tokenStore
	}
	return sdkauth.GetTokenStore()
}

func (s *Service) wake() {
	if s == nil {
		return
	}
	select {
	case s.wakeCh <- struct{}{}:
	default:
	}
}

func normalizeOptions(cfg *config.Config, configFilePath string) cleanerOptions {
	var raw config.AuthCleanerConfig
	proxyURL := ""
	authDir := ""
	if cfg != nil {
		raw = cfg.AuthCleaner
		proxyURL = strings.TrimSpace(cfg.ProxyURL)
		authDir = strings.TrimSpace(cfg.AuthDir)
	}
	baseDir := defaultBaseDir(configFilePath, authDir)
	stateFile := strings.TrimSpace(raw.StateFile)
	if stateFile == "" {
		stateFile = filepath.Join(baseDir, "auth-cleaner-state.json")
	}
	reportDir := strings.TrimSpace(raw.ReportDir)
	if reportDir == "" {
		reportDir = filepath.Join(baseDir, "reports", "auth-cleaner")
	}
	backupDir := strings.TrimSpace(raw.BackupDir)
	if backupDir == "" {
		backupDir = filepath.Join(baseDir, "backups", "auth-cleaner")
	}
	intervalSeconds := raw.IntervalSeconds
	if intervalSeconds <= 0 {
		intervalSeconds = defaultIntervalSeconds
	}
	timeoutSeconds := raw.TimeoutSeconds
	if timeoutSeconds <= 0 {
		timeoutSeconds = defaultTimeoutSeconds
	}
	apiCallURL := strings.TrimSpace(raw.APICallURL)
	if apiCallURL == "" {
		apiCallURL = defaultAPICallURL
	}
	apiCallMethod := strings.ToUpper(strings.TrimSpace(raw.APICallMethod))
	if apiCallMethod == "" {
		apiCallMethod = defaultAPICallMethod
	}
	apiCallUserAgent := strings.TrimSpace(raw.APICallUserAgent)
	if apiCallUserAgent == "" {
		apiCallUserAgent = defaultAPICallUserAgent
	}
	apiCallProviders := strings.TrimSpace(raw.APICallProviders)
	if apiCallProviders == "" {
		apiCallProviders = defaultAPICallProviders
	}
	apiCallMaxPerRun := raw.APICallMaxPerRun
	if apiCallMaxPerRun <= 0 {
		apiCallMaxPerRun = defaultAPICallMaxPerRun
	}
	if apiCallMaxPerRun > defaultAPICallMaxPerRun {
		apiCallMaxPerRun = defaultAPICallMaxPerRun
	}
	apiCallSleepMin := raw.APICallSleepMinSeconds
	if apiCallSleepMin < 0 {
		apiCallSleepMin = 0
	}
	if apiCallSleepMin == 0 {
		apiCallSleepMin = defaultAPICallSleepMinSeconds
	}
	apiCallSleepMax := raw.APICallSleepMaxSeconds
	if apiCallSleepMax < apiCallSleepMin {
		apiCallSleepMax = apiCallSleepMin
	}
	if apiCallSleepMax == 0 {
		apiCallSleepMax = defaultAPICallSleepMaxSeconds
		if apiCallSleepMax < apiCallSleepMin {
			apiCallSleepMax = apiCallSleepMin
		}
	}
	revivalWaitDays := raw.RevivalWaitDays
	if revivalWaitDays < 0 {
		revivalWaitDays = 0
	}
	if revivalWaitDays == 0 {
		revivalWaitDays = defaultRevivalWaitDays
	}
	revivalProbeHours := raw.RevivalProbeIntervalHours
	if revivalProbeHours <= 0 {
		revivalProbeHours = defaultRevivalProbeIntervalHours
	}
	keepReports := raw.RetentionKeepReports
	if keepReports <= 0 {
		keepReports = defaultRetentionKeepReports
	}
	reportMaxAgeDays := raw.RetentionReportMaxAgeDays
	if reportMaxAgeDays < 0 {
		reportMaxAgeDays = 0
	}
	if reportMaxAgeDays == 0 {
		reportMaxAgeDays = defaultRetentionReportMaxAgeDays
	}
	backupMaxAgeDays := raw.RetentionBackupMaxAgeDays
	if backupMaxAgeDays < 0 {
		backupMaxAgeDays = 0
	}
	if backupMaxAgeDays == 0 {
		backupMaxAgeDays = defaultRetentionBackupMaxAgeDays
	}
	return cleanerOptions{
		EffectiveConfig: EffectiveConfig{
			Enabled:                   raw.Enable,
			IntervalSeconds:           intervalSeconds,
			TimeoutSeconds:            timeoutSeconds,
			EnableAPICallCheck:        raw.EnableAPICallCheck,
			APICallURL:                apiCallURL,
			APICallMethod:             apiCallMethod,
			APICallAccountID:          strings.TrimSpace(raw.APICallAccountID),
			APICallUserAgent:          apiCallUserAgent,
			APICallBody:               raw.APICallBody,
			APICallProviders:          apiCallProviders,
			APICallMaxPerRun:          apiCallMaxPerRun,
			APICallSleepMinSeconds:    apiCallSleepMin,
			APICallSleepMaxSeconds:    apiCallSleepMax,
			RevivalWaitDays:           revivalWaitDays,
			RevivalProbeIntervalHours: revivalProbeHours,
			StateFile:                 filepath.Clean(stateFile),
			ReportDir:                 filepath.Clean(reportDir),
			BackupDir:                 filepath.Clean(backupDir),
			RetentionKeepReports:      keepReports,
			RetentionReportMaxAgeDays: reportMaxAgeDays,
			RetentionBackupMaxAgeDays: backupMaxAgeDays,
		},
		ProxyURL: proxyURL,
	}
}

func defaultBaseDir(configFilePath, authDir string) string {
	if trimmed := strings.TrimSpace(configFilePath); trimmed != "" {
		if abs, err := filepath.Abs(trimmed); err == nil {
			return filepath.Dir(abs)
		}
		return filepath.Dir(trimmed)
	}
	if trimmed := strings.TrimSpace(authDir); trimmed != "" {
		if abs, err := filepath.Abs(trimmed); err == nil {
			return filepath.Dir(abs)
		}
		return filepath.Dir(trimmed)
	}
	wd, err := os.Getwd()
	if err != nil {
		return "."
	}
	return wd
}

func sanitizeFileName(name string) string {
	name = filepath.Base(strings.TrimSpace(name))
	if name == "." || name == string(filepath.Separator) || name == "" {
		return ""
	}
	if strings.ContainsAny(name, `/\\`) {
		return ""
	}
	return name
}
