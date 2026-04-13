package authcleaner

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	coreauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
	"github.com/router-for-me/CLIProxyAPI/v6/sdk/proxyutil"
	log "github.com/sirupsen/logrus"
)

var (
	pattern401    = regexp.MustCompile(`(?i)(^|\D)401(\D|$)|unauthorized|unauthenticated|token\s+expired|login\s+required|authentication\s+failed|invalid_grant|refresh_token_reused|invalid\s+refresh`)
	patternQuota  = regexp.MustCompile(`(?i)(^|\D)(402|403|429)(\D|$)|quota|insufficient\s*quota|resource\s*exhausted|rate\s*limit|too\s+many\s+requests|payment\s+required|billing|credit|额度|用完|超限|上限|usage_limit_reached`)
	patternRemove = regexp.MustCompile(`(?i)invalid_grant|refresh_token_reused|account.*deactivated|account.*disabled|missing\s+refresh\s+token|invalid\s+token`)
)

const (
	openAITokenURL = "https://auth.openai.com/oauth/token"
	openAIClientID = "app_EMoamEEZ73f0CkXaXp7hrann"
	openAIScope    = "openid profile email"
)

type RunReport struct {
	RunID       string           `json:"run_id"`
	GeneratedAt time.Time        `json:"generated_at"`
	DryRun      bool             `json:"dry_run"`
	Config      EffectiveConfig  `json:"config"`
	Results     []map[string]any `json:"results"`
	Summary     *RunSummary      `json:"summary"`
	State       *stateFile       `json:"state"`
	ReportPath  string           `json:"report_path,omitempty"`
}

type RunSummary struct {
	CheckedTotal      int `json:"checked_total"`
	Available         int `json:"available"`
	QuotaExhausted    int `json:"quota_exhausted"`
	Disabled          int `json:"disabled"`
	Unavailable       int `json:"unavailable"`
	APICallCandidates int `json:"api_call_candidates"`
	APICallBatches    int `json:"api_call_batches"`
	APICallProbed     int `json:"api_call_probed"`
	APICallFound401   int `json:"api_call_found_401"`
	APICallFoundQuota int `json:"api_call_found_quota"`
	APICallFailures   int `json:"api_call_failures"`
	PendingDelete401  int `json:"pending_delete_401"`
	Deleted           int `json:"deleted"`
	BackupFailures    int `json:"backup_failures"`
	DeleteFailures    int `json:"delete_failures"`
	QuotaDisabled     int `json:"quota_disabled"`
	DisableFailures   int `json:"disable_failures"`
	RevivalPending    int `json:"revival_pending"`
	RefreshAttempts   int `json:"refresh_attempts"`
	RefreshSucceeded  int `json:"refresh_succeeded"`
	RefreshFailed     int `json:"refresh_failed"`
	RevivalEnabled    int `json:"revival_enabled"`
	RevivalStillQuota int `json:"revival_still_quota"`
	RevivalDeleted    int `json:"revival_deleted"`
	StateCleared      int `json:"state_cleared"`
}

func (s *RunSummary) Clone() *RunSummary {
	if s == nil {
		return nil
	}
	copySummary := *s
	return &copySummary
}

type stateFile struct {
	Version       int                         `json:"version"`
	QuotaAccounts map[string]*quotaStateEntry `json:"quota_accounts"`
}

type quotaStateEntry struct {
	Provider           string `json:"provider,omitempty"`
	Reason             string `json:"reason,omitempty"`
	Path               string `json:"path,omitempty"`
	AuthIndex          string `json:"auth_index,omitempty"`
	Email              string `json:"email,omitempty"`
	AccountID          string `json:"account_id,omitempty"`
	QuotaDisabledAt    string `json:"quota_disabled_at,omitempty"`
	NextRevivalCheckAt string `json:"next_revival_check_at,omitempty"`
	LastSeenAt         string `json:"last_seen_at,omitempty"`
	LastRevivalCheckAt string `json:"last_revival_check_at,omitempty"`
	LastError          string `json:"last_error,omitempty"`
}

type authItem struct {
	ID            string
	AuthIndex     string
	Name          string
	Provider      string
	Status        string
	StatusMessage string
	Disabled      bool
	Unavailable   bool
	RuntimeOnly   bool
	Source        string
	Path          string
	Email         string
	AccountID     string
}

type apiProbeResult struct {
	Request        map[string]any     `json:"request,omitempty"`
	Response       *httpProbeResponse `json:"response,omitempty"`
	Classification string             `json:"classification,omitempty"`
	Reason         string             `json:"reason,omitempty"`
	StatusCode     int                `json:"status_code,omitempty"`
	Error          string             `json:"error,omitempty"`
}

type httpProbeResponse struct {
	StatusCode int                 `json:"status_code"`
	Header     map[string][]string `json:"header,omitempty"`
	Body       any                 `json:"body,omitempty"`
}

func init() {
	rand.Seed(time.Now().UnixNano())
}

func (s *Service) run(ctx context.Context, opts cleanerOptions, dryRun bool) (*RunReport, error) {
	manager := s.snapshotManager()
	if manager == nil {
		return nil, fmt.Errorf("core auth manager unavailable")
	}
	store := s.snapshotTokenStore()
	if dirSetter, ok := store.(interface{ SetBaseDir(string) }); ok && strings.TrimSpace(opts.StateFile) != "" {
		// Keep token store pointed at auth-dir defaults if caller provided absolute paths elsewhere.
		// No-op when the store does not support base-dir updates.
		_ = dirSetter
	}
	state, err := loadStateFile(opts.StateFile)
	if err != nil {
		return nil, err
	}
	items := collectAuthItems(manager.List())
	runID := time.Now().UTC().Format("20060102T150405Z")
	backupRoot := filepath.Join(opts.BackupDir, runID)
	summary := &RunSummary{}
	results := make([]map[string]any, 0, len(items))
	nameToItem := make(map[string]authItem, len(items))
	providerSet := parseProviderSet(opts.APICallProviders)

	probeResults := s.runAPICallFullScan(ctx, opts, items, providerSet, summary)

	for _, item := range items {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		summary.CheckedTotal++
		nameToItem[item.Name] = item
		kind, reason := classifyItem(item)
		row := map[string]any{
			"id":             item.ID,
			"name":           item.Name,
			"provider":       item.Provider,
			"auth_index":     item.AuthIndex,
			"status":         item.Status,
			"status_message": item.StatusMessage,
			"disabled":       item.Disabled,
			"unavailable":    item.Unavailable,
			"runtime_only":   item.RuntimeOnly,
			"source":         item.Source,
			"path":           item.Path,
		}
		if item.Email != "" {
			row["email"] = item.Email
		}
		if item.AccountID != "" {
			row["account_id"] = item.AccountID
		}
		if probe, ok := probeResults[itemKey(item)]; ok {
			row["api_call_probe"] = probe
			if probe.Classification == "delete_401" {
				kind = "delete_401"
				reason = probe.Reason
			} else if probe.Classification == "quota_exhausted" {
				kind = "quota_exhausted"
				reason = probe.Reason
			}
		}
		row["final_classification"] = kind
		row["reason"] = reason

		switch kind {
		case "available":
			summary.Available++
			if item.Name != "" && !item.Disabled && !dryRun && clearQuotaState(state, item.Name) {
				summary.StateCleared++
				row["state_cleared"] = "became_available"
			}
		case "quota_exhausted":
			summary.QuotaExhausted++
			if dryRun {
				row["disable_result"] = "dry_run_skip"
			} else {
				if errDisable := s.disableAuth(ctx, item, "disabled by auth cleaner"); errDisable != nil {
					summary.DisableFailures++
					row["disable_result"] = "disable_failed"
					row["disable_error"] = errDisable.Error()
				} else {
					summary.QuotaDisabled++
					row["disable_result"] = "disabled_true"
					stateEntry := ensureQuotaState(state, item, reason, opts)
					row["revival_tracking"] = stateEntry
				}
			}
		case "disabled":
			summary.Disabled++
		case "unavailable":
			summary.Unavailable++
		case "delete_401":
			summary.PendingDelete401++
			if dryRun {
				row["delete_result"] = "dry_run_skip"
			} else if item.RuntimeOnly || item.Source != "file" || item.Path == "" {
				summary.BackupFailures++
				row["delete_result"] = "skip_runtime_only"
				row["delete_error"] = "runtime_only/source!=file，无法删除"
			} else if !strings.HasSuffix(strings.ToLower(item.Name), ".json") {
				summary.BackupFailures++
				row["delete_result"] = "skip_no_json_name"
				row["delete_error"] = "不是标准 .json 文件名，默认不删"
			} else {
				backupPath, errDelete := s.deleteAuthFile(ctx, store, item, filepath.Join(backupRoot, "delete-401"))
				if errDelete != nil {
					summary.DeleteFailures++
					row["delete_result"] = "delete_failed"
					row["delete_error"] = errDelete.Error()
				} else {
					summary.Deleted++
					row["delete_result"] = "deleted"
					row["backup_path"] = backupPath
					_ = clearQuotaState(state, item.Name)
				}
			}
		default:
			summary.Unavailable++
		}
		results = append(results, row)
	}

	for name := range state.QuotaAccounts {
		if _, ok := nameToItem[name]; !ok && !dryRun {
			delete(state.QuotaAccounts, name)
			summary.StateCleared++
		}
	}

	if !dryRun {
		now := time.Now().UTC()
		names := make([]string, 0, len(state.QuotaAccounts))
		for name := range state.QuotaAccounts {
			names = append(names, name)
		}
		sort.Strings(names)
		for _, name := range names {
			entry := state.QuotaAccounts[name]
			item, ok := nameToItem[name]
			if !ok || entry == nil {
				continue
			}
			dueAt := parseISOTime(entry.NextRevivalCheckAt)
			if !dueAt.IsZero() && dueAt.After(now) {
				continue
			}
			row := s.runRevivalCycle(ctx, opts, store, item, entry, summary, backupRoot)
			results = append(results, row)
			if result, _ := row["revival_result"].(string); result == "enabled" || strings.HasPrefix(result, "deleted_") {
				delete(state.QuotaAccounts, name)
			}
		}
	}

	if !dryRun {
		if errSave := saveStateFile(opts.StateFile, state); errSave != nil {
			return nil, errSave
		}
	}
	if errRetention := applyRetention(opts); errRetention != nil {
		log.WithError(errRetention).Warn("auth cleaner retention cleanup failed")
	}

	report := &RunReport{
		RunID:       runID,
		GeneratedAt: time.Now().UTC(),
		DryRun:      dryRun,
		Config:      opts.EffectiveConfig,
		Results:     results,
		Summary:     summary,
		State:       state,
	}
	if errWrite := writeReportFile(opts.ReportDir, report); errWrite != nil {
		return report, errWrite
	}
	return report, nil
}

func collectAuthItems(auths []*coreauth.Auth) []authItem {
	items := make([]authItem, 0, len(auths))
	for _, auth := range auths {
		if auth == nil {
			continue
		}
		auth.EnsureIndex()
		runtimeOnly := authRuntimeOnly(auth)
		if runtimeOnly && (auth.Disabled || auth.Status == coreauth.StatusDisabled) {
			continue
		}
		path := authPath(auth)
		if path == "" && !runtimeOnly {
			continue
		}
		source := "memory"
		if path != "" {
			source = "file"
		}
		name := strings.TrimSpace(auth.FileName)
		if name == "" {
			name = strings.TrimSpace(auth.ID)
		}
		item := authItem{
			ID:            strings.TrimSpace(auth.ID),
			AuthIndex:     strings.TrimSpace(auth.Index),
			Name:          name,
			Provider:      strings.ToLower(strings.TrimSpace(auth.Provider)),
			Status:        string(auth.Status),
			StatusMessage: strings.TrimSpace(auth.StatusMessage),
			Disabled:      auth.Disabled,
			Unavailable:   auth.Unavailable,
			RuntimeOnly:   runtimeOnly,
			Source:        source,
			Path:          path,
			Email:         authEmail(auth),
			AccountID:     authAccountID(auth),
		}
		items = append(items, item)
	}
	sort.Slice(items, func(i, j int) bool {
		if items[i].Name == items[j].Name {
			return items[i].ID < items[j].ID
		}
		return items[i].Name < items[j].Name
	})
	return items
}

func authRuntimeOnly(auth *coreauth.Auth) bool {
	if auth == nil || auth.Attributes == nil {
		return false
	}
	return strings.EqualFold(strings.TrimSpace(auth.Attributes["runtime_only"]), "true")
}

func authPath(auth *coreauth.Auth) string {
	if auth == nil || auth.Attributes == nil {
		return ""
	}
	return strings.TrimSpace(auth.Attributes["path"])
}

func authEmail(auth *coreauth.Auth) string {
	if auth == nil {
		return ""
	}
	if auth.Metadata != nil {
		if v, ok := auth.Metadata["email"].(string); ok {
			return strings.TrimSpace(v)
		}
	}
	if auth.Attributes != nil {
		if v := strings.TrimSpace(auth.Attributes["email"]); v != "" {
			return v
		}
		if v := strings.TrimSpace(auth.Attributes["account_email"]); v != "" {
			return v
		}
	}
	return ""
}

func authAccountID(auth *coreauth.Auth) string {
	if auth == nil {
		return ""
	}
	candidates := []any{}
	if auth.Metadata != nil {
		candidates = append(candidates,
			auth.Metadata["account_id"],
			auth.Metadata["accountId"],
		)
	}
	if auth.Attributes != nil {
		candidates = append(candidates,
			auth.Attributes["account_id"],
			auth.Attributes["accountId"],
		)
	}
	for _, candidate := range candidates {
		if v := strings.TrimSpace(stringValueAny(candidate)); v != "" {
			return v
		}
	}
	return ""
}

func itemKey(item authItem) string {
	if item.AuthIndex != "" {
		return "auth_index:" + item.AuthIndex
	}
	return "name:" + item.Name
}

func classifyItem(item authItem) (string, string) {
	status := strings.ToLower(strings.TrimSpace(item.Status))
	msg := strings.TrimSpace(item.StatusMessage)
	errorType, _ := extractErrorMessage(msg)
	text := strings.ToLower(status + "\n" + msg)
	if pattern401.MatchString(text) {
		return "delete_401", firstNonEmpty(msg, status, "401/unauthorized")
	}
	if errorType == "usage_limit_reached" || strings.Contains(text, "usage_limit_reached") {
		return "quota_exhausted", firstNonEmpty(msg, status, "usage_limit_reached")
	}
	if patternQuota.MatchString(text) {
		return "quota_exhausted", firstNonEmpty(msg, status, "quota")
	}
	if item.Disabled || status == "disabled" {
		return "disabled", firstNonEmpty(msg, status, "disabled")
	}
	if item.Unavailable || status == "error" {
		return "unavailable", firstNonEmpty(msg, status, "error")
	}
	return "available", firstNonEmpty(msg, status, "active")
}

func extractErrorMessage(msg string) (string, string) {
	trimmed := strings.TrimSpace(msg)
	if trimmed == "" || !strings.HasPrefix(trimmed, "{") {
		return "", msg
	}
	var payload map[string]any
	if err := json.Unmarshal([]byte(trimmed), &payload); err != nil {
		return "", msg
	}
	errorObj, ok := payload["error"]
	if !ok {
		return "", msg
	}
	switch v := errorObj.(type) {
	case map[string]any:
		return strings.TrimSpace(stringValueAny(v["type"])), strings.TrimSpace(stringValueAny(v["message"]))
	case string:
		return "error", strings.TrimSpace(v)
	default:
		return "", msg
	}
}

func parseProviderSet(raw string) map[string]struct{} {
	out := make(map[string]struct{})
	for _, part := range strings.Split(raw, ",") {
		part = strings.ToLower(strings.TrimSpace(part))
		if part == "" {
			continue
		}
		out[part] = struct{}{}
	}
	return out
}

func shouldProbeAPICall(item authItem, providerSet map[string]struct{}) bool {
	if item.AuthIndex == "" {
		return false
	}
	if len(providerSet) > 0 {
		if _, ok := providerSet[strings.ToLower(item.Provider)]; !ok {
			return false
		}
	}
	kind, _ := classifyItem(item)
	return kind == "available"
}

func (s *Service) runAPICallFullScan(ctx context.Context, opts cleanerOptions, items []authItem, providerSet map[string]struct{}, summary *RunSummary) map[string]apiProbeResult {
	results := make(map[string]apiProbeResult)
	if !opts.EnableAPICallCheck {
		return results
	}
	eligible := make([]authItem, 0)
	for _, item := range items {
		if shouldProbeAPICall(item, providerSet) {
			eligible = append(eligible, item)
		}
	}
	summary.APICallCandidates = len(eligible)
	if len(eligible) == 0 {
		return results
	}
	batchSize := opts.APICallMaxPerRun
	if batchSize <= 0 {
		batchSize = defaultAPICallMaxPerRun
	}
	if batchSize > defaultAPICallMaxPerRun {
		batchSize = defaultAPICallMaxPerRun
	}
	summary.APICallBatches = (len(eligible) + batchSize - 1) / batchSize
	for batchStart := 0; batchStart < len(eligible); batchStart += batchSize {
		if ctx.Err() != nil {
			break
		}
		batchEnd := batchStart + batchSize
		if batchEnd > len(eligible) {
			batchEnd = len(eligible)
		}
		batch := eligible[batchStart:batchEnd]
		var mu sync.Mutex
		var wg sync.WaitGroup
		for _, item := range batch {
			item := item
			wg.Add(1)
			go func() {
				defer wg.Done()
				probe, err := s.runSingleAPICallProbe(ctx, opts, item)
				mu.Lock()
				defer mu.Unlock()
				summary.APICallProbed++
				if err != nil {
					summary.APICallFailures++
					results[itemKey(item)] = apiProbeResult{Error: err.Error()}
					return
				}
				results[itemKey(item)] = *probe
				switch probe.Classification {
				case "delete_401":
					summary.APICallFound401++
				case "quota_exhausted":
					summary.APICallFoundQuota++
				}
			}()
		}
		wg.Wait()
		if batchEnd < len(eligible) {
			sleep := pickProbeSleep(opts)
			if sleep > 0 {
				select {
				case <-ctx.Done():
					return results
				case <-time.After(sleep):
				}
			}
		}
	}
	return results
}

func pickProbeSleep(opts cleanerOptions) time.Duration {
	min := opts.APICallSleepMinSeconds
	max := opts.APICallSleepMaxSeconds
	if max < min {
		max = min
	}
	if min <= 0 && max <= 0 {
		return 0
	}
	if max == min {
		return time.Duration(min * float64(time.Second))
	}
	return time.Duration((min + rand.Float64()*(max-min)) * float64(time.Second))
}

func (s *Service) runSingleAPICallProbe(ctx context.Context, opts cleanerOptions, item authItem) (*apiProbeResult, error) {
	auth := s.lookupAuth(item.ID)
	if auth == nil {
		return nil, fmt.Errorf("auth not found")
	}
	requestPayload := map[string]any{
		"authIndex": item.AuthIndex,
		"method":    opts.APICallMethod,
		"url":       opts.APICallURL,
		"header": map[string]string{
			"Authorization": "Bearer $TOKEN$",
			"Content-Type":  "application/json",
			"User-Agent":    opts.APICallUserAgent,
		},
	}
	if accountID := chooseAccountID(item, auth.Metadata, opts.APICallAccountID); accountID != "" {
		requestPayload["header"].(map[string]string)["Chatgpt-Account-Id"] = accountID
	}
	if strings.TrimSpace(opts.APICallBody) != "" {
		requestPayload["data"] = opts.APICallBody
	}
	resp, err := doAuthedProbe(ctx, opts, auth, requestPayload)
	if err != nil {
		return nil, err
	}
	classification, reason := classifyAPICallResponse(resp)
	return &apiProbeResult{
		Request:        requestPayload,
		Response:       resp,
		Classification: classification,
		Reason:         reason,
		StatusCode:     resp.StatusCode,
	}, nil
}

func doAuthedProbe(ctx context.Context, opts cleanerOptions, auth *coreauth.Auth, requestPayload map[string]any) (*httpProbeResponse, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	method := strings.ToUpper(strings.TrimSpace(stringValueAny(requestPayload["method"])))
	urlStr := strings.TrimSpace(stringValueAny(requestPayload["url"]))
	headers, _ := requestPayload["header"].(map[string]string)
	data := stringValueAny(requestPayload["data"])
	if method == "" || urlStr == "" {
		return nil, fmt.Errorf("invalid probe request")
	}
	token := tokenValueForAuth(auth)
	if token == "" {
		return nil, fmt.Errorf("auth token not found")
	}
	bodyReader := io.Reader(nil)
	if data != "" {
		bodyReader = strings.NewReader(data)
	}
	req, err := http.NewRequestWithContext(ctx, method, urlStr, bodyReader)
	if err != nil {
		return nil, err
	}
	for key, value := range headers {
		req.Header.Set(key, strings.ReplaceAll(value, "$TOKEN$", token))
	}
	if req.Header.Get("Accept") == "" {
		req.Header.Set("Accept", "application/json")
	}
	client := newHTTPClient(auth, opts.ProxyURL, time.Duration(opts.TimeoutSeconds)*time.Second)
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }()
	bodyBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	return &httpProbeResponse{StatusCode: resp.StatusCode, Header: resp.Header, Body: parseBody(bodyBytes)}, nil
}

func classifyAPICallResponse(resp *httpProbeResponse) (string, string) {
	if resp == nil {
		return "", ""
	}
	statusCode := resp.StatusCode
	bodySignal := bodyToString(resp.Body)
	headerText := bodyToString(resp.Header)
	if statusCode == http.StatusUnauthorized {
		return "delete_401", firstNonEmpty(bodySignal, fmt.Sprintf("status_code=%d", statusCode))
	}
	if statusCode == http.StatusPaymentRequired || statusCode == http.StatusForbidden || statusCode == http.StatusTooManyRequests {
		return "quota_exhausted", firstNonEmpty(bodySignal, fmt.Sprintf("status_code=%d", statusCode))
	}
	if bodyMap, ok := resp.Body.(map[string]any); ok {
		if errorObj, okErr := bodyMap["error"].(map[string]any); okErr {
			errorType := strings.ToLower(strings.TrimSpace(stringValueAny(errorObj["type"])))
			errorMessage := strings.TrimSpace(stringValueAny(errorObj["message"]))
			errorText := strings.ToLower(errorType + "\n" + errorMessage)
			if errorType == "usage_limit_reached" || patternQuota.MatchString(errorText) {
				return "quota_exhausted", firstNonEmpty(bodySignal, errorMessage, errorType)
			}
			if pattern401.MatchString(errorText) {
				return "delete_401", firstNonEmpty(bodySignal, errorMessage, errorType)
			}
		}
		if isLimitReachedWindow(bodyMap["rate_limit"]) || isLimitReachedWindow(bodyMap["code_review_rate_limit"]) {
			return "quota_exhausted", firstNonEmpty(bodySignal, "rate_limit_reached")
		}
		if statusCode == http.StatusOK {
			return "", firstNonEmpty(bodySignal, "ok")
		}
	}
	fallback := strings.ToLower(fmt.Sprintf("%d\n%s\n%s", statusCode, headerText, bodySignal))
	if pattern401.MatchString(fallback) {
		return "delete_401", firstNonEmpty(bodySignal, fmt.Sprintf("status_code=%d", statusCode))
	}
	if statusCode != http.StatusOK && patternQuota.MatchString(fallback) {
		return "quota_exhausted", firstNonEmpty(bodySignal, fmt.Sprintf("status_code=%d", statusCode))
	}
	return "", firstNonEmpty(bodySignal, "ok")
}

func isLimitReachedWindow(value any) bool {
	m, ok := value.(map[string]any)
	if !ok {
		return false
	}
	if allowed, okAllowed := m["allowed"].(bool); okAllowed && !allowed {
		return true
	}
	if limitReached, okLimit := m["limit_reached"].(bool); okLimit && limitReached {
		return true
	}
	return false
}

func parseBody(body []byte) any {
	trimmed := bytes.TrimSpace(body)
	if len(trimmed) == 0 {
		return ""
	}
	var payload any
	if err := json.Unmarshal(trimmed, &payload); err == nil {
		return payload
	}
	return string(trimmed)
}

func bodyToString(value any) string {
	switch v := value.(type) {
	case nil:
		return ""
	case string:
		return strings.TrimSpace(v)
	case []byte:
		return strings.TrimSpace(string(v))
	default:
		data, err := json.Marshal(v)
		if err != nil {
			return strings.TrimSpace(fmt.Sprint(v))
		}
		return strings.TrimSpace(string(data))
	}
}

func tokenValueForAuth(auth *coreauth.Auth) string {
	if auth == nil {
		return ""
	}
	if auth.Metadata != nil {
		for _, key := range []string{"access_token", "token", "id_token", "cookie"} {
			if value := strings.TrimSpace(stringValueAny(auth.Metadata[key])); value != "" {
				return value
			}
		}
	}
	if auth.Attributes != nil {
		if value := strings.TrimSpace(auth.Attributes["api_key"]); value != "" {
			return value
		}
	}
	return ""
}

func chooseAccountID(item authItem, metadata map[string]any, fallback string) string {
	candidates := []any{item.AccountID}
	if metadata != nil {
		candidates = append(candidates, metadata["account_id"], metadata["accountId"])
	}
	candidates = append(candidates, fallback)
	for _, candidate := range candidates {
		if value := strings.TrimSpace(stringValueAny(candidate)); value != "" {
			return value
		}
	}
	return ""
}

func newHTTPClient(auth *coreauth.Auth, globalProxy string, timeout time.Duration) *http.Client {
	proxyURL := strings.TrimSpace(globalProxy)
	if auth != nil && strings.TrimSpace(auth.ProxyURL) != "" {
		proxyURL = strings.TrimSpace(auth.ProxyURL)
	}
	client := &http.Client{Timeout: timeout}
	transport, _, err := proxyutil.BuildHTTPTransport(proxyURL)
	if err != nil {
		log.WithError(err).Warn("auth cleaner proxy configuration invalid, falling back to direct")
	}
	if transport == nil {
		transport = proxyutil.NewDirectTransport()
	}
	client.Transport = transport
	return client
}

func loadStateFile(path string) (*stateFile, error) {
	state := defaultState()
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return state, nil
		}
		return nil, err
	}
	if len(bytes.TrimSpace(data)) == 0 {
		return state, nil
	}
	if errUnmarshal := json.Unmarshal(data, state); errUnmarshal != nil {
		return state, nil
	}
	if state.QuotaAccounts == nil {
		state.QuotaAccounts = make(map[string]*quotaStateEntry)
	}
	if state.Version == 0 {
		state.Version = 1
	}
	return state, nil
}

func saveStateFile(path string, state *stateFile) error {
	if state == nil {
		state = defaultState()
	}
	if state.QuotaAccounts == nil {
		state.QuotaAccounts = make(map[string]*quotaStateEntry)
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return err
	}
	data, err := json.MarshalIndent(state, "", "  ")
	if err != nil {
		return err
	}
	tmp := path + ".tmp"
	if errWrite := os.WriteFile(tmp, append(data, '\n'), 0o600); errWrite != nil {
		return errWrite
	}
	return os.Rename(tmp, path)
}

func defaultState() *stateFile {
	return &stateFile{Version: 1, QuotaAccounts: make(map[string]*quotaStateEntry)}
}

func ensureQuotaState(state *stateFile, item authItem, reason string, opts cleanerOptions) *quotaStateEntry {
	if state == nil {
		state = defaultState()
	}
	if state.QuotaAccounts == nil {
		state.QuotaAccounts = make(map[string]*quotaStateEntry)
	}
	entry := state.QuotaAccounts[item.Name]
	if entry == nil {
		entry = &quotaStateEntry{}
		state.QuotaAccounts[item.Name] = entry
	}
	now := time.Now().UTC()
	if entry.QuotaDisabledAt == "" {
		entry.QuotaDisabledAt = formatISOTime(now)
	}
	if entry.NextRevivalCheckAt == "" {
		entry.NextRevivalCheckAt = formatISOTime(now.Add(time.Duration(opts.RevivalWaitDays) * 24 * time.Hour))
	}
	entry.Provider = item.Provider
	entry.Reason = strings.TrimSpace(reason)
	entry.Path = item.Path
	entry.AuthIndex = item.AuthIndex
	entry.Email = item.Email
	entry.AccountID = item.AccountID
	entry.LastSeenAt = formatISOTime(now)
	return entry
}

func clearQuotaState(state *stateFile, name string) bool {
	if state == nil || state.QuotaAccounts == nil {
		return false
	}
	if _, ok := state.QuotaAccounts[name]; ok {
		delete(state.QuotaAccounts, name)
		return true
	}
	return false
}

func formatISOTime(t time.Time) string {
	return t.UTC().Format(time.RFC3339)
}

func parseISOTime(raw string) time.Time {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return time.Time{}
	}
	t, err := time.Parse(time.RFC3339, trimmed)
	if err != nil {
		return time.Time{}
	}
	return t.UTC()
}

func writeReportFile(reportDir string, report *RunReport) error {
	if report == nil {
		return fmt.Errorf("report is nil")
	}
	if err := os.MkdirAll(reportDir, 0o700); err != nil {
		return err
	}
	name := fmt.Sprintf("report-%s.json", report.RunID)
	path := filepath.Join(reportDir, name)
	data, err := json.MarshalIndent(report, "", "  ")
	if err != nil {
		return err
	}
	if errWrite := os.WriteFile(path, append(data, '\n'), 0o600); errWrite != nil {
		return errWrite
	}
	report.ReportPath = path
	return nil
}

func applyRetention(opts cleanerOptions) error {
	if err := cleanupReports(opts.ReportDir, opts.RetentionKeepReports, opts.RetentionReportMaxAgeDays); err != nil {
		return err
	}
	return cleanupBackupRoot(opts.BackupDir, opts.RetentionBackupMaxAgeDays)
}

func cleanupReports(reportDir string, keepCount, maxAgeDays int) error {
	entries, err := os.ReadDir(reportDir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	type reportEntry struct {
		name string
		path string
		mod  time.Time
	}
	files := make([]reportEntry, 0)
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		name := entry.Name()
		if !strings.HasPrefix(name, "report-") || !strings.HasSuffix(strings.ToLower(name), ".json") {
			continue
		}
		info, errInfo := entry.Info()
		if errInfo != nil {
			continue
		}
		files = append(files, reportEntry{name: name, path: filepath.Join(reportDir, name), mod: info.ModTime().UTC()})
	}
	sort.Slice(files, func(i, j int) bool { return files[i].mod.After(files[j].mod) })
	var cutoff time.Time
	if maxAgeDays > 0 {
		cutoff = time.Now().UTC().Add(-time.Duration(maxAgeDays) * 24 * time.Hour)
	}
	for idx, file := range files {
		remove := false
		if keepCount > 0 && idx >= keepCount {
			remove = true
		}
		if !cutoff.IsZero() && file.mod.Before(cutoff) {
			remove = true
		}
		if remove {
			_ = os.Remove(file.path)
		}
	}
	return nil
}

func cleanupBackupRoot(root string, maxAgeDays int) error {
	entries, err := os.ReadDir(root)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	var cutoff time.Time
	if maxAgeDays > 0 {
		cutoff = time.Now().UTC().Add(-time.Duration(maxAgeDays) * 24 * time.Hour)
	}
	for _, entry := range entries {
		full := filepath.Join(root, entry.Name())
		info, errInfo := entry.Info()
		if errInfo != nil {
			continue
		}
		if !cutoff.IsZero() && info.ModTime().UTC().Before(cutoff) {
			_ = os.RemoveAll(full)
			continue
		}
		if entry.IsDir() {
			_ = removeEmptyDirs(full)
		}
	}
	return nil
}

func removeEmptyDirs(root string) error {
	entries, err := os.ReadDir(root)
	if err != nil {
		return err
	}
	for _, entry := range entries {
		if entry.IsDir() {
			_ = removeEmptyDirs(filepath.Join(root, entry.Name()))
		}
	}
	entries, err = os.ReadDir(root)
	if err != nil {
		return err
	}
	if len(entries) == 0 {
		return os.Remove(root)
	}
	return nil
}

func (s *Service) lookupAuth(id string) *coreauth.Auth {
	manager := s.snapshotManager()
	if manager == nil || strings.TrimSpace(id) == "" {
		return nil
	}
	auth, _ := manager.GetByID(id)
	return auth
}

func (s *Service) disableAuth(ctx context.Context, item authItem, statusMessage string) error {
	manager := s.snapshotManager()
	if manager == nil {
		return fmt.Errorf("core auth manager unavailable")
	}
	auth, ok := manager.GetByID(item.ID)
	if !ok || auth == nil {
		return fmt.Errorf("auth not found")
	}
	now := time.Now().UTC()
	auth.Disabled = true
	auth.Status = coreauth.StatusDisabled
	auth.StatusMessage = statusMessage
	auth.UpdatedAt = now
	_, err := manager.Update(ctx, auth)
	return err
}

func (s *Service) enableAuth(ctx context.Context, item authItem, metadata map[string]any) error {
	manager := s.snapshotManager()
	if manager == nil {
		return fmt.Errorf("core auth manager unavailable")
	}
	auth, ok := manager.GetByID(item.ID)
	if !ok || auth == nil {
		return fmt.Errorf("auth not found")
	}
	now := time.Now().UTC()
	auth.Metadata = cloneMap(metadata)
	auth.Disabled = false
	auth.Unavailable = false
	auth.Status = coreauth.StatusActive
	auth.StatusMessage = ""
	auth.Quota = coreauth.QuotaState{}
	auth.LastError = nil
	auth.NextRetryAfter = time.Time{}
	auth.ModelStates = nil
	auth.LastRefreshedAt = now
	auth.UpdatedAt = now
	_, err := manager.Update(ctx, auth)
	return err
}

func (s *Service) updateAuthMetadata(ctx context.Context, item authItem, metadata map[string]any, keepDisabled bool) error {
	manager := s.snapshotManager()
	if manager == nil {
		return fmt.Errorf("core auth manager unavailable")
	}
	auth, ok := manager.GetByID(item.ID)
	if !ok || auth == nil {
		return fmt.Errorf("auth not found")
	}
	now := time.Now().UTC()
	auth.Metadata = cloneMap(metadata)
	auth.LastRefreshedAt = now
	auth.UpdatedAt = now
	if keepDisabled {
		auth.Disabled = true
		auth.Status = coreauth.StatusDisabled
		if strings.TrimSpace(auth.StatusMessage) == "" {
			auth.StatusMessage = "disabled by auth cleaner"
		}
	}
	_, err := manager.Update(ctx, auth)
	return err
}

func cloneMap(in map[string]any) map[string]any {
	if in == nil {
		return nil
	}
	data, err := json.Marshal(in)
	if err != nil {
		out := make(map[string]any, len(in))
		for k, v := range in {
			out[k] = v
		}
		return out
	}
	var out map[string]any
	if err := json.Unmarshal(data, &out); err != nil {
		out = make(map[string]any, len(in))
		for k, v := range in {
			out[k] = v
		}
	}
	return out
}

func (s *Service) deleteAuthFile(ctx context.Context, store coreauth.Store, item authItem, backupDir string) (string, error) {
	if item.Path == "" {
		return "", fmt.Errorf("missing auth file path")
	}
	data, err := os.ReadFile(item.Path)
	if err != nil {
		return "", err
	}
	backupPath, err := writeBackupBytes(backupDir, item.Name, data)
	if err != nil {
		return "", err
	}
	if store != nil {
		if errDelete := store.Delete(ctx, item.Path); errDelete != nil && !os.IsNotExist(errDelete) {
			if errRemove := os.Remove(item.Path); errRemove != nil && !os.IsNotExist(errRemove) {
				return "", errDelete
			}
		}
	} else {
		if errRemove := os.Remove(item.Path); errRemove != nil && !os.IsNotExist(errRemove) {
			return "", errRemove
		}
	}
	_ = s.disableAuth(ctx, item, "removed by auth cleaner")
	return backupPath, nil
}

func writeBackupBytes(dir, name string, data []byte) (string, error) {
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return "", err
	}
	base := sanitizeFileName(name)
	if base == "" {
		base = "unknown.json"
	}
	path := filepath.Join(dir, base)
	if err := os.WriteFile(path, data, 0o600); err != nil {
		return "", err
	}
	return path, nil
}

func copyJSONFile(src, dstDir string) (string, error) {
	data, err := os.ReadFile(src)
	if err != nil {
		return "", err
	}
	return writeBackupBytes(dstDir, filepath.Base(src), data)
}

func loadJSONPayload(path string) (map[string]any, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var payload map[string]any
	if errUnmarshal := json.Unmarshal(data, &payload); errUnmarshal != nil {
		return nil, errUnmarshal
	}
	return payload, nil
}

func writeJSONPayload(path string, payload map[string]any) error {
	if payload == nil {
		payload = make(map[string]any)
	}
	data, err := json.MarshalIndent(payload, "", "  ")
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return err
	}
	tmp := path + ".tmp"
	if errWrite := os.WriteFile(tmp, append(data, '\n'), 0o600); errWrite != nil {
		return errWrite
	}
	return os.Rename(tmp, path)
}

func (s *Service) runRevivalCycle(ctx context.Context, opts cleanerOptions, store coreauth.Store, item authItem, entry *quotaStateEntry, summary *RunSummary, backupRoot string) map[string]any {
	row := map[string]any{
		"name":          item.Name,
		"provider":      item.Provider,
		"auth_index":    item.AuthIndex,
		"path":          firstNonEmpty(item.Path, entry.Path),
		"revival_cycle": true,
	}
	summary.RevivalPending++
	now := time.Now().UTC()
	entry.LastRevivalCheckAt = formatISOTime(now)

	if item.Provider != "codex" && item.Provider != "openai" && item.Provider != "chatgpt" {
		row["revival_result"] = "skip_unsupported_provider"
		row["reason"] = "provider 不在 codex/openai/chatgpt 范围内"
		entry.NextRevivalCheckAt = formatISOTime(now.Add(time.Duration(opts.RevivalProbeIntervalHours) * time.Hour))
		return row
	}

	path := firstNonEmpty(item.Path, entry.Path)
	if path == "" {
		row["revival_result"] = "skip_missing_path"
		row["reason"] = "缺少本地文件路径，无法刷新并写回 token"
		entry.NextRevivalCheckAt = formatISOTime(now.Add(time.Duration(opts.RevivalProbeIntervalHours) * time.Hour))
		return row
	}

	authPayload, err := loadJSONPayload(path)
	if err != nil {
		row["revival_result"] = "load_failed"
		row["reason"] = err.Error()
		if isDeleteWorthy(err.Error()) {
			backupPath, errDelete := s.deleteAuthFile(ctx, store, item, filepath.Join(backupRoot, "delete-on-load-failed"))
			if errDelete != nil {
				row["delete_error"] = errDelete.Error()
				entry.NextRevivalCheckAt = formatISOTime(now.Add(time.Duration(opts.RevivalProbeIntervalHours) * time.Hour))
			} else {
				summary.RevivalDeleted++
				row["revival_result"] = "deleted_on_load_failure"
				row["backup_path"] = backupPath
			}
		} else {
			entry.NextRevivalCheckAt = formatISOTime(now.Add(time.Duration(opts.RevivalProbeIntervalHours) * time.Hour))
		}
		return row
	}
	row["auth_payload_loaded"] = true

	summary.RefreshAttempts++
	if backupPath, errBackup := copyJSONFile(path, filepath.Join(backupRoot, "before-refresh")); errBackup == nil {
		row["refresh_backup"] = backupPath
	}
	refreshedPayload, refreshResp, errRefresh := refreshOpenAIFamilyTokens(ctx, opts, item, authPayload)
	if errRefresh != nil {
		summary.RefreshFailed++
		row["refresh_result"] = "failed"
		row["reason"] = errRefresh.Error()
		entry.LastError = errRefresh.Error()
		if isDeleteWorthy(errRefresh.Error()) {
			backupPath, errDelete := s.deleteAuthFile(ctx, store, item, filepath.Join(backupRoot, "delete-on-refresh-failed"))
			if errDelete != nil {
				row["delete_error"] = errDelete.Error()
				entry.NextRevivalCheckAt = formatISOTime(now.Add(time.Duration(opts.RevivalProbeIntervalHours) * time.Hour))
			} else {
				summary.RevivalDeleted++
				row["revival_result"] = "deleted_on_refresh_failure"
				row["backup_path"] = backupPath
			}
		} else {
			entry.NextRevivalCheckAt = formatISOTime(now.Add(time.Duration(opts.RevivalProbeIntervalHours) * time.Hour))
		}
		return row
	}
	summary.RefreshSucceeded++
	row["refresh_result"] = "ok"
	row["refresh_response_keys"] = sortedMapKeys(refreshResp)
	if errWrite := writeJSONPayload(path, refreshedPayload); errWrite != nil {
		row["revival_result"] = "write_failed"
		row["reason"] = errWrite.Error()
		entry.LastError = errWrite.Error()
		entry.NextRevivalCheckAt = formatISOTime(now.Add(time.Duration(opts.RevivalProbeIntervalHours) * time.Hour))
		return row
	}
	_ = s.updateAuthMetadata(ctx, item, refreshedPayload, true)

	probeResp, errProbe := directProbeAuth(ctx, opts, item, refreshedPayload)
	if errProbe != nil {
		row["revival_result"] = "probe_failed"
		row["reason"] = errProbe.Error()
		entry.LastError = errProbe.Error()
		entry.NextRevivalCheckAt = formatISOTime(now.Add(time.Duration(opts.RevivalProbeIntervalHours) * time.Hour))
		return row
	}
	row["probe"] = probeResp
	classification, reason := classifyAPICallResponse(probeResp)
	row["revival_classification"] = classification
	row["reason"] = reason

	switch classification {
	case "", "ok":
		if errEnable := s.enableAuth(ctx, item, refreshedPayload); errEnable != nil {
			row["revival_result"] = "enable_failed"
			row["reason"] = errEnable.Error()
			entry.LastError = errEnable.Error()
			entry.NextRevivalCheckAt = formatISOTime(now.Add(time.Duration(opts.RevivalProbeIntervalHours) * time.Hour))
			return row
		}
		summary.RevivalEnabled++
		row["revival_result"] = "enabled"
		return row
	case "quota_exhausted":
		summary.RevivalStillQuota++
		row["revival_result"] = "still_quota_exhausted"
		entry.LastError = simplifyReason(reason)
		entry.NextRevivalCheckAt = formatISOTime(now.Add(time.Duration(opts.RevivalProbeIntervalHours) * time.Hour))
		return row
	default:
		if classification == "delete_401" || isDeleteWorthy(reason) {
			backupPath, errDelete := s.deleteAuthFile(ctx, store, item, filepath.Join(backupRoot, "delete-on-bad-probe"))
			if errDelete != nil {
				row["revival_result"] = "delete_failed"
				row["delete_error"] = errDelete.Error()
				entry.LastError = errDelete.Error()
				entry.NextRevivalCheckAt = formatISOTime(now.Add(time.Duration(opts.RevivalProbeIntervalHours) * time.Hour))
				return row
			}
			summary.RevivalDeleted++
			row["revival_result"] = "deleted_on_bad_probe"
			row["backup_path"] = backupPath
			return row
		}
		row["revival_result"] = "retry_later"
		entry.LastError = simplifyReason(reason)
		entry.NextRevivalCheckAt = formatISOTime(now.Add(time.Duration(opts.RevivalProbeIntervalHours) * time.Hour))
		return row
	}
}

func refreshOpenAIFamilyTokens(ctx context.Context, opts cleanerOptions, item authItem, authPayload map[string]any) (map[string]any, map[string]any, error) {
	refreshToken := strings.TrimSpace(stringValueAny(authPayload["refresh_token"]))
	if refreshToken == "" {
		return nil, nil, fmt.Errorf("missing refresh token")
	}
	form := url.Values{}
	form.Set("client_id", openAIClientID)
	form.Set("grant_type", "refresh_token")
	form.Set("refresh_token", refreshToken)
	form.Set("scope", openAIScope)
	auth := &coreauth.Auth{ProxyURL: firstNonEmpty(stringValueAny(authPayload["proxy_url"]), opts.ProxyURL)}
	client := newHTTPClient(auth, opts.ProxyURL, time.Duration(opts.TimeoutSeconds)*time.Second)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, openAITokenURL, strings.NewReader(form.Encode()))
	if err != nil {
		return nil, nil, err
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.Header.Set("Accept", "application/json")
	req.Header.Set("User-Agent", opts.APICallUserAgent)
	resp, err := client.Do(req)
	if err != nil {
		return nil, nil, fmt.Errorf("refresh request failed: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()
	bodyBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, nil, err
	}
	bodyText := strings.TrimSpace(string(bodyBytes))
	if resp.StatusCode != http.StatusOK {
		return nil, nil, fmt.Errorf("token refresh failed with status %d: %s", resp.StatusCode, bodyText)
	}
	var tokenResp map[string]any
	if err := json.Unmarshal(bodyBytes, &tokenResp); err != nil {
		return nil, nil, fmt.Errorf("failed to parse refresh response: %w", err)
	}
	accessToken := strings.TrimSpace(stringValueAny(tokenResp["access_token"]))
	if accessToken == "" {
		return nil, tokenResp, fmt.Errorf("refresh response missing access_token")
	}
	newPayload := cloneMap(authPayload)
	if newPayload == nil {
		newPayload = make(map[string]any)
	}
	newPayload["access_token"] = accessToken
	if refresh := strings.TrimSpace(stringValueAny(tokenResp["refresh_token"])); refresh != "" {
		newPayload["refresh_token"] = refresh
	}
	if idToken := strings.TrimSpace(stringValueAny(tokenResp["id_token"])); idToken != "" {
		newPayload["id_token"] = idToken
		claims := parseJWTPayload(idToken)
		if email := strings.TrimSpace(stringValueAny(claims["email"])); email != "" {
			newPayload["email"] = email
		}
		for _, candidate := range []string{
			stringValueAny(claims["https://api.openai.com/profile/account_id"]),
			stringValueAny(claims["account_id"]),
			stringValueAny(claims["org_id"]),
		} {
			if trimmed := strings.TrimSpace(candidate); trimmed != "" {
				newPayload["account_id"] = trimmed
				break
			}
		}
	}
	newPayload["last_refresh"] = formatISOTime(time.Now().UTC())
	newPayload["expired"] = false
	if newPayload["account_id"] == nil && item.AccountID != "" {
		newPayload["account_id"] = item.AccountID
	}
	return newPayload, tokenResp, nil
}

func directProbeAuth(ctx context.Context, opts cleanerOptions, item authItem, authPayload map[string]any) (*httpProbeResponse, error) {
	accessToken := strings.TrimSpace(stringValueAny(authPayload["access_token"]))
	if accessToken == "" {
		return nil, fmt.Errorf("missing access_token，无法测活")
	}
	bodyReader := io.Reader(nil)
	if strings.TrimSpace(opts.APICallBody) != "" {
		bodyReader = strings.NewReader(opts.APICallBody)
	}
	req, err := http.NewRequestWithContext(ctx, strings.ToUpper(opts.APICallMethod), opts.APICallURL, bodyReader)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", "Bearer "+accessToken)
	req.Header.Set("User-Agent", opts.APICallUserAgent)
	req.Header.Set("Accept", "application/json")
	if bodyReader != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	if accountID := chooseAccountID(item, authPayload, opts.APICallAccountID); accountID != "" {
		req.Header.Set("Chatgpt-Account-Id", accountID)
	}
	auth := &coreauth.Auth{ProxyURL: firstNonEmpty(stringValueAny(authPayload["proxy_url"]), opts.ProxyURL)}
	client := newHTTPClient(auth, opts.ProxyURL, time.Duration(opts.TimeoutSeconds)*time.Second)
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }()
	bodyBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	return &httpProbeResponse{StatusCode: resp.StatusCode, Header: resp.Header, Body: parseBody(bodyBytes)}, nil
}

func parseJWTPayload(token string) map[string]any {
	parts := strings.Split(strings.TrimSpace(token), ".")
	if len(parts) < 2 {
		return map[string]any{}
	}
	payloadPart := parts[1]
	padding := strings.Repeat("=", (4-len(payloadPart)%4)%4)
	raw, err := base64.URLEncoding.DecodeString(payloadPart + padding)
	if err != nil {
		return map[string]any{}
	}
	var payload map[string]any
	if err := json.Unmarshal(raw, &payload); err != nil {
		return map[string]any{}
	}
	return payload
}

func isDeleteWorthy(text string) bool {
	trimmed := strings.TrimSpace(text)
	if trimmed == "" {
		return false
	}
	lowered := strings.ToLower(trimmed)
	return pattern401.MatchString(lowered) || patternRemove.MatchString(lowered)
}

func simplifyReason(reason string) string {
	trimmed := strings.TrimSpace(reason)
	if trimmed == "" {
		return ""
	}
	if !strings.HasPrefix(trimmed, "{") {
		if len(trimmed) > 160 {
			return trimmed[:160]
		}
		return trimmed
	}
	var payload map[string]any
	if err := json.Unmarshal([]byte(trimmed), &payload); err != nil {
		if len(trimmed) > 160 {
			return trimmed[:160]
		}
		return trimmed
	}
	errorObj, ok := payload["error"]
	if !ok {
		if len(trimmed) > 160 {
			return trimmed[:160]
		}
		return trimmed
	}
	if m, okMap := errorObj.(map[string]any); okMap {
		errorType := strings.TrimSpace(stringValueAny(m["type"]))
		errorMessage := strings.TrimSpace(stringValueAny(m["message"]))
		out := firstNonEmpty(errorType, errorMessage, trimmed)
		if errorType == "usage_limit_reached" && errorMessage != "" {
			out = errorType + ": " + errorMessage
		}
		if len(out) > 160 {
			return out[:160]
		}
		return out
	}
	out := strings.TrimSpace(stringValueAny(errorObj))
	if len(out) > 160 {
		return out[:160]
	}
	return out
}

func sortedMapKeys(m map[string]any) []string {
	keys := make([]string, 0, len(m))
	for key := range m {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if trimmed := strings.TrimSpace(value); trimmed != "" {
			return trimmed
		}
	}
	return ""
}

func stringValueAny(value any) string {
	switch v := value.(type) {
	case nil:
		return ""
	case string:
		return v
	case fmt.Stringer:
		return v.String()
	case json.Number:
		return v.String()
	case int:
		return strconv.Itoa(v)
	case int64:
		return strconv.FormatInt(v, 10)
	case float64:
		return strconv.FormatFloat(v, 'f', -1, 64)
	case bool:
		if v {
			return "true"
		}
		return "false"
	default:
		return fmt.Sprint(v)
	}
}
