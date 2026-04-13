(() => {
  const STORAGE_KEY = "cpa.authCleaner.managementKey";
  const ROOT_ID = "cpa-auth-cleaner-root";
  const BTN_ID = "cpa-auth-cleaner-open";
  const STYLE_ID = "cpa-auth-cleaner-style";

  function ready(fn) {
    if (document.readyState === "loading") {
      document.addEventListener("DOMContentLoaded", fn, { once: true });
      return;
    }
    fn();
  }

  function formatJson(value) {
    return JSON.stringify(value, null, 2);
  }

  function formatTime(value) {
    if (!value) return "-";
    const date = new Date(value);
    if (Number.isNaN(date.getTime())) return String(value);
    return date.toLocaleString();
  }

  function create(tag, props = {}, children = []) {
    const el = document.createElement(tag);
    Object.entries(props).forEach(([key, value]) => {
      if (key === "className") {
        el.className = value;
      } else if (key === "text") {
        el.textContent = value;
      } else if (key === "html") {
        el.innerHTML = value;
      } else if (key.startsWith("on") && typeof value === "function") {
        el.addEventListener(key.slice(2).toLowerCase(), value);
      } else if (value != null) {
        el.setAttribute(key, value);
      }
    });
    for (const child of [].concat(children || [])) {
      if (child == null) continue;
      el.append(child.nodeType ? child : document.createTextNode(String(child)));
    }
    return el;
  }

  function ensureStyle() {
    if (document.getElementById(STYLE_ID)) return;
    const style = create("style", {
      id: STYLE_ID,
      text: `
#${BTN_ID}{
position:fixed;right:20px;bottom:20px;z-index:2147483646;border:none;border-radius:999px;
background:linear-gradient(135deg,#0f766e,#2563eb);color:#fff;padding:12px 18px;font-size:14px;
box-shadow:0 12px 32px rgba(15,23,42,.35);cursor:pointer;font-weight:700
}
#${ROOT_ID}{position:fixed;inset:0;z-index:2147483647;display:none}
#${ROOT_ID}.open{display:block}
#${ROOT_ID} .ac-mask{position:absolute;inset:0;background:rgba(2,6,23,.6);backdrop-filter:blur(2px)}
#${ROOT_ID} .ac-panel{
position:absolute;top:3vh;right:2vw;width:min(1180px,96vw);height:94vh;background:#0f172a;color:#e2e8f0;
border:1px solid rgba(148,163,184,.3);border-radius:18px;box-shadow:0 25px 80px rgba(2,6,23,.45);
display:flex;flex-direction:column;overflow:hidden;font-family:ui-sans-serif,system-ui,-apple-system,BlinkMacSystemFont
}
#${ROOT_ID} .ac-header{display:flex;align-items:center;justify-content:space-between;padding:16px 18px;border-bottom:1px solid rgba(148,163,184,.2);background:#111827}
#${ROOT_ID} .ac-title{font-size:18px;font-weight:800}
#${ROOT_ID} .ac-subtitle{font-size:12px;color:#94a3b8;margin-top:4px}
#${ROOT_ID} .ac-close{border:none;background:#1e293b;color:#fff;border-radius:10px;padding:8px 12px;cursor:pointer}
#${ROOT_ID} .ac-body{display:grid;grid-template-columns:300px 1fr;min-height:0;flex:1}
#${ROOT_ID} .ac-side{border-right:1px solid rgba(148,163,184,.18);padding:14px;background:#0b1220;overflow:auto}
#${ROOT_ID} .ac-main{padding:16px;overflow:auto;background:#0f172a}
#${ROOT_ID} .ac-block{background:#111827;border:1px solid rgba(148,163,184,.16);border-radius:14px;padding:12px 14px;margin-bottom:14px}
#${ROOT_ID} .ac-label{display:block;font-size:12px;color:#94a3b8;margin-bottom:6px}
#${ROOT_ID} .ac-input,#${ROOT_ID} .ac-select{
width:100%;box-sizing:border-box;border-radius:10px;border:1px solid rgba(148,163,184,.25);background:#020617;color:#e2e8f0;
padding:10px 12px;font-size:13px
}
#${ROOT_ID} .ac-actions{display:grid;grid-template-columns:1fr 1fr;gap:8px}
#${ROOT_ID} .ac-btn{
border:none;border-radius:10px;background:#1d4ed8;color:#fff;padding:10px 12px;font-size:13px;font-weight:700;cursor:pointer
}
#${ROOT_ID} .ac-btn.secondary{background:#334155}
#${ROOT_ID} .ac-btn.warn{background:#b45309}
#${ROOT_ID} .ac-btn.danger{background:#b91c1c}
#${ROOT_ID} .ac-btn:disabled{opacity:.55;cursor:not-allowed}
#${ROOT_ID} .ac-grid{display:grid;grid-template-columns:repeat(4,minmax(0,1fr));gap:10px;margin-bottom:14px}
#${ROOT_ID} .ac-card{background:#111827;border:1px solid rgba(148,163,184,.16);border-radius:14px;padding:12px}
#${ROOT_ID} .ac-card .k{font-size:12px;color:#94a3b8;margin-bottom:8px}
#${ROOT_ID} .ac-card .v{font-size:24px;font-weight:800}
#${ROOT_ID} .ac-section-title{font-size:15px;font-weight:800;margin-bottom:10px}
#${ROOT_ID} .ac-meta{display:flex;flex-wrap:wrap;gap:8px;margin-bottom:10px}
#${ROOT_ID} .ac-pill{background:#0b1220;color:#cbd5e1;border:1px solid rgba(148,163,184,.14);border-radius:999px;padding:6px 10px;font-size:12px}
#${ROOT_ID} .ac-kv{display:grid;grid-template-columns:180px 1fr;gap:8px 12px;font-size:13px}
#${ROOT_ID} .ac-kv .k{color:#94a3b8}
#${ROOT_ID} .ac-kv .v{word-break:break-word}
#${ROOT_ID} .ac-report-list{display:flex;flex-direction:column;gap:8px}
#${ROOT_ID} .ac-report-item{
background:#0b1220;border:1px solid rgba(148,163,184,.12);border-radius:12px;padding:10px;cursor:pointer
}
#${ROOT_ID} .ac-report-item:hover{border-color:rgba(96,165,250,.6)}
#${ROOT_ID} .ac-report-item.active{border-color:#60a5fa;background:#0f1b33}
#${ROOT_ID} .ac-actions-list{display:flex;flex-direction:column;gap:8px}
#${ROOT_ID} .ac-action-item{background:#0b1220;border:1px solid rgba(148,163,184,.12);border-radius:12px;padding:10px}
#${ROOT_ID} .ac-pre{
margin:0;white-space:pre-wrap;word-break:break-word;background:#020617;border:1px solid rgba(148,163,184,.15);
border-radius:12px;padding:12px;max-height:220px;overflow:auto;font-size:12px;line-height:1.5
}
@media (max-width: 980px){
  #${ROOT_ID} .ac-body{grid-template-columns:1fr}
  #${ROOT_ID} .ac-side{border-right:none;border-bottom:1px solid rgba(148,163,184,.18)}
  #${ROOT_ID} .ac-grid{grid-template-columns:repeat(2,minmax(0,1fr))}
  #${ROOT_ID} .ac-kv{grid-template-columns:1fr}
}
      `,
    });
    document.head.append(style);
  }

  function init() {
    if (document.getElementById(ROOT_ID) || document.getElementById(BTN_ID)) return;
    ensureStyle();

    const state = {
      busy: false,
      reports: [],
      activeReportName: "",
      latestStatus: null,
      latestConfig: null,
      latestState: null,
      latestReport: null,
    };

    const openBtn = create("button", { id: BTN_ID, text: "Auth Cleaner" });
    const root = create("div", { id: ROOT_ID });
    const mask = create("div", { className: "ac-mask" });
    const keyInput = create("input", {
      className: "ac-input",
      type: "password",
      placeholder: "输入 Management Key",
      value: localStorage.getItem(STORAGE_KEY) || "",
    });
    const runMode = create("select", { className: "ac-select" }, [
      create("option", { value: "dry", text: "Dry Run（只分析）" }),
      create("option", { value: "real", text: "真实运行（会改动 auth）" }),
    ]);

    const statusMeta = create("div", { className: "ac-meta" });
    const summaryGrid = create("div", { className: "ac-grid" });
    const overviewKv = create("div", { className: "ac-kv" });
    const reportList = create("div", { className: "ac-report-list" });
    const reportMeta = create("div", { className: "ac-meta" });
    const reportSummaryGrid = create("div", { className: "ac-grid" });
    const reportActionsList = create("div", { className: "ac-actions-list" });
    const logPre = create("pre", { className: "ac-pre", text: "等待操作…" });

    function setBusy(flag) {
      state.busy = flag;
      root.querySelectorAll(".ac-btn").forEach((btn) => {
        btn.disabled = flag;
      });
    }

    function writeLog(title, payload) {
      const lines = [];
      lines.push(`[${new Date().toLocaleString()}] ${title}`);
      if (payload !== undefined) {
        lines.push(typeof payload === "string" ? payload : formatJson(payload));
      }
      logPre.textContent = lines.join("\n");
    }

    function getKey(required = true) {
      const value = keyInput.value.trim();
      if (value) {
        localStorage.setItem(STORAGE_KEY, value);
        return value;
      }
      const cached = localStorage.getItem(STORAGE_KEY) || "";
      if (cached) {
        keyInput.value = cached;
        return cached;
      }
      if (!required) return "";
      const prompted = window.prompt("请输入 Management Key");
      if (!prompted) throw new Error("缺少 Management Key");
      keyInput.value = prompted.trim();
      localStorage.setItem(STORAGE_KEY, keyInput.value);
      return keyInput.value;
    }

    async function api(path, options = {}) {
      const headers = new Headers(options.headers || {});
      const key = getKey();
      if (key) headers.set("X-Management-Key", key);
      if (options.body != null && !headers.has("Content-Type")) {
        headers.set("Content-Type", "application/json");
      }
      const response = await fetch(path, {
        method: options.method || "GET",
        headers,
        body:
          options.body == null
            ? undefined
            : typeof options.body === "string"
              ? options.body
              : JSON.stringify(options.body),
        credentials: "same-origin",
      });
      const text = await response.text();
      let data = text;
      try {
        data = text ? JSON.parse(text) : {};
      } catch {
        // keep raw text
      }
      if (!response.ok) {
        const message =
          typeof data === "object" && data && data.error
            ? data.error
            : `${response.status} ${response.statusText}`;
        throw new Error(message);
      }
      return data;
    }

    function renderSummaryCards(status) {
      const summary = status?.last_summary || {};
      const cards = [
        ["本轮已扫描", summary.checked_total ?? 0],
        ["当前可用", summary.available ?? 0],
        ["当前已禁用", summary.disabled ?? 0],
        ["本轮新禁用", summary.quota_disabled ?? 0],
        ["本轮刷新成功", summary.refresh_succeeded ?? 0],
        ["本轮恢复启用", summary.revival_enabled ?? 0],
        ["本轮进入恢复", summary.revival_pending ?? 0],
        ["本轮删除401", summary.deleted ?? 0],
      ];
      summaryGrid.innerHTML = "";
      cards.forEach(([key, value]) => {
        summaryGrid.append(
          create("div", { className: "ac-card" }, [
            create("div", { className: "k", text: key }),
            create("div", { className: "v", text: String(value) }),
          ]),
        );
      });
    }

    function renderStatusMeta(status) {
      statusMeta.innerHTML = "";
      const cleanerConfig = state.latestConfig?.["auth-cleaner"] || {};
      const trackedQuota = Object.keys(state.latestState?.quota_accounts || {}).length;
      const pills = [
        `运行中: ${status?.running ? "是" : "否"}`,
        `启用: ${status?.enabled ? "是" : "否"}`,
        `间隔: ${status?.interval_seconds ?? "-"} 秒`,
        `主动探测: ${cleanerConfig.enable_api_call_check ? "开" : "关"}`,
        `quota 追踪数: ${trackedQuota}`,
        `下次: ${formatTime(status?.next_run_at)}`,
        `上次开始: ${formatTime(status?.last_started_at)}`,
        `上次结束: ${formatTime(status?.last_finished_at)}`,
      ];
      if (status?.last_error) pills.push(`最后错误: ${status.last_error}`);
      pills.forEach((text) => statusMeta.append(create("div", { className: "ac-pill", text })));
    }

    function renderOverview() {
      const status = state.latestStatus || {};
      const cleanerConfig = state.latestConfig?.["auth-cleaner"] || {};
      const quotaAccounts = Object.keys(state.latestState?.quota_accounts || {}).length;
      const entries = [
        ["调度间隔", `${status?.interval_seconds ?? "-"} 秒`],
        ["超时时间", `${cleanerConfig.timeout_seconds ?? "-"} 秒`],
        ["主动探测", cleanerConfig.enable_api_call_check ? "开启" : "关闭"],
        ["探测 Provider", cleanerConfig.api_call_providers || "-"],
        ["下次自动扫描", formatTime(status?.next_run_at)],
        ["状态文件追踪数", quotaAccounts],
        ["报告目录", cleanerConfig.report_dir || "-"],
        ["备份目录", cleanerConfig.backup_dir || "-"],
      ];
      overviewKv.innerHTML = "";
      entries.forEach(([k, v]) => {
        overviewKv.append(
          create("div", { className: "k", text: k }),
          create("div", { className: "v", text: String(v) }),
        );
      });
    }

    function renderReports(activeName) {
      reportList.innerHTML = "";
      if (!state.reports.length) {
        reportList.append(create("div", { className: "ac-pill", text: "暂无报告" }));
        return;
      }
      state.reports.forEach((item) => {
        const button = create(
          "button",
          {
            className: `ac-report-item${item.name === activeName ? " active" : ""}`,
            onClick: () => loadReport(item.name),
          },
          [
            create("div", { text: item.name }),
            create("div", {
              className: "ac-subtitle",
              text: `生成时间: ${formatTime(item.generated_at || item.mod_time)} | dry_run: ${item.dry_run ? "true" : "false"}`,
            }),
          ],
        );
        reportList.append(button);
      });
    }

    function renderReportDetails(payload) {
      state.latestReport = payload || null;
      reportMeta.innerHTML = "";
      reportSummaryGrid.innerHTML = "";
      reportActionsList.innerHTML = "";

      if (!payload || typeof payload !== "object") {
        reportMeta.append(create("div", { className: "ac-pill", text: "暂无报告详情" }));
        return;
      }

      const summary = payload.summary || {};
      [
        `报告时间: ${formatTime(payload.generated_at)}`,
        `dry_run: ${payload.dry_run ? "true" : "false"}`,
        `run_id: ${payload.run_id || "-"}`,
      ].forEach((text) => reportMeta.append(create("div", { className: "ac-pill", text })));

      const cards = [
        ["已扫描", summary.checked_total ?? 0],
        ["新禁用", summary.quota_disabled ?? 0],
        ["刷新成功", summary.refresh_succeeded ?? 0],
        ["恢复启用", summary.revival_enabled ?? 0],
        ["仍然 quota", summary.revival_still_quota ?? 0],
        ["删除 401", summary.deleted ?? 0],
      ];
      cards.forEach(([key, value]) => {
        reportSummaryGrid.append(
          create("div", { className: "ac-card" }, [
            create("div", { className: "k", text: key }),
            create("div", { className: "v", text: String(value) }),
          ]),
        );
      });

      const actionRows = (payload.results || []).filter(
        (row) => row?.disable_result || row?.delete_result || row?.revival_result,
      );
      if (!actionRows.length) {
        reportActionsList.append(create("div", { className: "ac-pill", text: "本报告没有实际动作" }));
        return;
      }

      actionRows.slice(0, 80).forEach((row) => {
        const texts = [
          `provider=${row.provider || "-"}`,
          row.disable_result ? `disable=${row.disable_result}` : null,
          row.delete_result ? `delete=${row.delete_result}` : null,
          row.revival_result ? `revival=${row.revival_result}` : null,
          row.revival_classification ? `class=${row.revival_classification}` : null,
        ].filter(Boolean);
        reportActionsList.append(
          create("div", { className: "ac-action-item" }, [
            create("div", { text: row.name || "-" }),
            create("div", { className: "ac-subtitle", text: texts.join(" | ") }),
            row.reason
              ? create("div", {
                  className: "ac-subtitle",
                  text: `原因: ${String(row.reason).slice(0, 220)}`,
                })
              : null,
          ]),
        );
      });
    }

    async function loadStatusBundle() {
      setBusy(true);
      try {
        const [status, config, cleanerState] = await Promise.all([
          api("/v0/management/auth-cleaner/status"),
          api("/v0/management/auth-cleaner/config"),
          api("/v0/management/auth-cleaner/state"),
        ]);
        state.latestStatus = status;
        state.latestConfig = config;
        state.latestState = cleanerState;
        renderStatusMeta(status);
        renderSummaryCards(status);
        renderOverview();
        writeLog("已刷新状态 / 配置 / 状态文件", {
          next_run_at: status?.next_run_at,
          last_finished_at: status?.last_finished_at,
        });
      } catch (error) {
        writeLog("刷新状态失败", String(error?.message || error));
        throw error;
      } finally {
        setBusy(false);
      }
    }

    async function loadReports(selectLatest = false) {
      setBusy(true);
      try {
        const payload = await api("/v0/management/auth-cleaner/reports?limit=50");
        state.reports = Array.isArray(payload?.reports) ? payload.reports : [];
        const active = selectLatest ? state.reports[0]?.name || "" : state.activeReportName;
        renderReports(active);
        if (active) await loadReport(active);
        else renderReportDetails(null);
        writeLog("已刷新报告列表", {
          reports: state.reports.length,
          latest: state.reports[0]?.name || null,
        });
      } catch (error) {
        writeLog("刷新报告列表失败", String(error?.message || error));
        throw error;
      } finally {
        setBusy(false);
      }
    }

    async function loadReport(name) {
      if (!name) return;
      state.activeReportName = name;
      renderReports(name);
      try {
        const payload = await api(`/v0/management/auth-cleaner/reports/${encodeURIComponent(name)}`);
        renderReportDetails(payload);
      } catch (error) {
        renderReportDetails(null);
        throw error;
      }
    }

    async function runCleaner(dryRun) {
      const modeText = dryRun ? "Dry Run" : "真实运行";
      if (!dryRun) {
        const ok = window.confirm("真实运行会禁用 quota auth、删除不可恢复 401 文件（先备份）。确认继续？");
        if (!ok) return;
      }
      setBusy(true);
      try {
        const payload = await api(`/v0/management/auth-cleaner/run?dry_run=${dryRun ? "1" : "0"}`, {
          method: "POST",
        });
        writeLog(`${modeText} 完成`, payload);
        await loadStatusBundle();
        await loadReports(true);
      } catch (error) {
        writeLog(`${modeText} 失败`, String(error?.message || error));
        throw error;
      } finally {
        setBusy(false);
      }
    }

    async function reviveNow() {
      const ok = window.confirm("这会把所有已记录的 quota 账号立即推进到 refresh/revival 流程，并立刻执行一轮真实运行。确认继续？");
      if (!ok) return;
      setBusy(true);
      try {
        const payload = await api("/v0/management/auth-cleaner/revive-now", {
          method: "POST",
          body: { run_now: true },
        });
        writeLog("立即刷新额度账号完成", payload);
        await loadStatusBundle();
        await loadReports(true);
      } catch (error) {
        writeLog("立即刷新额度账号失败", String(error?.message || error));
        throw error;
      } finally {
        setBusy(false);
      }
    }

    const panel = create("div", { className: "ac-panel" }, [
      create("div", { className: "ac-header" }, [
        create("div", {}, [
          create("div", { className: "ac-title", text: "Auth Cleaner 控制页" }),
          create("div", {
            className: "ac-subtitle",
            text: "直接对接 /v0/management/auth-cleaner/* 后端接口",
          }),
        ]),
        create("button", {
          className: "ac-close",
          text: "关闭",
          onClick: () => root.classList.remove("open"),
        }),
      ]),
      create("div", { className: "ac-body" }, [
        create("div", { className: "ac-side" }, [
          create("div", { className: "ac-block" }, [
            create("label", { className: "ac-label", text: "Management Key" }),
            keyInput,
            create("div", {
              className: "ac-subtitle",
              text: "仅存本地浏览器 localStorage，用于调已存在的管理 API",
            }),
          ]),
          create("div", { className: "ac-block" }, [
            create("label", { className: "ac-label", text: "运行模式" }),
            runMode,
          ]),
          create("div", { className: "ac-block" }, [
            create("div", { className: "ac-actions" }, [
              create("button", {
                className: "ac-btn secondary",
                text: "刷新状态",
                onClick: () => loadStatusBundle(),
              }),
              create("button", {
                className: "ac-btn secondary",
                text: "刷新报告",
                onClick: () => loadReports(false),
              }),
              create("button", {
                className: "ac-btn warn",
                text: "执行扫描",
                onClick: () => runCleaner(runMode.value === "dry"),
              }),
              create("button", {
                className: "ac-btn danger",
                text: "立即刷新额度账号",
                onClick: reviveNow,
              }),
            ]),
          ]),
          create("div", { className: "ac-block" }, [
            create("div", { className: "ac-section-title", text: "报告列表" }),
            reportList,
          ]),
        ]),
        create("div", { className: "ac-main" }, [
          statusMeta,
          summaryGrid,
          create("div", { className: "ac-block" }, [
            create("div", { className: "ac-section-title", text: "运行概览" }),
            overviewKv,
          ]),
          create("div", { className: "ac-block" }, [
            create("div", { className: "ac-section-title", text: "当前报告概览" }),
            reportMeta,
            reportSummaryGrid,
          ]),
          create("div", { className: "ac-block" }, [
            create("div", { className: "ac-section-title", text: "当前报告动作明细" }),
            reportActionsList,
          ]),
          create("div", { className: "ac-block" }, [
            create("div", { className: "ac-section-title", text: "操作日志" }),
            logPre,
          ]),
        ]),
      ]),
    ]);

    root.append(mask, panel);
    document.body.append(openBtn, root);

    async function openPanel() {
      root.classList.add("open");
      await loadStatusBundle();
      await loadReports(true);
    }

    openBtn.addEventListener("click", () => {
      openPanel().catch((error) => writeLog("打开面板失败", String(error?.message || error)));
    });
    mask.addEventListener("click", () => root.classList.remove("open"));

    if (location.hash === "#auth-cleaner" || location.search.includes("auth-cleaner=1")) {
      openPanel().catch((error) => writeLog("自动打开面板失败", String(error?.message || error)));
    }
  }

  ready(init);
})();
