/* Harbor 管理台（2026-11 重写）
   结构：工具函数 → 通用组件 → 七个页面 → 登录与路由。
   约定：所有动态文本走 esc()；交互走 data-act / data-form / data-change 委托；
   不使用行内样式（CSP: style-src 'self'）。 */
(function () {
  "use strict";

  /* ================= 常量 ================= */
  var TOKEN_KEY = "harbor.admin.token";
  var TABS = { overview: "概览", queue: "队列", requests: "请求", keys: "密钥", providers: "上游", models: "模型", settings: "设置" };
  var PROV_TYPES = ["openai-compatible", "openai", "azure-openai", "anthropic", "gemini", "deepseek", "custom"];
  var CAPS = ["chat", "embedding", "vision", "reasoning", "tools"];
  var CAP_LABEL = { chat: "对话", embedding: "向量", vision: "视觉", reasoning: "推理", tools: "工具" };
  var COLS_KEY = "harbor.model.columns";
  var PROV_KEY = "harbor.model.provider";

  /* ================= 工具 ================= */
  function $(sel, root) { return (root || document).querySelector(sel); }
  function $$(sel, root) { return Array.prototype.slice.call((root || document).querySelectorAll(sel)); }

  function esc(v) {
    return String(v == null ? "" : v).replace(/[&<>"']/g, function (c) {
      return { "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[c];
    });
  }

  function icon(name) {
    return '<svg class="ico" aria-hidden="true"><use href="#i-' + esc(name) + '"></use></svg>';
  }

  function num(n) { return (Number(n) || 0).toLocaleString("en-US"); }

  function fmtMs(ms) {
    ms = Number(ms) || 0;
    if (ms <= 0) return "0";
    if (ms < 1000) return ms + "ms";
    var s = ms / 1000;
    if (s < 60) return (s < 10 ? s.toFixed(2) : s.toFixed(1)) + "s";
    return (s / 60).toFixed(1) + "m";
  }

  function fmtTime(v) {
    if (!v) return "—";
    var d = new Date(v);
    if (isNaN(d.getTime()) || d.getFullYear() < 1000) return "—";
    function p(n) { return String(n).padStart(2, "0"); }
    return p(d.getMonth() + 1) + "-" + p(d.getDate()) + " " + p(d.getHours()) + ":" + p(d.getMinutes()) + ":" + p(d.getSeconds());
  }

  function fmtTok(n) {
    n = Number(n) || 0;
    if (n >= 1e9) return (n / 1e9).toFixed(1) + "B";
    if (n >= 1e6) return (n / 1e6).toFixed(1) + "M";
    if (n >= 1e3) return (n / 1e3).toFixed(1) + "K";
    return String(n);
  }

  function errText(data) {
    if (!data) return "";
    if (typeof data === "string") return data;
    if (typeof data.error === "string") return data.error;
    if (data.error && data.error.message) return data.error.message;
    if (data.message) return data.message;
    return "";
  }

  /* ================= 状态 ================= */
  var state = {
    token: "",
    tab: "overview",
    toastTimer: null,
    clockTimer: null,
    pollTimer: null,
    modalReturn: null,
    stats: null,
    queue: null,
    reqs: { rows: [], total: 0, limit: 50, offset: 0, q: "", status: "", range: "today", provider: "", model: "", key: "" },
    keys: [],
    providers: [],
    models: { provider: "", rows: [], q: "", status: "all", view: "all", pick: {}, pickQ: "", probe: null, cols: { ctx: true, caps: true } },
    settings: null
  };

  var el = {
    login: $("#login"),
    app: $("#app"),
    loginForm: $("#login-form"),
    loginToken: $("#login-token"),
    loginErr: $("#login-err"),
    loginBtn: $("#login-btn"),
    rail: $("#rail"),
    nav: $("#nav"),
    menu: $("#menu"),
    logout: $("#logout"),
    title: $("#page-title"),
    clock: $("#clock"),
    modal: $("#modal"),
    modalTitle: $("#modal-title"),
    modalBody: $("#modal-body"),
    modalFoot: $("#modal-foot"),
    modalClose: $("#modal-close"),
    toast: $("#toast"),
    pages: {
      overview: $("#tab-overview"),
      queue: $("#tab-queue"),
      requests: $("#tab-requests"),
      keys: $("#tab-keys"),
      providers: $("#tab-providers"),
      models: $("#tab-models"),
      settings: $("#tab-settings")
    }
  };

  /* ================= 基础交互 ================= */
  async function api(path, opts) {
    var init = Object.assign({
      cache: "no-store",
      headers: { Authorization: "Bearer " + state.token, "Content-Type": "application/json" }
    }, opts || {});
    var res = await fetch(path, init);
    if (res.status === 401) { logout(); throw new Error("登录已过期，请重新输入 token"); }
    var data = null;
    try { data = await res.json(); } catch (e) { /* 空响应 */ }
    if (!res.ok) throw new Error(errText(data) || ("HTTP " + res.status));
    return data;
  }

  function toast(msg, isErr) {
    el.toast.textContent = msg;
    el.toast.classList.toggle("err", !!isErr);
    el.toast.hidden = false;
    clearTimeout(state.toastTimer);
    state.toastTimer = setTimeout(function () { el.toast.hidden = true; }, 4200);
  }

  async function busy(btn, fn) {
    if (btn && btn.disabled) return;
    var label = btn ? btn.innerHTML : "";
    if (btn) { btn.disabled = true; btn.innerHTML = icon("loader-circle") + "处理中…"; }
    try {
      await fn();
    } catch (e) {
      toast(e.message || "操作未完成", true);
    } finally {
      if (btn && btn.isConnected) { btn.disabled = false; btn.innerHTML = label; }
    }
  }

  function openModal(opt) {
    el.modalTitle.textContent = opt.title || "";
    el.modalBody.innerHTML = opt.body || "";
    el.modalFoot.innerHTML = opt.foot || "";
    el.modalFoot.hidden = !opt.foot;
    $(".modal", el.modal).classList.toggle("wide", !!opt.wide);
    state.modalReturn = document.activeElement;
    el.modal.hidden = false;
    if (opt.onMount) opt.onMount(el.modalBody, el.modalFoot);
    var first = $("input, select, textarea, .modal-foot .btn", el.modal);
    if (first) setTimeout(function () { first.focus(); }, 30);
  }

  function closeModal() {
    if (el.modal.hidden) return;
    el.modal.hidden = true;
    el.modalBody.innerHTML = "";
    el.modalFoot.innerHTML = "";
    if (state.modalReturn && state.modalReturn.isConnected) state.modalReturn.focus();
    state.modalReturn = null;
  }

  function confirmAction(opt) {
    openModal({
      title: opt.title,
      body: '<p class="modal-intro">' + esc(opt.text) + '</p>',
      foot: '<button type="button" class="btn ghost" data-act="modal.close">取消</button>' +
        '<button type="button" class="btn ' + (opt.danger === false ? "" : "danger") + '" id="confirm-btn">' + esc(opt.okLabel || "确认") + '</button>',
      onMount: function () {
        $("#confirm-btn").onclick = function () {
          var btn = this;
          busy(btn, async function () {
            await opt.onOk();
            closeModal();
            if (opt.after) await opt.after();
          });
        };
      }
    });
  }

  /* ================= 委托 ================= */
  var actions = {};
  var forms = {};
  var changes = {};

  document.addEventListener("click", function (ev) {
    var node = ev.target.closest("[data-act]");
    if (!node) return;
    var fn = actions[node.getAttribute("data-act")];
    if (!fn) return;
    ev.preventDefault();
    fn(node, ev);
  });

  document.addEventListener("submit", function (ev) {
    var form = ev.target.closest("form[data-form]");
    if (!form) return;
    ev.preventDefault();
    var fn = forms[form.getAttribute("data-form")];
    if (fn) fn(form);
  });

  document.addEventListener("change", function (ev) {
    var node = ev.target.closest("[data-change]");
    if (!node) return;
    var fn = changes[node.getAttribute("data-change")];
    if (fn) fn(node);
  });

  actions["modal.close"] = closeModal;

  /* ================= 通用片段 ================= */
  function pageHead(kicker, title, desc, actionsHTML) {
    return '<div class="page-head"><div><span class="kicker">' + esc(kicker) + '</span><h2>' + esc(title) + '</h2>' +
      (desc ? '<p>' + esc(desc) + '</p>' : "") + '</div>' +
      (actionsHTML ? '<div class="page-actions">' + actionsHTML + '</div>' : "") + '</div>';
  }

  function emptyState(iconName, title, text, actionHTML) {
    return '<div class="empty"><div class="box">' + icon(iconName) + '</div><strong>' + esc(title) + '</strong>' +
      '<p>' + esc(text) + '</p>' + (actionHTML || "") + '</div>';
  }

  function seg(name, current, options) {
    return '<div class="seg" role="group">' + options.map(function (opt) {
      return '<button type="button" data-act="' + esc(name) + '" data-val="' + esc(opt[0]) + '" aria-pressed="' + (String(current) === String(opt[0])) + '">' + esc(opt[1]) + '</button>';
    }).join("") + '</div>';
  }

  function statusBadge(status) {
    var map = {
      success: ["ok", "成功"], error: ["err", "失败"], timeout: ["warn", "超时"],
      cancelled: ["", "已取消"], active: ["ok", "可用"], banned: ["err", "已封禁"],
      expired: ["warn", "已过期"], cooling: ["warn", "冷却中"], unavailable: ["err", "不可用"],
      disabled: ["", "已禁用"], mapped: ["info", "有映射"], visible: ["ok", "可见"]
    };
    var item = map[status] || ["", status || "未知"];
    return '<span class="badge ' + item[0] + '">' + esc(item[1]) + '</span>';
  }

  /* ================= 概览 ================= */
  async function loadOverview() {
    state.stats = await api("/api/stats");
    renderOverview();
  }

  function renderOverview() {
    var d = state.stats || {};
    var t = d.today || {};
    var q = d.queue || {};
    var total = Number(t.requests) || 0;
    var pct = q.max_depth ? Math.min(100, Math.round((Number(q.depth) || 0) * 100 / q.max_depth)) : 0;
    var root = el.pages.overview;
    root.innerHTML =
      pageHead("HARBOR / OVERVIEW", "运行概览", "今天的请求、排队与 token 用量。数据每 5 秒刷新。",
        '<button type="button" class="btn ghost" data-act="overview.reload">' + icon("refresh") + '刷新</button>') +
      '<div class="stats">' +
      statCard("requests", "今日请求", num(total), "成功 " + num(t.success) + " · 失败 " + num(t.failed)) +
      statCard("queue", "排队深度", num(q.depth), "正在执行 " + num(q.running)) +
      statCard("activity", "P50 等待", fmtMs(t.p50_wait), "P95 " + fmtMs(t.p95_wait)) +
      statCard("cpu", "Token 用量", fmtTok(t.tokens_total), "入 " + fmtTok(t.tokens_in) + " · 出 " + fmtTok(t.tokens_out)) +
      '</div>' +
      '<div class="overview-grid">' +
      '<section class="card"><div class="card-head"><div><h3>队列水位</h3><p>排队中的请求占队列容量的比例，超过 80% 会更容易出现 429。</p></div></div>' +
      '<div class="card-body"><div class="queue-meter"><strong>' + num(q.depth) + '</strong><span>/ ' + num(q.max_depth) + ' 个等待中</span></div>' +
      '<progress class="progress" max="100" value="' + pct + '"></progress>' +
      '<div class="kv"><div><span>最长等待</span><strong>' + esc(q.longest_wait || "0s") + '</strong></div>' +
      '<div><span>排队中</span><strong>' + num(t.queued) + '</strong></div>' +
      '<div><span>最大等待</span><strong>' + fmtMs(t.max_wait_ms) + '</strong></div></div></div></section>' +
      '<section class="card"><div class="card-head"><div><h3>用量明细</h3><p>token 计数来自上游 usage 与本地估算，用于观察配额消耗。</p></div></div>' +
      '<div class="card-body"><div class="kv"><div><span>输入 token</span><strong>' + num(t.tokens_in) + '</strong></div>' +
      '<div><span>输出 token</span><strong>' + num(t.tokens_out) + '</strong></div>' +
      '<div><span>缓存命中</span><strong>' + num(t.tokens_cached) + '</strong></div></div></div></section></div>';

    var bar = $("progress.progress", root);
    if (bar) bar.value = pct;
  }

  function statCard(iconName, label, value, note) {
    return '<div class="stat"><span>' + icon(iconName) + esc(label) + '</span><strong>' + esc(value) + '</strong><small>' + esc(note) + '</small></div>';
  }

  actions["overview.reload"] = function (btn) {
    busy(btn, async function () { await loadOverview(); });
  };

  /* ================= 队列 ================= */
  async function loadQueue() {
    state.queue = await api("/api/queue");
    renderQueue();
  }

  function renderQueue() {
    var d = state.queue || { depth: 0, max_depth: 0, items: [] };
    var items = d.items || [];
    var rows = items.map(function (it) {
      return '<tr><td class="num">' + num(it.position) + '</td>' +
        '<td class="mono">' + esc(it.request_id) + '</td>' +
        '<td>' + esc(it.model) + '</td>' +
        '<td class="num">' + esc(it.waiting_for) + '</td></tr>';
    }).join("");
    el.pages.queue.innerHTML =
      pageHead("HARBOR / QUEUE", "实时队列", "FIFO 队列里的等待者；队头拿不到额度时后面的请求不会越位。",
        '<button type="button" class="btn ghost" data-act="queue.reload">' + icon("refresh") + '刷新</button>') +
      '<div class="stats">' +
      statCard("queue", "排队中", num(d.depth), "容量 " + num(d.max_depth)) +
      statCard("activity", "执行中", num((state.stats && state.stats.queue && state.stats.queue.running) || 0), "已获得上游额度") +
      statCard("timer", "最长等待", esc((state.stats && state.stats.queue && state.stats.queue.longest_wait) || "0s"), "队首等待时长") +
      '</div>' +
      '<section class="card"><div class="card-head"><div><h3>等待列表</h3><p>按入队顺序排列，位置 1 是队头。</p></div></div>' +
      '<div class="table-wrap">' + (items.length ?
        '<table class="table"><thead><tr><th>位置</th><th>请求 ID</th><th>模型</th><th class="num">已等待</th></tr></thead><tbody>' + rows + '</tbody></table>' :
        emptyState("check", "队列是空的", "当前没有请求在等待，说明上游额度够用。")) + '</div></section>';
  }

  actions["queue.reload"] = function (btn) {
    busy(btn, async function () { await loadQueue(); });
  };

  /* ================= 请求 ================= */
  var REQ_RANGES = [["today", "今天"], ["yesterday", "昨天"], ["7d", "7 天"], ["30d", "30 天"], ["month", "本月"], ["all", "全部"]];
  var REQ_STATUS = [["", "全部状态"], ["queued", "排队中"], ["connecting", "连接中"], ["waiting", "等待响应"], ["responding", "响应中"], ["success", "成功"], ["error", "失败"], ["timeout", "超时"], ["cancelled", "取消"]];

  function reqWindow(preset) {
    var now = new Date();
    var start = new Date(now.getFullYear(), now.getMonth(), now.getDate());
    if (preset === "all") return {};
    if (preset === "yesterday") {
      var y = new Date(start); y.setDate(y.getDate() - 1);
      return { since: y.toISOString(), until: start.toISOString() };
    }
    if (preset === "7d") { var d = new Date(start); d.setDate(d.getDate() - 6); return { since: d.toISOString() }; }
    if (preset === "30d") { var d2 = new Date(start); d2.setDate(d2.getDate() - 29); return { since: d2.toISOString() }; }
    if (preset === "month") return { since: new Date(now.getFullYear(), now.getMonth(), 1).toISOString() };
    return { since: start.toISOString() };
  }

  function reqDate(v) {
    if (!v) return "—";
    var d = new Date(v);
    if (isNaN(d.getTime()) || d.getFullYear() < 1000) return "—";
    function p(n) { return String(n).padStart(2, "0"); }
    return p(d.getMonth() + 1) + "-" + p(d.getDate());
  }

  function reqClock(v) {
    if (!v) return "";
    var d = new Date(v);
    if (isNaN(d.getTime())) return "";
    function p(n) { return String(n).padStart(2, "0"); }
    return p(d.getHours()) + ":" + p(d.getMinutes()) + ":" + p(d.getSeconds());
  }

  function reqElapsed(it) {
    function parse(v) {
      var d = new Date(v);
      return isNaN(d.getTime()) || d.getFullYear() < 1000 ? null : d;
    }
    var start = parse(it.started_at) || parse(it.queued_at);
    if (!start) return 0;
    var end = parse(it.finished_at) || new Date();
    return Math.max(0, end - start);
  }

  function reqWhen(it) {
    var end = it.finished_at;
    if (end) {
      var d = new Date(end);
      if (!isNaN(d.getTime()) && d.getFullYear() >= 1000) return end;
    }
    return it.queued_at;
  }

  function reqDuration(it) {
    var ms = reqElapsed(it);
    if (!ms) return '<span class="muted">—</span>';
    return fmtMs(ms) + (it.first_token_ms ? '<small class="hint log-sub">首帧 ' + fmtMs(it.first_token_ms) + "</small>" : "");
  }

  function reqSpeed(it) {
    var ms = reqElapsed(it);
    if (!ms || !it.tokens_out) return '<span class="muted">—</span>';
    var v = it.tokens_out / (ms / 1000);
    return (v >= 1000 ? (v / 1000).toFixed(1) + "k" : v.toFixed(1)) + " tok/s";
  }

  function reqTokens(it) {
    var inTok = it.tokens_in || 0;
    var outTok = it.tokens_out || 0;
    var total = inTok + outTok;
    var cached = it.cached_tokens || 0;
    var written = it.cache_write_tokens || 0;
    var rate = inTok > 0 ? (cached / inTok * 100).toFixed(2) + "%" : "—";
    return '<div class="tok-lines"><span>in: ' + num(inTok) + " / out: " + num(outTok) + " / all: " + num(total) + '</span>' +
      "<span>缓存读取: " + num(cached) + " / 写入: " + num(written) + " / 命中率: " + rate + "</span></div>";
  }

  function reqStatusText(it) {
    if (it.status === "queued") return "排队中";
    if (it.status === "connecting") return "连接中";
    if (it.status === "waiting") return "等待响应";
    if (it.status === "responding") return "响应中";
    if (it.status === "success") return "成功 (200)";
    if (it.status === "timeout") return "超时";
    if (it.status === "cancelled") return "已取消";
    return "失败" + (it.upstream_http ? " (" + it.upstream_http + ")" : (it.error_code ? " · " + it.error_code : ""));
  }

  function reqStatusCell(it) {
    if (it.status === "queued") return '<span class="badge">● 排队中</span>';
    if (it.status === "connecting") return '<span class="badge info">● 连接中</span>';
    if (it.status === "waiting") return '<span class="badge info">● 等待响应</span>';
    if (it.status === "responding") return '<span class="badge ok">● 响应中</span>';
    if (it.status === "success") return '<span class="badge ok">200</span>';
    if (it.status === "timeout") return '<span class="badge warn">超时</span>';
    if (it.status === "cancelled") return '<span class="badge">取消</span>';
    var code = it.upstream_http ? String(it.upstream_http) : "错误";
    return '<span class="badge err">' + esc(code) + "</span>";
  }

  function withThinkingLevel(model, level) {
    var display = String(model || "");
    level = String(level || "").toLowerCase();
    if (!level) return display;
    var lower = display.toLowerCase();
    if (lower.indexOf("(" + level + ")") >= 0 || lower.indexOf("[" + level + "]") >= 0 || lower.indexOf("（" + level + "）") >= 0) {
      return display;
    }
    return display + "(" + level + ")";
  }

  function reqModelCell(it) {
    var upstream = it.upstream_model || "";
    if (!upstream) {
      var route = (state.reqOptions.routes || []).filter(function (r) { return r.model === it.model; })[0];
      if (route) {
        var m = (route.mappings || []).filter(function (x) { return x.provider === it.provider; })[0];
        if (m && m.upstream_model) upstream = m.upstream_model;
      }
    }
    return '<div class="model-chain"><strong>' + esc(withThinkingLevel(it.model, it.thinking_level)) + (it.stream ? ' <span class="badge info">SSE</span>' : "") + "</strong>" +
      (upstream ? "<small>→ " + esc(upstream) + "</small>" : "") + "</div>";
  }

  function reqClientCell(it) {
    if (!it.client_ua && !it.client_ip) return '<span class="muted">—</span>';
    var ua = String(it.client_ua || "").split(/[ (\/]/)[0];
    return '<div class="model-chain"><strong>' + esc(ua || "未知客户端") + "</strong>" +
      (it.client_ip ? "<small>" + esc(it.client_ip) + "</small>" : "") + "</div>";
  }

  function reqErrorCell(it) {
    if (!it.error_code && !it.error_detail) return '<span class="muted">—</span>';
    var text = it.error_code || "错误";
    return '<span class="badge err" title="' + esc(it.error_detail || text) + '">' + esc(text) + "</span>";
  }

  async function loadRequests() {
    var r = state.reqs;
    var qs = new URLSearchParams({ limit: String(r.limit), offset: String(r.offset) });
    if (r.q) qs.set("q", r.q);
    if (r.status) qs.set("status", r.status);
    if (r.provider) qs.set("provider", r.provider);
    if (r.model) qs.set("model", r.model);
    if (r.key) qs.set("key_id", r.key);
    var win = reqWindow(r.range);
    if (win.since) qs.set("since", win.since);
    if (win.until) qs.set("until", win.until);

    var res = await Promise.all([
      api("/api/requests?" + qs.toString()),
      api("/api/providers"),
      api("/api/keys"),
      api("/api/routes")
    ]);
    r.rows = res[0].requests || [];
    r.total = res[0].total || 0;
    var providers = res[1].providers || [];
    var models = {};
    (res[3].routes || []).forEach(function (rt) { models[rt.model] = true; });
    providers.forEach(function (p) { (p.models || []).forEach(function (m) { models[m] = true; }); });
    state.reqOptions = {
      providers: providers.map(function (p) { return p.name; }),
      keys: (res[2].keys || []).map(function (k) { return k.id; }),
      models: Object.keys(models).sort(),
      routes: res[3].routes || []
    };
    renderRequests();
  }

  function reqSelect(name, current, values, placeholder) {
    return '<select class="ctl" data-change="req.filter" data-filter="' + name + '" aria-label="' + esc(placeholder) + '">' +
      '<option value="">' + esc(placeholder) + "</option>" +
      values.map(function (v) {
        return '<option value="' + esc(v) + '"' + (v === current ? " selected" : "") + ">" + esc(v) + "</option>";
      }).join("") + "</select>";
  }

  function renderRequests() {
    var r = state.reqs;
    var opt = state.reqOptions || { providers: [], keys: [], models: [], routes: [] };
    var rows = r.rows.map(function (it) {
      return '<tr><td class="log-time">' + reqDate(reqWhen(it)) + "<small>" + reqClock(reqWhen(it)) + "</small></td>" +
        "<td>" + esc(it.provider || "—") + "</td>" +
        '<td class="mono" title="' + esc(it.key_id || "") + '">' + esc(it.key_id || "—") + "</td>" +
        '<td class="num">' + num(it.attempts) + "</td>" +
        "<td>" + reqStatusCell(it) + "</td>" +
        "<td>" + reqModelCell(it) + "</td>" +
        '<td class="mono" title="' + esc(it.request_id) + '">' + esc(it.request_id) + (it.upstream_request_id ? '<small class="req-sub" title="' + esc(it.upstream_request_id) + '">' + esc(it.upstream_request_id) + "</small>" : "") + "</td>" +
        "<td>" + reqClientCell(it) + "</td>" +
        '<td class="num">' + reqDuration(it) + "</td>" +
        '<td class="num">' + reqSpeed(it) + "</td>" +
        "<td>" + reqTokens(it) + "</td>" +
        "<td>" + reqErrorCell(it) + "</td>" +
        '<td class="actions"><button type="button" class="btn ghost sm" data-act="req.detail" data-id="' + esc(it.request_id) + '">详情</button></td></tr>';
    }).join("");

    var from = r.total ? r.offset + 1 : 0;
    var to = Math.min(r.offset + r.limit, r.total);

    el.pages.requests.innerHTML =
      pageHead("HARBOR / REQUESTS", "请求日志", "按时间、状态、上游、模型与 Key 过滤；每条记录可查看完整审计。",
        '<button type="button" class="btn ghost" data-act="req.reload">' + icon("refresh") + "刷新</button>") +
      '<section class="card"><div class="toolbar req-toolbar">' +
      seg("req.range", r.range, REQ_RANGES) +
      reqSelect("provider", r.provider, opt.providers, "全部提供商") +
      reqSelect("model", r.model, opt.models, "全部模型") +
      reqSelect("key", r.key, opt.keys, "全部 API Key") +
      seg("req.status", r.status, REQ_STATUS) +
      '<div class="search">' + icon("search") + '<input class="ctl" id="req-q" placeholder="搜索请求 ID / 模型 / 错误码" value="' + esc(r.q) + '"></div>' +
      "</div>" +
      '<div class="table-wrap">' + (r.rows.length ?
        '<table class="table dense log-table"><colgroup><col><col><col><col><col><col><col><col><col><col><col><col><col></colgroup><thead><tr><th>时间</th><th>提供商</th><th>API Key</th><th class="num">重试</th><th>状态</th><th>模型</th><th>请求 ID</th><th>客户端</th><th class="num">用时</th><th class="num">速度</th><th>用量</th><th>错误</th><th></th></tr></thead><tbody>' + rows + "</tbody></table>" :
        emptyState("file", "这段时间没有请求", "换个时间范围或清空筛选；有流量后这里会逐条记录。")) +
      "</div>" +
      '<div class="pager"><span>共 ' + num(r.total) + " 条 · 显示 " + from + "–" + to + '</span><div class="page-actions">' +
      '<button type="button" class="btn ghost sm" data-act="req.prev"' + (r.offset <= 0 ? " disabled" : "") + ">上一页</button>" +
      '<button type="button" class="btn ghost sm" data-act="req.next"' + (to >= r.total ? " disabled" : "") + ">下一页</button></div></div></section>";

    var input = $("#req-q");
    if (input) {
      input.oninput = function () {
        clearTimeout(state.reqTimer);
        var v = input.value;
        state.reqTimer = setTimeout(function () {
          state.reqs.q = v;
          state.reqs.offset = 0;
          loadRequests().catch(function (e) { toast(e.message, true); });
        }, 280);
      };
    }
  }

  changes["req.filter"] = function (select) {
    var field = select.getAttribute("data-filter");
    state.reqs[field] = select.value;
    state.reqs.offset = 0;
    loadRequests().catch(function (e) { toast(e.message, true); });
  };

  actions["req.reload"] = function (btn) {
    busy(btn, async function () { await loadRequests(); });
  };
  actions["req.range"] = function (btn) {
    state.reqs.range = btn.getAttribute("data-val");
    state.reqs.offset = 0;
    loadRequests().catch(function (e) { toast(e.message, true); });
  };
  actions["req.status"] = function (btn) {
    state.reqs.status = btn.getAttribute("data-val");
    state.reqs.offset = 0;
    loadRequests().catch(function (e) { toast(e.message, true); });
  };
  actions["req.prev"] = function () {
    state.reqs.offset = Math.max(0, state.reqs.offset - state.reqs.limit);
    loadRequests().catch(function (e) { toast(e.message, true); });
  };
  actions["req.next"] = function () {
    state.reqs.offset += state.reqs.limit;
    loadRequests().catch(function (e) { toast(e.message, true); });
  };

  function reqKv(label, value, mono) {
    return '<div><span>' + esc(label) + "</span><strong" + (mono ? ' class="mono"' : "") + ">" + esc(value == null || value === "" ? "—" : value) + "</strong></div>";
  }

  function reqDetailBody(it) {
    var ms = reqElapsed(it);
    var total = (it.tokens_in || 0) + (it.tokens_out || 0);
    var speed = ms && it.tokens_out ? (it.tokens_out / (ms / 1000)).toFixed(1) + " tok/s" : "—";
    var modelLine = withThinkingLevel(it.model, it.thinking_level);
    if (it.provider) {
      modelLine += " → " + (it.upstream_model || it.model);
    }
    var kvRows = [
      ["时间", fmtTime(reqWhen(it))],
      ["请求 ID", it.request_id, true],
      ["入站路径", it.inbound_path || "—", true],
      ["上游 Request ID", it.upstream_request_id || "—", true],
      ["状态", reqStatusText(it)],
      ["流式", it.stream ? "是（SSE）" : "否（普通 JSON，全部生成完才返回）"],
      ["思考等级", it.thinking_level || "—"],
      ["用时", ms ? fmtMs(ms) : "—"],
      ["首帧", it.first_token_ms ? fmtMs(it.first_token_ms) : "—"],
      ["上游响应头", it.upstream_header_ms ? fmtMs(it.upstream_header_ms) : "—"],
      ["上游 Content-Type", it.upstream_content_type || "—", true],
      ["重试次数", it.attempts],
      ["API Key", it.key_id, true],
      ["来源客户端", (it.client_ua || "—") + (it.client_ip ? " · " + it.client_ip : "")],
      ["模型", modelLine, true],
      ["上游响应模型", it.response_model || it.upstream_model || "—", true],
      ["提供商", it.provider || "—"],
      ["令牌", "入 " + num(it.tokens_in) + " / 出 " + num(it.tokens_out) + " / 共 " + num(total) + (it.cached_tokens ? " · 缓存读 " + num(it.cached_tokens) : "") + (it.cache_write_tokens ? " · 缓存写 " + num(it.cache_write_tokens) : "") + (it.tokens_in && it.cached_tokens ? " · 命中率 " + (it.cached_tokens / it.tokens_in * 100).toFixed(2) + "%" : "")],
      ["速度", speed],
      ["排队等待", fmtMs(it.queue_wait_ms)],
      ["错误", (it.error_code || "") + (it.error_detail ? " · " + it.error_detail : "")]
    ];
    var cells = kvRows.map(function (row) { return reqKv(row[0], row[1], row[2]); });
    while (cells.length % 5 !== 0) cells.push('<div class="kv-empty"></div>');
    var detail = '<div class="kv">' + cells.join("") + "</div>";

    var route = '<div class="chain"><span class="node">' + esc(it.model) + "</span><span class=\"arrow\">→</span>" +
      '<span class="node">' + esc(it.provider || "未命中上游") + "</span>" +
      (it.upstream_model && it.upstream_model !== it.model ? '<span class="arrow">→</span><span class="node">' + esc(it.upstream_model) + "</span>" : "") +
      "</div>" +
      '<table class="table"><thead><tr><th>轮次</th><th>提供商</th><th>结果</th><th class="num">上游 HTTP</th><th>提示</th></tr></thead><tbody>' +
      "<tr><td>" + num(it.attempts) + "</td><td>" + esc(it.provider || "—") + "</td>" +
      "<td>" + reqStatusCell(it) + "</td><td class=\"num\">" + esc(it.upstream_http || "—") + "</td>" +
      "<td>" + esc(it.error_detail || it.error_code || "—") + "</td></tr></tbody></table>" +
      '<p class="hint">本版本按请求汇总记录，未保留每次重试的独立明细。</p>' +
      '<div class="preview">' + esc(JSON.stringify(it, null, 2)) + "</div>";

    return '<div class="tabs" role="tablist">' +
      '<button type="button" role="tab" aria-selected="true" data-tab-btn="detail">请求详情</button>' +
      '<button type="button" role="tab" aria-selected="false" data-tab-btn="route">路由回放</button></div>' +
      '<div data-pane="detail">' + detail + "</div>" +
      '<div data-pane="route" hidden>' + route + "</div>";
  }

  actions["req.detail"] = function (btn) {
    var id = btn.getAttribute("data-id");
    busy(btn, async function () {
      var it = await api("/api/requests/" + encodeURIComponent(id));
      openModal({
        title: "请求详情",
        wide: true,
        body: reqDetailBody(it),
        foot: '<button type="button" class="btn ghost" data-act="modal.close">关闭</button>',
        onMount: function (body) {
          $$("[data-tab-btn]", body).forEach(function (tab) {
            tab.onclick = function () {
              $$("[data-tab-btn]", body).forEach(function (t) { t.setAttribute("aria-selected", String(t === tab)); });
              $$("[data-pane]", body).forEach(function (pane) { pane.hidden = pane.getAttribute("data-pane") !== tab.getAttribute("data-tab-btn"); });
            };
          });
        }
      });
    });
  };

  /* ================= 密钥 ================= */
  async function loadKeys() {
    var data = await api("/api/keys");
    state.keys = (data.keys || []).slice().sort(function (a, b) { return String(a.created_at).localeCompare(String(b.created_at)); });
    renderKeys();
  }

  function renderKeys() {
    var rows = state.keys.map(function (k) {
      var models = (k.allowed_models || []).length
        ? (k.allowed_models || []).map(function (m) { return '<span class="chip">' + esc(m) + '</span>'; }).join("")
        : '<span class="chip soft">全部模型</span>';
      var banned = k.status === "banned";
      return '<div class="key-row"><div class="grow"><div class="id">' + esc(k.id) + '</div>' +
        '<div class="meta">' + statusBadge(k.status || "active") + models + '<span class="hint">创建于 ' + fmtTime(k.created_at) + '</span></div></div>' +
        '<button type="button" class="btn ghost sm" data-act="key.edit" data-id="' + esc(k.id) + '">模型权限</button>' +
        (banned
          ? '<button type="button" class="btn ghost sm" data-act="key.unban" data-id="' + esc(k.id) + '">解封</button>'
          : '<button type="button" class="btn ghost sm" data-act="key.ban" data-id="' + esc(k.id) + '">封禁</button>') +
        '<button type="button" class="icon-btn danger" data-act="key.remove" data-id="' + esc(k.id) + '" aria-label="删除密钥">' + icon("trash-2") + '</button></div>';
    }).join("");

    el.pages.keys.innerHTML =
      pageHead("HARBOR / KEYS", "访问密钥", "客户端用这些 Key 调用网关；secret 只在创建时显示一次。",
        '<button type="button" class="btn" data-act="key.create">' + icon("plus") + '创建密钥</button>') +
      '<section class="card"><div class="card-head"><div><h3>密钥列表</h3><p>封禁立即生效；删除会让该 secret 立刻失效。</p></div><span class="badge">' + state.keys.length + ' 个</span></div>' +
      (state.keys.length ? rows : emptyState("key", "还没有密钥", "创建第一个 Key 后，客户端就能用 Bearer 调用 /v1。",
        '<button type="button" class="btn" data-act="key.create">' + icon("plus") + '创建密钥</button>')) + '</section>';
  }

  actions["key.create"] = function () {
    openModal({
      title: "创建密钥",
      body: '<form data-form="key.create">' +
        '<label class="field"><span>允许的模型（可选）</span><input class="ctl" name="models" placeholder="留空 = 全部模型，多个用逗号分隔"></label>' +
        '<p class="hint">创建后 secret 只显示这一次，请立刻复制到客户端配置里。</p></form>',
      foot: '<button type="button" class="btn ghost" data-act="modal.close">取消</button><button type="button" class="btn" data-act="key.createSubmit">创建</button>'
    });
  };

  forms["key.create"] = function () {
    var btn = $('.modal-foot .btn:not(.ghost)');
    if (btn) btn.click();
  };

  forms["prov.save"] = function () {
    var btn = $('.modal-foot .btn:not(.ghost)');
    if (btn) btn.click();
  };

  function keyPayloadModels(form) {
    var raw = (form.querySelector('[name="models"]').value || "").trim();
    return raw ? raw.split(/[,，\s]+/).filter(Boolean) : null;
  }

  actions["key.createSubmit"] = function (btn) {
    var form = $('[data-form="key.create"]');
    busy(btn, async function () {
      var models = keyPayloadModels(form);
      var body = models ? { allowed_models: models } : {};
      var data = await api("/api/keys", { method: "POST", body: JSON.stringify(body) });
      openModal({
        title: "密钥已创建",
        body: '<p class="modal-intro">这是唯一一次显示 secret，关闭后无法再取回。</p>' +
          '<div class="secret-box"><code>' + esc(data.secret) + '</code></div>' +
          '<p class="hint">用法：<span class="mono">Authorization: Bearer &lt;secret&gt;</span></p>',
        foot: '<button type="button" class="btn" data-act="modal.close">我已保存</button>',
        onMount: function () {
          var box = $(".secret-box code", el.modalBody);
          if (box) box.onclick = function () { try { navigator.clipboard.writeText(box.textContent); toast("已复制"); } catch (e) { /* 忽略 */ } };
        }
      });
      await loadKeys();
    });
  };

  actions["key.edit"] = function (btn) {
    var id = btn.getAttribute("data-id");
    var key = state.keys.filter(function (k) { return k.id === id; })[0] || {};
    openModal({
      title: "模型权限",
      body: '<p class="modal-intro mono">' + esc(id) + '</p>' +
        '<label class="field"><span>允许的模型（留空 = 全部）</span><input class="ctl" id="key-models" value="' + esc((key.allowed_models || []).join(", ")) + '"></label>' +
        '<p class="hint">保存后会立即影响该 Key 能调用的模型列表。</p>',
      foot: '<button type="button" class="btn ghost" data-act="modal.close">取消</button><button type="button" class="btn" id="key-save">保存</button>',
      onMount: function () {
        $("#key-save").onclick = function () {
          var btn2 = this;
          busy(btn2, async function () {
            var raw = $("#key-models").value.trim();
            var models = raw ? raw.split(/[,，\s]+/).filter(Boolean) : [];
            await api("/api/keys/" + encodeURIComponent(id), { method: "PUT", body: JSON.stringify({ allowed_models: models }) });
            closeModal();
            toast("模型权限已更新");
            await loadKeys();
          });
        };
      }
    });
  };

  actions["key.ban"] = function (btn) {
    var id = btn.getAttribute("data-id");
    busy(btn, async function () {
      await api("/api/keys/" + encodeURIComponent(id) + "/ban", { method: "POST", body: "{}" });
      toast("已封禁");
      await loadKeys();
    });
  };

  actions["key.unban"] = function (btn) {
    var id = btn.getAttribute("data-id");
    busy(btn, async function () {
      await api("/api/keys/" + encodeURIComponent(id) + "/unban", { method: "POST", body: "{}" });
      toast("已解封");
      await loadKeys();
    });
  };

  actions["key.remove"] = function (btn) {
    var id = btn.getAttribute("data-id");
    confirmAction({
      title: "删除密钥",
      text: "删除 “" + id + "” 后该 secret 立即失效，且无法恢复。",
      okLabel: "删除",
      onOk: async function () {
        await api("/api/keys/" + encodeURIComponent(id), { method: "DELETE" });
        toast("已删除");
      },
      after: loadKeys
    });
  };

  /* ================= 上游 ================= */
  async function loadProviders() {
    var data = await api("/api/providers");
    state.providers = (data.providers || []).slice().sort(function (a, b) { return a.name.localeCompare(b.name); });
    renderProviders();
  }

  function providerCard(p) {
    var creds = (p.credentials || []).map(function (c) {
      return '<div class="cred-row"><span class="id">' + esc(c.id) + '</span>' +
        statusBadge(c.status) +
        '<button type="button" class="icon-btn danger" data-act="prov.credRemove" data-name="' + esc(p.name) + '" data-cred="' + esc(c.id) + '" aria-label="移除凭证">' + icon("x") + '</button></div>';
    }).join("") || '<p class="hint">还没有凭证，添加后即可转发。</p>';
    var models = (p.models || []).map(function (m) { return '<span class="chip">' + esc(m) + '</span>'; }).join("") || '<p class="hint">未限制模型范围</p>';
    var limits = [["并发", p.concurrency], ["RPM", p.rpm], ["TPM", p.tpm], ["TPH", p.tph], ["TPD", p.tpd]]
      .map(function (x) { return '<div><span>' + esc(x[0]) + '</span><strong>' + (Number(x[1]) > 0 ? num(x[1]) : "∞") + '</strong></div>'; }).join("");

    return '<article class="provider"><div class="provider-top"><div class="provider-mark">' + icon("server") + '</div>' +
      '<div><div class="provider-name">' + esc(p.name) + '</div><div class="provider-type">' + esc(p.type) + '</div></div>' +
      '<div class="spacer"></div>' + statusBadge(p.available ? "active" : "unavailable") + '</div>' +
      '<div class="provider-url">' + esc(p.base_url) + '</div>' +
      '<div class="limits">' + limits + '</div>' +
      '<div class="provider-section"><h4>模型范围</h4><div class="cell">' + models + '</div></div>' +
      '<div class="provider-section"><h4>访问凭证</h4>' + creds +
      '<form class="inline-form" data-form="prov.credAdd" data-name="' + esc(p.name) + '">' +
      '<input class="ctl" type="password" name="secret" autocomplete="new-password" placeholder="输入上游 API Key" required>' +
      '<button type="submit" class="btn ghost sm">' + icon("plus") + '添加</button></form></div>' +
      '<div class="provider-actions"><button type="button" class="btn ghost sm" data-act="prov.edit" data-name="' + esc(p.name) + '">' + icon("sliders-horizontal") + '编辑配置</button>' +
      '<div class="spacer"></div>' +
      '<button type="button" class="icon-btn danger" data-act="prov.remove" data-name="' + esc(p.name) + '" aria-label="删除上游">' + icon("trash-2") + '</button></div></article>';
  }

  function renderProviders() {
    var list = state.providers;
    var available = list.filter(function (p) { return p.available; }).length;
    var creds = list.reduce(function (s, p) { return s + (p.credentials || []).length; }, 0);
    var models = list.reduce(function (s, p) { return s + (p.models || []).length; }, 0);

    el.pages.providers.innerHTML =
      pageHead("HARBOR / UPSTREAM", "上游服务", "连接模型服务、管理凭证与账号配额；模型映射在「模型」页维护。",
        '<button type="button" class="btn ghost" data-act="prov.reload">' + icon("refresh") + '刷新</button>' +
        '<button type="button" class="btn" data-act="prov.create">' + icon("plus") + '连接上游</button>') +
      '<div class="stats">' +
      statCard("server", "可用上游", available + " / " + list.length, "当前可接受请求") +
      statCard("key", "访问凭证", num(creds), "已配置的上游 Key") +
      statCard("layers-3", "模型声明", num(models), "跨上游合计") +
      '</div>' +
      '<div class="provider-grid" data-block>' + (list.length ? list.map(providerCard).join("") :
        emptyState("server", "连接第一个上游", "填写服务地址与访问凭证，就能开始转发请求。",
          '<button type="button" class="btn" data-act="prov.create">' + icon("plus") + '连接上游</button>')) + '</div>';
  }

  function providerForm(p) {
    var creating = !p;
    p = p || {};
    var limits = [["concurrency", "并发请求"], ["rpm", "请求 / 分钟"], ["tpm", "Tokens / 分钟"], ["tph", "Tokens / 小时"], ["tpd", "Tokens / 天"]];
    return '<form data-form="prov.save" data-name="' + esc(p.name || "") + '">' +
      (creating ? '<div class="setting-form"><label class="field"><span>服务名称</span><input class="ctl" name="name" required placeholder="例如 openai"></label>' +
        '<label class="field"><span>接口类型</span><select class="ctl" name="type">' + PROV_TYPES.map(function (t) { return '<option value="' + t + '">' + t + '</option>'; }).join("") + '</select></label></div>' : "") +
      '<label class="field"><span>服务地址</span><input class="ctl" name="base_url" type="url" required placeholder="https://api.openai.com/v1" value="' + esc(p.base_url || "") + '"></label>' +
      '<label class="field"><span>模型范围（可选）</span><input class="ctl" name="models" placeholder="gpt-4o, deepseek-chat" value="' + esc((p.models || []).join(", ")) + '"><span class="hint">逗号分隔；留空表示不限制。也可以在「模型」页逐个登记。</span></label>' +
      (creating ? '<label class="field"><span>访问凭证（可选）</span><input class="ctl" name="credentials" type="password" autocomplete="new-password" placeholder="sk-... "><span class="hint">多个用逗号分隔，也可以稍后添加。</span></label>' : "") +
      '<div class="setting-form">' + limits.map(function (l) {
        return '<label class="field"><span>' + esc(l[1]) + '</span><input class="ctl" name="' + l[0] + '" type="number" min="0" step="1" value="' + esc(String(p[l[0]] || 0)) + '"></label>';
      }).join("") + '</div></form>';
  }

  actions["prov.reload"] = function (btn) {
    busy(btn, async function () { await loadProviders(); });
  };

  actions["prov.create"] = function () {
    openModal({
      title: "连接上游",
      wide: true,
      body: providerForm(null),
      foot: '<button type="button" class="btn ghost" data-act="modal.close">取消</button><button type="button" class="btn" data-act="prov.save">保存</button>'
    });
  };

  actions["prov.edit"] = function (btn) {
    var name = btn.getAttribute("data-name");
    var p = state.providers.filter(function (x) { return x.name === name; })[0];
    if (!p) return;
    openModal({
      title: "编辑 " + name,
      wide: true,
      body: providerForm(p),
      foot: '<button type="button" class="btn ghost" data-act="modal.close">取消</button><button type="button" class="btn" data-act="prov.save">保存</button>'
    });
  };

  actions["prov.save"] = function (btn) {
    var form = $('form[data-form="prov.save"]');
    var name = form.getAttribute("data-name");
    busy(btn, async function () {
      var body = {};
      var base = form.querySelector('[name="base_url"]').value.trim();
      if (!base) throw new Error("请填写服务地址");
      body.base_url = base;
      body.models = (form.querySelector('[name="models"]').value || "").split(/[,，\s]+/).filter(Boolean);
      ["concurrency", "rpm", "tpm", "tph", "tpd"].forEach(function (key) {
        var node = form.querySelector('[name="' + key + '"]');
        if (node) body[key] = Math.max(0, parseInt(node.value, 10) || 0);
      });
      if (name) {
        await api("/api/providers/" + encodeURIComponent(name), { method: "PUT", body: JSON.stringify(body) });
        toast("已保存 " + name);
      } else {
        body.name = form.querySelector('[name="name"]').value.trim();
        body.type = form.querySelector('[name="type"]').value;
        var credInput = form.querySelector('[name="credentials"]');
        var creds = credInput ? credInput.value.split(/[,，\s]+/).filter(Boolean) : [];
        if (!body.name) throw new Error("请填写服务名称");
        if (creds.length) body.credentials = creds;
        await api("/api/providers", { method: "POST", body: JSON.stringify(body) });
        toast("已连接 " + body.name);
      }
      closeModal();
      await loadProviders();
    });
  };

  actions["prov.remove"] = function (btn) {
    var name = btn.getAttribute("data-name");
    confirmAction({
      title: "删除上游",
      text: "删除 “" + name + "” 会一并移除它的凭证，以及引用它的模型映射。",
      okLabel: "删除",
      onOk: async function () {
        await api("/api/providers/" + encodeURIComponent(name), { method: "DELETE" });
        toast("已删除 " + name);
      },
      after: async function () {
        state.models.provider = "";
        await loadProviders();
      }
    });
  };

  forms["prov.credAdd"] = function (form) {
    var name = form.getAttribute("data-name");
    var secret = form.querySelector('[name="secret"]').value.trim();
    if (!secret) return;
    var btn = form.querySelector("button[type=submit]");
    busy(btn, async function () {
      await api("/api/providers/" + encodeURIComponent(name) + "/credentials", { method: "POST", body: JSON.stringify({ secret: secret }) });
      toast("凭证已添加");
      await loadProviders();
    });
  };

  actions["prov.credRemove"] = function (btn) {
    var name = btn.getAttribute("data-name");
    var cred = btn.getAttribute("data-cred");
    confirmAction({
      title: "移除凭证",
      text: "移除 “" + cred + "” 后，该凭证不再参与转发。",
      okLabel: "移除",
      onOk: async function () {
        await api("/api/providers/" + encodeURIComponent(name) + "/credentials/" + encodeURIComponent(cred), { method: "DELETE" });
        toast("凭证已移除");
      },
      after: loadProviders
    });
  };

  /* ================= 模型 ================= */
  function modelCols() {
    try {
      var saved = JSON.parse(localStorage.getItem(COLS_KEY) || "null");
      if (saved) return { ctx: saved.ctx !== false, caps: saved.caps !== false };
    } catch (e) { /* 忽略 */ }
    return { ctx: true, caps: true };
  }

  async function loadModels() {
    var data = await api("/api/providers");
    state.providers = (data.providers || []).slice().sort(function (a, b) { return a.name.localeCompare(b.name); });
    var names = state.providers.map(function (p) { return p.name; });
    var wanted = state.models.provider || localStorage.getItem(PROV_KEY) || "";
    if (names.indexOf(wanted) < 0) wanted = names[0] || "";
    state.models.provider = wanted;
    if (wanted) localStorage.setItem(PROV_KEY, wanted);
    state.models.cols = modelCols();
    state.models.rows = [];
    if (wanted) {
      var detail = await api("/api/providers/" + encodeURIComponent(wanted) + "/models");
      state.models.rows = detail.models || [];
    }
    if (state.models.probe && state.models.probe.provider !== wanted) state.models.probe = null;
    renderModels();
  }

  function modelRow(m) {
    var caps = CAPS.map(function (c) {
      var on = (m.capabilities || []).indexOf(c) >= 0;
      return '<button type="button" class="cap ' + (on ? "on" : "off") + '" data-act="model.cap" data-id="' + esc(m.id) + '" data-cap="' + c + '" title="' + (on ? "移除 " : "添加 ") + esc(c) + '">' + esc(CAP_LABEL[c]) + '</button>';
    }).join("");

    var map = "";
    if ((m.mapped_to || []).length) {
      map = m.mapped_to.map(function (u) {
        return '<button type="button" class="map-chip" data-act="model.unmap" data-id="' + esc(m.id) + '" data-unified="' + esc(u) + '" title="点击取消映射"><span class="u">' + esc(u) + '</span><span class="x">×</span></button>';
      }).join("");
    } else {
      map = '<span class="map-chip plain">默认</span>';
    }
    map += '<button type="button" class="map-add" data-act="model.map" data-id="' + esc(m.id) + '">+ 映射</button>';

    return '<div class="mrow' + (m.enabled ? "" : " is-off") + '">' +
      '<div class="cell name"><button type="button" class="model-name" data-act="model.copy" data-name="' + esc(m.id) + '" title="点击复制模型名">' + esc(m.id) + '</button>' + statusBadge(m.state) + '</div>' +
      '<div class="cell col-ctx">' + ctxSelect(m) + '</div>' +
      '<div class="cell col-caps">' + caps + '</div>' +
      '<div class="cell col-cost"><input class="ctl cost-input" type="number" min="0.05" max="10" step="0.05" data-change="model.cost" data-id="' + esc(m.id) + '" value="' + esc(String(m.concurrency_cost || 1)) + '" aria-label="并发系数" title="每次请求占用的并发槽；默认 1.0"></div>' +
      '<div class="cell">' + map + '</div>' +
      '<div class="cell">' +
      '<label class="switch" title="' + (m.enabled ? "禁用（不参与路由）" : "启用") + '"><input type="checkbox" data-change="model.enabled" data-id="' + esc(m.id) + '"' + (m.enabled ? " checked" : "") + ' aria-label="启用/禁用"><span class="track"></span></label>' +
      '<button type="button" class="icon-btn danger" data-act="model.remove" data-id="' + esc(m.id) + '" aria-label="移除模型">' + icon("trash-2") + '</button></div></div>';
  }

  function renderModels() {
    var m = state.models;
    var rows = m.rows;
    var q = (m.q || "").toLowerCase();
    var shown = rows.filter(function (r) {
      if (m.status === "on" && !r.enabled) return false;
      if (m.status === "off" && r.enabled) return false;
      if (m.view === "visible" && r.state !== "visible") return false;
      if (m.view === "mapped" && r.state !== "mapped") return false;
      if (!q) return true;
      return r.id.toLowerCase().indexOf(q) >= 0 || (r.mapped_to || []).join(" ").toLowerCase().indexOf(q) >= 0;
    });

    var enabled = rows.filter(function (r) { return r.enabled; }).length;
    var mapped = rows.filter(function (r) { return (r.mapped_to || []).length; }).length;

    var side = state.providers.map(function (p) {
      return '<button type="button" class="provider-item" data-act="model.pick" data-name="' + esc(p.name) + '" aria-current="' + (p.name === m.provider) + '">' +
        '<span class="name">' + esc(p.name) + '</span><span class="count">' + (p.models || []).length + '</span>' +
        '<span class="type">' + esc(p.type) + '</span></button>';
    }).join("") || '<p class="hint">还没有上游</p>';

    var head = '<div class="mtable-head"><span>上游模型</span><span class="col-ctx">上下文</span><span class="col-caps">能力</span><span class="col-cost">并发</span><span>模型映射</span><span></span></div>';
    var body = shown.length ? shown.map(modelRow).join("") :
      '<div class="empty"><div class="box">' + icon("layers-3") + '</div><strong>' + (rows.length ? "当前筛选没有结果" : "还没有声明模型") + '</strong>' +
      '<p>' + (rows.length ? "换个筛选条件，或清空搜索。" : "点「获取模型」从上游拉取清单，或手动添加一个。") + '</p></div>';

    el.pages.models.innerHTML =
      pageHead("HARBOR / MODELS", "模型管理", "以 Provider 为轴、以上游模型为行：启用、并发系数与统一名映射都在这里。",
        '<button type="button" class="btn ghost" data-act="model.probe"' + (m.provider ? "" : " disabled") + '>' + icon("zap") + '获取模型</button>' +
        '<button type="button" class="btn ghost" data-act="model.add"' + (m.provider ? "" : " disabled") + '>' + icon("plus") + '添加模型</button>' +
        '<button type="button" class="btn ghost" data-act="model.catalog"' + (m.provider ? "" : " disabled") + '>' + icon("database") + '模型元数据</button>' +
        '<div class="picker"><button type="button" class="btn ghost" data-act="model.cols">' + icon("menu") + '列设置</button></div>') +
      '<div class="models' + (m.cols.ctx ? "" : " hide-ctx") + (m.cols.caps ? "" : " hide-caps") + '">' +
      '<aside class="provider-list">' + side + '</aside>' +
      '<section class="card"><div class="model-toolbar">' +
      seg("model.status", m.status, [["all", "全部"], ["on", "已启用"], ["off", "已禁用"]]) +
      seg("model.view", m.view, [["all", "全部"], ["visible", "可见"], ["mapped", "有映射"]]) +
      '<div class="search">' + icon("search") + '<input class="ctl" id="model-q" placeholder="搜索模型 / 映射名" value="' + esc(m.q) + '"></div>' +
      '<span class="hint">' + (m.provider ? "已声明 " + rows.length + " · 启用 " + enabled + " · 映射 " + mapped : "先选择上游") + '</span>' +
      '</div>' + head + body + '</section></div>';

    var input = $("#model-q");
    if (input) {
      input.oninput = function () {
        m.q = input.value;
        renderModels();
        var again = $("#model-q");
        if (again) {
          again.focus();
          var v = again.value;
          again.value = "";
          again.value = v;
        }
      };
    }
  }

  var CTX_OPTIONS = [[0, "—"], [32768, "32K"], [65536, "64K"], [131072, "128K"], [200000, "200K"], [262144, "256K"], [524288, "512K"], [1048576, "1M"]];

  function ctxSelect(m) {
    var current = m.context || 0;
    var known = CTX_OPTIONS.some(function (o) { return o[0] === current; });
    var html = '<select class="ctx-input" data-change="model.ctx" data-id="' + esc(m.id) + '" aria-label="上下文窗口">';
    CTX_OPTIONS.forEach(function (o) {
      html += '<option value="' + o[0] + '"' + (o[0] === current ? " selected" : "") + ">" + esc(o[1]) + "</option>";
    });
    if (!known && current > 0) {
      html += '<option value="' + current + '" selected>' + num(current) + "</option>";
    }
    html += '<option value="custom">自定义…</option></select>';
    return html;
  }

  function renderCatalog() {
    var data = state.catalog || {};
    var matches = data.matches || [];
    var rows = matches.map(function (item, index) {
      var m = item.model;
      if (!m) {
        return '<tr><td class="mono">' + esc(item.requested) + '</td><td colspan="3" class="hint">目录中未找到</td><td></td></tr>';
      }
      var caps = (m.capabilities || []).map(function (c) {
        return '<span class="cap on">' + esc(CAP_LABEL[c] || c) + "</span>";
      }).join("");
      return '<tr><td class="mono">' + esc(item.requested) + "</td>" +
        '<td class="mono">' + esc(m.id) + (m.provider ? '<small class="req-sub">' + esc(m.provider) + "</small>" : "") + "</td>" +
        '<td class="num">' + (m.context ? num(m.context) : "—") + "</td>" +
        "<td>" + (caps || '<span class="muted">—</span>') + "</td>" +
        '<td class="actions"><button type="button" class="btn ghost sm" data-act="model.catalogApply" data-index="' + index + '">应用</button></td></tr>';
    }).join("");
    var box = $("#catalog-body");
    if (!box) return;
    box.innerHTML = (data.error ? '<div class="note">目录拉取异常：' + esc(data.error) + "</div>" : "") +
      '<p class="hint">已声明 ' + matches.length + " 个模型 · 目录 " + num(data.total || 0) + " 条。应用会写入上下文窗口，并合并能力（不删除已有能力）。</p>" +
      '<div class="table-wrap"><table class="table"><thead><tr><th>模型</th><th>目录匹配</th><th class="num">上下文</th><th>能力</th><th></th></tr></thead><tbody>' +
      (rows || '<tr><td colspan="5" class="hint">没有可匹配的模型</td></tr>') + "</tbody></table></div>";
    var all = $("#catalog-apply-all");
    if (all) all.disabled = !matches.some(function (x) { return x.model; });
  }

  async function applyCatalog(indexes) {
    var matches = (state.catalog && state.catalog.matches) || [];
    var map = {};
    indexes.forEach(function (i) {
      var item = matches[i];
      if (item && item.model) map[item.requested] = item.model;
    });
    if (!Object.keys(map).length) return 0;
    var rows = state.models.rows.map(function (r) {
      var m = map[r.id];
      if (!m) return r;
      var caps = (r.capabilities || []).slice();
      (m.capabilities || []).forEach(function (c) { if (caps.indexOf(c) < 0) caps.push(c); });
      return Object.assign({}, r, { context: m.context || r.context, capabilities: caps });
    });
    await saveModels(rows);
    return Object.keys(map).length;
  }

  actions["model.catalog"] = function () {
    var rows = state.models.rows;
    if (!rows.length) { toast("先给这个上游添加模型", true); return; }
    var ids = rows.map(function (r) { return r.id; }).join(",");
    state.catalog = null;
    openModal({
      title: "模型元数据 · " + state.models.provider,
      wide: true,
      body: '<div id="catalog-body"><p class="hint">正在从 models.dev 读取模型目录…</p></div>',
      foot: '<button type="button" class="btn ghost" data-act="modal.close">取消</button><button type="button" class="btn" id="catalog-apply-all" disabled>全部应用</button>'
    });
    api("/api/model-catalog?ids=" + encodeURIComponent(ids)).then(function (data) {
      state.catalog = data;
      renderCatalog();
    }).catch(function (e) {
      var box = $("#catalog-body");
      if (box) box.innerHTML = '<div class="note">读取失败：' + esc(e.message) + "</div>";
    });
  };

  actions["model.catalogApply"] = function (btn) {
    busy(btn, async function () {
      var n = await applyCatalog([Number(btn.getAttribute("data-index"))]);
      if (n) toast("已应用 " + n + " 个模型的元数据");
    });
  };

  function findModel(id) {
    return state.models.rows.filter(function (r) { return r.id === id; })[0];
  }

  // 复制模型名：优先 Clipboard API，失败时回退到隐藏 textarea + execCommand。
  function copyText(text) {
    if (navigator.clipboard && navigator.clipboard.writeText) {
      return navigator.clipboard.writeText(text);
    }
    return new Promise(function (resolve, reject) {
      var el = document.createElement("textarea");
      el.className = "copy-fallback";
      el.value = text;
      el.setAttribute("readonly", "");
      document.body.appendChild(el);
      el.select();
      var ok = false;
      try { ok = document.execCommand("copy"); } catch (e) { /* 忽略 */ }
      el.remove();
      ok ? resolve() : reject(new Error("复制失败"));
    });
  }

  actions["model.copy"] = function (btn) {
    var name = btn.getAttribute("data-name");
    copyText(name).then(function () {
      toast("已复制模型名：" + name);
    }).catch(function (e) {
      toast(e.message, true);
    });
  };

  async function saveModels(rows) {
    var payload = rows.map(function (r) {
      return { id: r.id, enabled: !!r.enabled, context: Number(r.context) || 0, capabilities: r.capabilities || [], concurrency_cost: Number(r.concurrency_cost) || 1 };
    });
    var data = await api("/api/providers/" + encodeURIComponent(state.models.provider) + "/models", {
      method: "PUT", body: JSON.stringify({ models: payload })
    });
    state.models.rows = data.models || [];
    renderModels();
  }

  function mutateModel(id, fn) {
    var rows = state.models.rows.map(function (r) {
      if (r.id !== id) return r;
      var copy = Object.assign({}, r, { capabilities: (r.capabilities || []).slice(), mapped_to: (r.mapped_to || []).slice() });
      fn(copy);
      return copy;
    });
    return saveModels(rows);
  }

  actions["model.pick"] = function (btn) {
    var name = btn.getAttribute("data-name");
    if (name === state.models.provider) return;
    state.models.provider = name;
    state.models.probe = null;
    state.models.q = "";
    localStorage.setItem(PROV_KEY, name);
    loadModels().catch(function (e) { toast(e.message, true); });
  };

  actions["model.status"] = function (btn) { state.models.status = btn.getAttribute("data-val"); renderModels(); };
  actions["model.view"] = function (btn) { state.models.view = btn.getAttribute("data-val"); renderModels(); };

  changes["model.enabled"] = function (input) {
    mutateModel(input.getAttribute("data-id"), function (r) { r.enabled = input.checked; })
      .catch(function (e) { toast(e.message, true); loadModels(); });
  };

  changes["model.cost"] = function (input) {
    var value = parseFloat(input.value);
    if (!isFinite(value) || value <= 0 || value > 1000) {
      toast("并发系数需大于 0 且不超过 1000", true);
      loadModels();
      return;
    }
    mutateModel(input.getAttribute("data-id"), function (r) { r.concurrency_cost = value; })
      .then(function () { toast("并发系数已更新"); })
      .catch(function (e) { toast(e.message, true); loadModels(); });
  };

  changes["model.ctx"] = function (node) {
    if (node.tagName === "SELECT") {
      if (node.value === "custom") {
        var input = document.createElement("input");
        input.className = "ctx-input";
        input.type = "number";
        input.min = "0";
        input.step = "1000";
        input.placeholder = "token 数";
        input.setAttribute("data-change", "model.ctx");
        input.setAttribute("data-id", node.getAttribute("data-id"));
        node.replaceWith(input);
        input.focus();
        return;
      }
      var picked = Math.max(0, parseInt(node.value, 10) || 0);
      mutateModel(node.getAttribute("data-id"), function (r) { r.context = picked; })
        .catch(function (e) { toast(e.message, true); loadModels(); });
      return;
    }
    var value = Math.max(0, parseInt(node.value, 10) || 0);
    mutateModel(node.getAttribute("data-id"), function (r) { r.context = value; })
      .catch(function (e) { toast(e.message, true); loadModels(); });
  };

  actions["model.cap"] = function (btn) {
    var id = btn.getAttribute("data-id");
    var cap = btn.getAttribute("data-cap");
    mutateModel(id, function (r) {
      var idx = r.capabilities.indexOf(cap);
      if (idx >= 0) r.capabilities.splice(idx, 1); else r.capabilities.push(cap);
    }).catch(function (e) { toast(e.message, true); loadModels(); });
  };

  actions["model.remove"] = function (btn) {
    var id = btn.getAttribute("data-id");
    var row = findModel(id) || {};
    var mapped = row.mapped_to || [];
    confirmAction({
      title: "移除模型",
      text: "从 “" + id + "” 的声明中移除" + (mapped.length ? "，并解除 " + mapped.join("、") + " 的映射" : "") + "？",
      okLabel: "移除",
      onOk: async function () {
        await saveModels(state.models.rows.filter(function (r) { return r.id !== id; }));
        for (var i = 0; i < mapped.length; i++) {
          await api("/api/model-map", { method: "POST", body: JSON.stringify({ provider: state.models.provider, upstream_model: id, unified: mapped[i], action: "remove" }) });
        }
      },
      after: loadModels
    });
  };

  actions["model.map"] = function (btn) {
    var id = btn.getAttribute("data-id");
    openModal({
      title: "映射到统一模型名",
      body: '<p class="modal-intro mono">' + esc(id) + '</p>' +
        '<label class="field"><span>统一模型名</span><input class="ctl" id="map-unified" placeholder="例如 deepseek-v4.1-flash"><span class="hint">客户端用这个名字调用；同名路由已存在时会追加该上游。</span></label>',
      foot: '<button type="button" class="btn ghost" data-act="modal.close">取消</button><button type="button" class="btn" id="map-save">保存映射</button>',
      onMount: function () {
        $("#map-save").onclick = function () {
          var save = this;
          busy(save, async function () {
            var unified = $("#map-unified").value.trim();
            if (!unified) throw new Error("请填写统一模型名");
            await api("/api/model-map", { method: "POST", body: JSON.stringify({ provider: state.models.provider, upstream_model: id, unified: unified, action: "add" }) });
            closeModal();
            toast("已映射 " + id + " → " + unified);
            await loadModels();
          });
        };
      }
    });
  };

  actions["model.unmap"] = function (btn) {
    var id = btn.getAttribute("data-id");
    var unified = btn.getAttribute("data-unified");
    confirmAction({
      title: "取消映射",
      text: "把 “" + id + "” 从统一模型 “" + unified + "” 中移除？该统一名若没有其它上游会被一起删掉。",
      okLabel: "取消映射",
      onOk: async function () {
        await api("/api/model-map", { method: "POST", body: JSON.stringify({ provider: state.models.provider, upstream_model: id, unified: unified, action: "remove" }) });
        toast("已取消映射");
      },
      after: loadModels
    });
  };

  actions["model.cols"] = function (btn) {
    var wrap = btn.parentElement;
    var exist = $(".popover", wrap);
    if (exist) { exist.remove(); return; }
    var cols = modelCols();
    var pop = document.createElement("div");
    pop.className = "popover";
    pop.innerHTML = '<label class="check"><input type="checkbox" id="col-ctx"' + (cols.ctx ? " checked" : "") + '>上下文</label>' +
      '<label class="check"><input type="checkbox" id="col-caps"' + (cols.caps ? " checked" : "") + '>能力</label>';
    wrap.appendChild(pop);
    function apply() {
      var next = { ctx: $("#col-ctx").checked, caps: $("#col-caps").checked };
      localStorage.setItem(COLS_KEY, JSON.stringify(next));
      state.models.cols = next;
      pop.remove();
      renderModels();
    }
    $("#col-ctx").onchange = apply;
    $("#col-caps").onchange = apply;
  };

  actions["model.add"] = function () {
    var p = state.providers.filter(function (x) { return x.name === state.models.provider; })[0] || {};
    var base = String(p.base_url || "").replace(/\/+$/, "");
    openModal({
      title: "添加自定义模型",
      body: '<label class="field"><span>提供商</span><input class="ctl" value="' + esc(p.name || "") + '" disabled></label>' +
        '<label class="field"><span>上游模型 ID</span><input class="ctl mono" id="add-model-id" placeholder="例如 gpt-5.5-preview"><span class="hint">必须和上游 /models 返回的名称一致。</span></label>' +
        '<label class="field"><span>请求协议</span><select class="ctl" disabled><option>跟随提供方默认（OpenAI · Chat Completions）</option></select></label>' +
        '<label class="field"><span>上下文窗口（可选）</span><input class="ctl" id="add-model-ctx" type="number" min="0" step="1000" placeholder="例如 128000"></label>' +
        '<div class="note">在 <b>' + esc(p.name || "") + '</b> 上登记上游模型 ID；登记后默认启用并对外可见，需要改名时再建映射。' +
        (base ? '<br>请求地址：<b class="mono">' + esc(base + "/chat/completions") + '</b>' : "") + '</div>',
      foot: '<button type="button" class="btn ghost" data-act="modal.close">取消</button><button type="button" class="btn" id="add-model-save">保存</button>',
      onMount: function () {
        $("#add-model-save").onclick = function () {
          var save = this;
          busy(save, async function () {
            var id = $("#add-model-id").value.trim();
            if (!id) throw new Error("请填写上游模型 ID");
            if (findModel(id)) throw new Error("该模型已存在");
            var ctx = Math.max(0, parseInt($("#add-model-ctx").value, 10) || 0);
            var rows = state.models.rows.concat([{ id: id, enabled: true, context: ctx, capabilities: ["chat"], concurrency_cost: 1, mapped_to: [] }]);
            await saveModels(rows);
            closeModal();
            toast("已添加 " + id + "（默认启用）");
          });
        };
      }
    });
  };

  actions["model.probe"] = function () {
    state.models.pick = {};
    state.models.pickQ = "";
    openModal({
      title: "获取模型 — " + state.models.provider,
      wide: true,
      body: '<div class="picker" id="probe-wrap"><p class="hint">正在向上游拉取模型列表…</p></div>',
      foot: '<button type="button" class="btn ghost" data-act="modal.close">取消</button><button type="button" class="btn" id="probe-import" disabled>导入</button>',
      onMount: function () { runProbe(); }
    });
  };

  async function runProbe() {
    var wrap = $("#probe-wrap");
    if (!wrap) return;
    wrap.innerHTML = '<p class="hint">正在向上游拉取模型列表…</p>';
    try {
      var data = await api("/api/providers/" + encodeURIComponent(state.models.provider) + "/models/probe", { method: "POST", body: "{}" });
      state.models.probe = { provider: state.models.provider, models: data.models || [], declared: data.declared || [] };
      renderProbe();
    } catch (e) {
      wrap.innerHTML = '<div class="note">拉取失败：' + esc(e.message) + '</div>';
    }
  }

  function renderProbe() {
    var wrap = $("#probe-wrap");
    if (!wrap) return;
    var probe = state.models.probe || { models: [], declared: [] };
    var declared = {};
    (probe.declared || []).forEach(function (id) { declared[id] = true; });
    var q = (state.models.pickQ || "").toLowerCase();
    var list = (probe.models || []).filter(function (id) { return !q || id.toLowerCase().indexOf(q) >= 0; });
    var pick = state.models.pick;
    var picked = Object.keys(pick).filter(function (k) { return pick[k]; });

    wrap.innerHTML =
      '<div class="picker-head"><span class="picker-count">已选 ' + picked.length + ' 个 · 上游共 ' + (probe.models || []).length + ' 个 · 清单已有 ' + (probe.declared || []).length + ' 个</span>' +
      '<div class="search">' + icon("search") + '<input class="ctl" id="probe-q" placeholder="搜索模型 ID…" value="' + esc(state.models.pickQ) + '"></div>' +
      '<button type="button" class="btn ghost sm" data-act="probe.all">全选未添加</button>' +
      '<button type="button" class="btn ghost sm" data-act="probe.clear">清空</button>' +
      '<button type="button" class="btn ghost sm" data-act="probe.reload">' + icon("refresh") + '重新获取</button></div>' +
      '<div class="pick-list"><div class="pick-row head"><span></span><span>上游模型</span><span>状态</span></div>' +
      (list.map(function (id) {
        var added = !!declared[id];
        return '<label class="pick-row' + (added ? " is-added" : "") + '"><input type="checkbox" class="check-box" data-change="probe.pick" data-id="' + esc(id) + '"' + (added ? " disabled" : (pick[id] ? " checked" : "")) + '>' +
          '<span class="name">' + esc(id) + '</span><span>' + (added ? '<span class="badge ok">已添加</span>' : '<span class="badge">未添加</span>') + '</span></label>';
      }).join("") || '<div class="pick-row"><span></span><span class="hint">没有匹配的模型</span><span></span></div>') + '</div>';

    var q2 = $("#probe-q");
    if (q2) q2.oninput = function () { state.models.pickQ = q2.value; renderProbe(); };
    var imp = $("#probe-import");
    if (imp) {
      imp.disabled = picked.length === 0;
      imp.textContent = picked.length ? "导入 " + picked.length + " 个" : "导入";
    }
  }

  changes["probe.pick"] = function (input) {
    state.models.pick[input.getAttribute("data-id")] = input.checked;
    renderProbe();
  };

  actions["probe.all"] = function () {
    var probe = state.models.probe || { models: [], declared: [] };
    var declared = {};
    (probe.declared || []).forEach(function (id) { declared[id] = true; });
    (probe.models || []).forEach(function (id) { if (!declared[id]) state.models.pick[id] = true; });
    renderProbe();
  };

  actions["probe.clear"] = function () {
    state.models.pick = {};
    renderProbe();
  };

  actions["probe.reload"] = function () { runProbe(); };

  /* 导入按钮在 modal-foot，随弹窗创建，用委托绑定。 */
  $("#modal-foot").addEventListener("click", function (ev) {
    var all = ev.target.closest("#catalog-apply-all");
    if (all) {
      var matches = (state.catalog && state.catalog.matches) || [];
      var idx = [];
      matches.forEach(function (m, i) { if (m.model) idx.push(i); });
      busy(all, async function () {
        var n = await applyCatalog(idx);
        if (n) { toast("已应用 " + n + " 个模型的元数据"); closeModal(); }
      });
      return;
    }
    var btn = ev.target.closest("#probe-import");
    if (!btn) return;
    var ids = Object.keys(state.models.pick).filter(function (k) { return state.models.pick[k]; });
    if (!ids.length) return;
    busy(btn, async function () {
      var rows = state.models.rows.slice();
      ids.forEach(function (id) {
        if (rows.some(function (r) { return r.id === id; })) return;
        rows.push({ id: id, enabled: true, context: 0, capabilities: ["chat"], concurrency_cost: 1, mapped_to: [] });
      });
      await saveModels(rows);
      closeModal();
      toast("已导入 " + ids.length + " 个模型（默认启用）");
    });
  });

  /* ================= 设置 ================= */
  async function loadSettings() {
    state.settings = await api("/api/settings");
    renderSettings();
  }

  function keepaliveFrame(style, comment) {
    if (style === "event") return 'data: {"type":"keepalive"}\n\n';
    var text = String(comment || "").trim() || "keepalive";
    return ": " + text + "\n\n";
  }

  var SETTING_GROUPS = [
    ["queue", "排队", "队列容量与等待上限；保存后对后续请求立即生效。"],
    ["limits", "超时与重试", "请求总超时、上游尝试次数与退避/冷却。"],
    ["keepalive", "心跳保活", "SSE / 流式响应静默时补空事件；WebSocket 不在本网关范围。"]
  ];

  function settingInput(f) {
    var id = "set-" + f.key;
    var label = "<span>" + esc(f.label) + "</span>";
    var hint = f.note ? '<span class="hint">' + esc(f.note) + "</span>" : "";
    if (f.kind === "bool") {
      return '<div class="field"><label class="switch"><input type="checkbox" id="' + id + '" data-key="' + esc(f.key) + '" data-kind="bool"' + (f.value ? " checked" : "") + '><span class="track"></span><span>' + esc(f.label) + "</span></label>" + hint + "</div>";
    }
    if (f.kind === "text" && f.key === "keepalive_style") {
      return '<label class="field">' + label + '<select class="ctl" id="' + id + '" data-key="' + esc(f.key) + '" data-kind="text">' +
        '<option value="comment"' + (f.value === "comment" ? " selected" : "") + ">SSE 注释行（推荐）</option>" +
        '<option value="event"' + (f.value === "event" ? " selected" : "") + ">data 事件（type=keepalive）</option></select>" + hint + "</label>";
    }
    var value = f.kind === "seconds" ? Math.round(Number(f.value) / 1000) : f.value;
    var unit = f.kind === "seconds" ? "秒" : (f.kind === "ms" ? "毫秒" : "");
    var input = '<input class="ctl" id="' + id + '" data-key="' + esc(f.key) + '" data-kind="' + esc(f.kind) + '" type="' +
      (f.kind === "text" ? "text" : "number") + '" min="0" step="1" value="' + esc(String(value == null ? "" : value)) + '">';
    return '<label class="field">' + label +
      (unit ? '<span class="inline-unit">' + input + "<span>" + esc(unit) + "</span></span>" : input) + hint + "</label>";
  }

  function renderSettings() {
    var s = state.settings || {};
    var fields = s.fields || [];
    var env = s.env || [];
    var groups = {};
    fields.filter(function (f) { return f.editable; }).forEach(function (f) {
      (groups[f.group] = groups[f.group] || []).push(f);
    });
    var readonly = fields.filter(function (f) { return !f.editable; });

    var editable = SETTING_GROUPS.map(function (g) {
      var list = groups[g[0]] || [];
      if (!list.length) return "";
      return '<section class="setting-group"><h4>' + esc(g[1]) + '</h4><p class="hint">' + esc(g[2]) + '</p>' +
        '<div class="setting-form">' + list.map(settingInput).join("") + "</div></section>";
    }).join("");

    var ro = readonly.map(function (f) {
      var v = f.value == null || f.value === "" ? "—" : f.value;
      return "<div><span>" + esc(f.label) + "</span><strong>" + esc(v) + "</strong></div>";
    }).join("");

    var envRows = env.map(function (e) {
      return '<tr><td class="mono">' + esc(e.name) + '</td><td class="mono">' + esc(e.value) + "</td><td>" +
        (e.set ? '<span class="badge ok">已设置</span>' : '<span class="badge">未设置</span>') + "</td></tr>";
    }).join("");

    el.pages.settings.innerHTML =
      pageHead("HARBOR / SETTINGS", "设置", "可热改的参数保存后立即生效并落库；只读项与环境变量用于排障。",
        '<button type="button" class="btn ghost" data-act="settings.reload">' + icon("refresh") + "刷新</button>") +
      '<div class="settings">' +
      '<section class="card"><div class="card-head"><div><h3>运行时参数</h3><p>改动立即作用于排队、超时与心跳，并写入 settings 表。</p></div>' +
      (s.persistent ? '<span class="badge ok">持久化</span>' : '<span class="badge warn">内存模式</span>') + "</div>" +
      '<div class="card-body">' + editable +
      '<div class="preview" id="ka-preview"></div>' +
      '<div class="setting-foot"><button type="button" class="btn" id="settings-save">保存设置</button></div></div></section>' +
      '<section class="card"><div class="card-head"><div><h3>只读配置</h3><p>来自 harbor.yaml / 环境变量；修改需要改配置并重启。</p></div></div>' +
      '<div class="card-body"><div class="kv">' + ro + "</div></div></section>" +
      '<section class="card"><div class="card-head"><div><h3>环境变量</h3><p>敏感值只显示是否已设置；完整配置可通过 HARBOR_CONFIG 注入。</p></div></div>' +
      '<div class="table-wrap"><table class="table"><thead><tr><th>变量</th><th>值</th><th>状态</th></tr></thead><tbody>' + envRows + "</tbody></table></div></section>" +
      "</div>";

    var styleNode = $("#set-keepalive_style");
    var commentNode = $("#set-keepalive_comment");
    function syncPreview() {
      var pre = $("#ka-preview");
      if (!pre) return;
      pre.textContent = "心跳帧预览：\n" + keepaliveFrame(styleNode ? styleNode.value : "comment", commentNode ? commentNode.value : "");
    }
    if (styleNode) {
      styleNode.onchange = function () {
        if (commentNode) commentNode.disabled = styleNode.value === "event";
        syncPreview();
      };
    }
    if (commentNode) {
      commentNode.oninput = syncPreview;
      commentNode.disabled = !!styleNode && styleNode.value === "event";
    }
    syncPreview();
    $("#settings-save").onclick = function () {
      var btn = this;
      busy(btn, saveSettings);
    };
  }

  async function saveSettings() {
    var payload = { keepalive: {} };
    $$("[data-key]", el.pages.settings).forEach(function (node) {
      var key = node.getAttribute("data-key");
      var kind = node.getAttribute("data-kind");
      var v;
      if (kind === "bool") v = node.checked;
      else if (kind === "seconds") v = Math.round(Number(node.value) * 1000);
      else if (kind === "ms" || kind === "int") v = Math.round(Number(node.value));
      else v = node.value;
      if (key.indexOf("keepalive_") === 0) payload.keepalive[key.slice("keepalive_".length)] = v;
      else payload[key] = v;
    });
    var data = await api("/api/settings", { method: "PUT", body: JSON.stringify(payload) });
    state.settings = Object.assign({}, state.settings, { fields: data.fields, keepalive: data.keepalive });
    renderSettings();
    toast("设置已保存并生效");
  }

  actions["settings.reload"] = function (btn) {
    busy(btn, async function () { await loadSettings(); });
  };

  /* ================= 路由 / 登录 ================= */
  function setTab(tab) {
    if (!TABS[tab]) tab = "overview";
    state.tab = tab;
    if (location.hash !== "#/" + tab) location.hash = "#/" + tab;
    el.title.textContent = TABS[tab];
    $$("button[data-tab]", el.nav).forEach(function (btn) {
      var on = btn.getAttribute("data-tab") === tab;
      btn.classList.toggle("active", on);
      if (on) btn.setAttribute("aria-current", "page"); else btn.removeAttribute("aria-current");
    });
    Object.keys(el.pages).forEach(function (name) { el.pages[name].hidden = name !== tab; });
    el.rail.classList.remove("open");
    el.menu.setAttribute("aria-expanded", "false");
    loadTab(tab);
  }

  function loadTab(tab) {
    var run = {
      overview: loadOverview,
      queue: loadQueue,
      requests: loadRequests,
      keys: loadKeys,
      providers: loadProviders,
      models: loadModels,
      settings: loadSettings
    }[tab];
    if (!run) return;
    run().catch(function (e) { toast(e.message || "加载失败", true); });
  }

  el.nav.addEventListener("click", function (ev) {
    var btn = ev.target.closest("button[data-tab]");
    if (btn) setTab(btn.getAttribute("data-tab"));
  });

  el.menu.onclick = function () {
    var open = el.rail.classList.toggle("open");
    el.menu.setAttribute("aria-expanded", String(open));
  };

  el.logout.onclick = logout;
  el.modalClose.onclick = closeModal;

  el.modal.addEventListener("click", function (ev) {
    if (ev.target === el.modal) closeModal();
  });

  document.addEventListener("keydown", function (ev) {
    if (ev.key === "Escape") {
      if (!el.modal.hidden) { closeModal(); return; }
      if (el.rail.classList.contains("open")) { el.rail.classList.remove("open"); el.menu.setAttribute("aria-expanded", "false"); }
    }
  });

  window.addEventListener("hashchange", function () {
    var tab = (location.hash || "").replace(/^#\//, "") || "overview";
    if (!el.app.hidden && tab !== state.tab) setTab(tab);
  });

  async function verifyToken(token) {
    var res = await fetch("/api/stats", { headers: { Authorization: "Bearer " + token }, cache: "no-store" });
    return res.status === 200;
  }

  function logout() {
    state.token = "";
    sessionStorage.removeItem(TOKEN_KEY);
    clearInterval(state.pollTimer);
    location.reload();
  }

  function startPolling() {
    clearInterval(state.pollTimer);
    state.pollTimer = setInterval(function () {
      if (document.hidden || !el.modal.hidden) return;
      if (state.tab === "requests") {
        var active = document.activeElement;
        if (active && active.id === "req-q") return;
        loadRequests().catch(function (e) { toast(e.message, true); });
        return;
      }
      if (state.tab === "overview" || state.tab === "queue") loadTab(state.tab);
    }, 2000);
    clearInterval(state.clockTimer);
    state.clockTimer = setInterval(tickClock, 1000);
    tickClock();
  }

  function tickClock() {
    var d = new Date();
    function p(n) { return String(n).padStart(2, "0"); }
    el.clock.textContent = p(d.getHours()) + ":" + p(d.getMinutes()) + ":" + p(d.getSeconds());
  }

  function enter() {
    el.login.hidden = true;
    el.app.hidden = false;
    startPolling();
    setTab((location.hash || "").replace(/^#\//, "") || "overview");
  }

  el.loginForm.addEventListener("submit", function (ev) {
    ev.preventDefault();
    var token = el.loginToken.value.trim();
    if (!token) { el.loginErr.textContent = "请输入管理 token"; return; }
    el.loginErr.textContent = "正在校验…";
    el.loginBtn.disabled = true;
    verifyToken(token).then(function (ok) {
      if (!ok) { el.loginErr.textContent = "token 无效"; el.loginBtn.disabled = false; return; }
      sessionStorage.setItem(TOKEN_KEY, token);
      state.token = token;
      el.loginErr.textContent = "";
      el.loginBtn.disabled = false;
      enter();
    }).catch(function () {
      el.loginErr.textContent = "无法连接网关";
      el.loginBtn.disabled = false;
    });
  });

  (function boot() {
    var token = sessionStorage.getItem(TOKEN_KEY) || "";
    if (!token) return;
    state.token = token;
    verifyToken(token).then(function (ok) {
      if (ok) enter(); else { sessionStorage.removeItem(TOKEN_KEY); state.token = ""; }
    }).catch(function () { el.loginErr.textContent = "无法连接网关"; });
  })();
})();