"""Real Chromium regression for the embedded admin UI, isolated from live data.

Run: python tools/admin-smoke.py
Requires Python Playwright + Chromium. Builds and starts a disposable Go server.
CRUD uses the real local API; queue/request/probe fixtures exercise UI-only states.
Screenshots and report: .snow/admin-review (no real credentials or production data).
"""
import json
import os
from pathlib import Path
import socket
import subprocess
import tempfile
import time
from urllib.request import urlopen
from playwright.sync_api import sync_playwright, expect

ROOT = Path(__file__).resolve().parent.parent
OUT = ROOT / ".snow/admin-review"


def free_port():
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def check_page(page, tab, width):
    page.wait_for_function("t => !document.getElementById('tab-' + t).hasAttribute('aria-busy')", arg=tab)
    errors = page.evaluate(r"""() => {
      const root = document.querySelector('.tab:not([hidden])');
      const missing = [...root.querySelectorAll('use')].filter(n => !document.querySelector(n.getAttribute('href'))).length;
      const unnamed = [...root.querySelectorAll('button')].filter(n => !n.textContent.trim() && !n.getAttribute('aria-label')).length;
      const unlabelled = [...root.querySelectorAll('input,select')].filter(n => !n.labels.length && !n.getAttribute('aria-label')).length;
      const emoji = /\p{Extended_Pictographic}/u.test(root.innerText);
      return {missing, unnamed, unlabelled, emoji, inline: root.querySelectorAll('[style], [onclick]').length,
        overflow: document.documentElement.scrollWidth > innerWidth + 1};
    }""")
    assert not any(errors.values()), (tab, width, errors)


def navigate(page, tab):
    page.evaluate("t => document.querySelector('[data-tab=' + t + ']').click()", tab)
    page.wait_for_selector(f"#tab-{tab} .page-shell" if tab != "overview" else ".overview-shell")
    page.wait_for_function("t => !document.getElementById('tab-' + t).hasAttribute('aria-busy')", arg=tab)


def run():
    OUT.mkdir(parents=True, exist_ok=True)
    report = {"checks": [], "screenshots": []}
    with tempfile.TemporaryDirectory(prefix="harbor-admin-") as temp:
        temp = Path(temp)
        exe = temp / ("harbor.exe" if os.name == "nt" else "harbor")
        subprocess.run(["go", "build", "-buildvcs=false", "-o", str(exe), "."], cwd=ROOT, check=True)
        port = free_port()
        base = f"http://127.0.0.1:{port}"
        config = f'''server:
  listen: "127.0.0.1:{port}"
queue:
  max_depth: 200
providers:
  - name: qa-main
    type: openai-compatible
    base_url: https://ui-test.example/v1
    models: [gpt-4o, deepseek-chat]
    credentials: [ui-test-credential]
    concurrency: 4
    rpm: 60
  - name: qa-backup
    type: openai-compatible
    base_url: https://backup.example/v1
    models: [gpt-4o]
    credentials: [ui-backup-credential]
keys:
  - secret: hr-ui-test-seed-key
admin:
  token: ui-test-admin-token
storage:
  db_path: {json.dumps(str(temp / 'test.db'))}
  key_path: {json.dumps(str(temp / 'test.key'))}
log:
  level: error
'''
        env = dict(os.environ, HARBOR_CONFIG=config)
        with (temp / "server.log").open("w") as log:
            server = subprocess.Popen([str(exe)], cwd=temp, env=env, stdout=log, stderr=log)
            try:
                for _ in range(100):
                    if server.poll() is not None:
                        raise RuntimeError((temp / "server.log").read_text())
                    try:
                        urlopen(base + "/harbor-console", timeout=.5).close()
                        break
                    except OSError:
                        time.sleep(.1)
                else:
                    raise RuntimeError("Local test server did not start")
                with sync_playwright() as p:
                    browser = p.chromium.launch()
                    context = browser.new_context(viewport={"width": 1440, "height": 1000}, permissions=["clipboard-read", "clipboard-write"])
                    page = context.new_page()
                    js_errors, csp_errors = [], []
                    page.on("pageerror", lambda e: js_errors.append(str(e)))
                    page.on("console", lambda m: csp_errors.append(m.text) if "Content Security Policy" in m.text else None)
                    # Capture the actual poll callback to test it deterministically without sleeping.
                    page.add_init_script("""const originalInterval = window.setInterval;
                      window.setInterval = function(fn, ms, ...args) {
                        if (ms === 5000) window.testPoll = fn;
                        return originalInterval(fn, ms, ...args);
                      };""")
                    page.goto(base + "/harbor-console#/queue")
                    page.locator("#token-input").fill("wrong-token")
                    page.locator("#login-btn").click()
                    expect(page.locator("#app")).to_be_hidden()
                    expect(page.locator("#login-err")).to_contain_text("无效")
                    page.locator("#token-input").fill("ui-test-admin-token")
                    page.locator("#login-btn").click()
                    expect(page.locator(".queue-shell")).to_be_visible()
                    report["checks"].append("invalid token rejected; deep-link login works")
                    check_page(page, "queue", 1440)
                    page.screenshot(path=str(OUT / "queue-empty.png"), full_page=True)
                    # Real keys API and its full lifecycle.
                    navigate(page, "keys")
                    key_input = page.locator("#key-form input")
                    key_input.fill("gpt-4o, deepseek-chat")
                    page.evaluate("window.testKeyForm=document.getElementById('key-form'); window.testPoll()")
                    page.locator("#keys-refresh").click()
                    page.wait_for_function("() => !document.getElementById('tab-keys').hasAttribute('aria-busy')")
                    assert page.evaluate("testKeyForm === document.getElementById('key-form')")
                    expect(key_input).to_have_value("gpt-4o, deepseek-chat")
                    before = page.locator(".key-item").count()
                    page.locator("#key-form button[type=submit]").click()
                    expect(page.locator("#new-key-secret")).to_be_visible()
                    secret = page.locator("#new-key-secret").inner_text()
                    assert secret.startswith("hr-")
                    page.locator("#copy-secret").click()
                    expect(page.locator("#copy-secret")).to_contain_text("已复制")
                    assert page.evaluate("navigator.clipboard.readText()") == secret
                    page.locator("#secret-ok").click()
                    expect(page.locator("#new-key-secret")).to_have_count(0)
                    expect(page.locator(".key-item")).to_have_count(before + 1)
                    item = page.locator(".key-item").last
                    item.locator('[data-act="edit"]').click()
                    page.locator("#edit-key-form input").fill("gpt-4o")
                    page.locator("#edit-key-form button[type=submit]").click()
                    expect(page.locator("#modal")).to_be_hidden()
                    expect(item).not_to_contain_text("deepseek-chat")
                    item.locator('[data-act="ban"]').click()
                    expect(item.locator(".badge")).to_have_text("已停用")
                    item.locator('[data-act="unban"]').click()
                    expect(item.locator(".badge")).to_have_text("可用")
                    item.locator('[data-act="del"]').click()
                    page.locator("#confirm-cancel").click()
                    expect(page.locator(".key-item")).to_have_count(before + 1)
                    item.locator('[data-act="del"]').click()
                    page.locator("#confirm-apply").click()
                    expect(page.locator("#modal")).to_be_hidden()
                    expect(page.locator(".key-item")).to_have_count(before)
                    report["checks"].append("key draft preserved; real create/copy/edit/disable/enable/delete/cancel")
                    # Real upstream and mapping API; model discovery is mocked to avoid external calls.
                    navigate(page, "providers")
                    page.locator("#new-prov").click()
                    page.locator('#prov-form [name="name"]').fill('qa "edge/one"')
                    page.locator('#prov-form [name="base_url"]').fill("https://edge.example/v1")
                    page.locator('#prov-form [name="credentials"]').fill("ui-only-secret")
                    page.route("**/api/providers/probe-models", lambda r: r.fulfill(json={"models": ["gpt-4o", "edge-model"]}))
                    page.locator("#probe-btn").click()
                    expect(page.locator('#prov-form [name="models"]')).to_have_value("gpt-4o, edge-model")
                    page.locator('#prov-form [name="concurrency"]').fill("8")
                    page.locator('#prov-form button[type="submit"]').click()
                    expect(page.locator("#modal")).to_be_hidden()
                    expect(page.locator(".provider-card")).to_have_count(3)
                    edge = page.locator(".provider-card").filter(has=page.locator("h3", has_text='qa "edge/one"'))
                    edge.locator("[data-edit-prov]").click()
                    page.locator('#edit-prov-form [name="rpm"]').fill("120")
                    page.locator('#edit-prov-form button[type="submit"]').click()
                    expect(page.locator("#modal")).to_be_hidden()
                    expect(edge.locator(".provider-limits")).to_contain_text("120")
                    edge.locator('.credential-add input').fill("another-test-secret")
                    edge.locator('.credential-add button').click()
                    expect(edge.locator(".credential-row")).to_have_count(2)
                    edge.locator("[data-rm-cred]").last.click()
                    page.locator("#confirm-apply").click()
                    expect(page.locator("#modal")).to_be_hidden()
                    expect(edge.locator(".credential-row")).to_have_count(1)
                    form = page.locator("#route-form")
                    form.locator('[name="model"]').fill('unified/test')
                    form.locator(".map-prov").select_option('qa "edge/one"')
                    form.locator(".map-up").fill("edge-model")
                    page.locator("#add-map").click()
                    form.locator(".map-prov").last.select_option("qa-backup")
                    edge.locator('.credential-add input').fill("unsaved-credential")
                    page.locator("#providers-refresh").click()
                    page.wait_for_function("() => !document.getElementById('tab-providers').hasAttribute('aria-busy')")
                    expect(form.locator('[name="model"]')).to_have_value("unified/test")
                    expect(form.locator(".map-row")).to_have_count(2)
                    expect(edge.locator('.credential-add input')).to_have_value("unsaved-credential")
                    page.evaluate("window.testPoll()")
                    expect(form.locator(".map-up").first).to_have_value("edge-model")
                    # Server failure must keep all typed mappings and re-enable submit.
                    page.route("**/api/routes", lambda r: r.fulfill(status=500, json={"error": {"message": "simulated save failure"}}) if r.request.method == "POST" else r.continue_())
                    form.locator('button[type="submit"]').click()
                    expect(page.locator("#toast-msg")).to_contain_text("simulated save failure")
                    expect(form.locator('[name="model"]')).to_have_value("unified/test")
                    expect(form.locator('button[type="submit"]')).to_be_enabled()
                    page.unroute("**/api/routes")
                    with page.expect_response(lambda r: r.url.endswith("/api/routes") and r.request.method == "POST") as response:
                        form.locator('button[type="submit"]').click()
                    assert response.value.status == 200
                    expect(page.locator(".route-card")).to_have_count(1)
                    expect(page.locator(".route-hop")).to_have_count(2)
                    expect(form.locator('[name="model"]')).to_have_value("")
                    page.locator("[data-edit-route]").click()
                    expect(form.locator('[name="model"]')).to_have_value("unified/test")
                    form.locator(".map-up").first.fill("updated-model")
                    form.locator('button[type="submit"]').click()
                    expect(page.locator(".route-card")).to_contain_text("updated-model")
                    page.locator("[data-del-route]").click()
                    page.locator("#confirm-apply").click()
                    expect(page.locator("#modal")).to_be_hidden()
                    expect(page.locator(".route-card")).to_have_count(0)
                    edge.locator('[data-del-prov]').click()
                    page.locator("#confirm-apply").click()
                    expect(page.locator("#modal")).to_be_hidden()
                    expect(page.locator(".provider-card")).to_have_count(2)
                    report["checks"].append("real upstream CRUD incl quoted/slash name; probe UI; credentials; multi-target routes; draft/failure recovery")
                    # Deterministic real browser UI fixtures, no production metrics fabricated.
                    now = "2026-09-10T06:20:00Z"
                    records = [{"request_id": f"req_test_{i:04}", "key_id": "key_demo", "model": "gpt-4o" if i % 2 else "deepseek-chat", "status": ["success", "error", "timeout", "cancelled"][i % 4], "error_code": "upstream_error" if i % 4 == 1 else "", "queue_wait_ms": 1234, "attempts": 2, "tokens_in": 2345, "tokens_out": 678, "finished_at": now} for i in range(61)]
                    from urllib.parse import urlparse, parse_qs
                    def requests_route(route):
                        url = urlparse(route.request.url)
                        if url.path != "/api/requests":
                            record = next((r for r in records if r["request_id"] == url.path.rsplit("/", 1)[-1]), records[0])
                            return route.fulfill(json=record)
                        q = parse_qs(url.query)
                        search = q.get("q", [""])[0]
                        status = q.get("status", [""])[0]
                        rows = [r for r in records if (not search or any(search in str(v) for v in r.values())) and (not status or r["status"] == status)]
                        offset, limit = int(q.get("offset", [0])[0]), int(q.get("limit", [50])[0])
                        route.fulfill(json={"requests": rows[offset:offset+limit], "total": len(rows)})
                    page.route("**/api/requests**", requests_route)
                    navigate(page, "requests")
                    expect(page.locator("button.request-id")).to_have_count(50)
                    page.locator("#req-next").click()
                    expect(page.locator("button.request-id")).to_have_count(11)
                    page.locator("#req-prev").click()
                    expect(page.locator("button.request-id")).to_have_count(50)
                    page.locator('[data-status="error"]').click()
                    expect(page.locator("button.request-id")).to_have_count(15)
                    page.locator("#req-search").fill("test_0001")
                    expect(page.locator("button.request-id")).to_have_count(1)
                    expect(page.locator("#req-search")).to_be_focused()
                    page.locator("#req-search").press("End")
                    page.evaluate("window.testPoll()")
                    expect(page.locator("#req-search")).to_be_focused()
                    page.locator("button.request-id").first.focus()
                    page.keyboard.press("Enter")
                    expect(page.locator("#modal")).to_be_visible()
                    expect(page.locator(".detail-raw pre")).to_be_hidden()
                    page.locator(".detail-raw summary").click()
                    expect(page.locator(".detail-raw pre")).to_be_visible()
                    page.keyboard.press("Escape")
                    expect(page.locator("#modal")).to_be_hidden()
                    expect(page.locator("button.request-id").first).to_be_focused()
                    page.locator("#req-search").fill("absent")
                    expect(page.locator(".request-table .empty")).to_be_visible()
                    expect(page.locator(".request-signal")).to_contain_text("没有记录")
                    page.locator("#req-search").fill("")
                    page.locator('[data-status=""]').click()
                    expect(page.locator("button.request-id")).to_have_count(50)
                    report["checks"].append("pagination/filter/debounced search retains focus; keyboard detail/focus return; zero-result state")
                    # Queue fixture + poll consistency on direct navigation.
                    page.route("**/api/queue", lambda r: r.fulfill(json={"depth": 2, "max_depth": 200, "items": [{"position": i+1, "request_id": f"req_wait_{i}", "model": "gpt-4o", "queued_at": now, "waiting_for": "2.4s"} for i in range(2)]}))
                    page.route("**/api/stats", lambda r: r.fulfill(json={"now": now, "queue": {"depth": 2, "max_depth": 200, "running": 3, "longest_wait": "2.4s"}, "today": {"requests": 120, "success": 118, "failed": 2, "queued": 16, "tokens_in": 284300, "tokens_out": 92300, "tokens_total": 376600, "p50_wait": 30, "p95_wait": 1240}}))
                    navigate(page, "queue")
                    expect(page.locator(".queue-item")).to_have_count(2)
                    expect(page.locator(".queue-stat-grid")).to_contain_text("2.4s")
                    expect(page.locator("progress")).to_have_attribute("value", "1.0")
                    report["checks"].append("queue capacity/running/waiting use fresh API snapshots")
                    for width in (1440, 1024, 768, 390, 320):
                        page.set_viewport_size({"width": width, "height": 1000 if width > 640 else 844})
                        for tab in ("queue", "requests", "keys", "providers", "overview"):
                            navigate(page, tab)
                            check_page(page, tab, width)
                            if width in (1440, 390):
                                path = OUT / f"{tab}-{width}.png"
                                page.screenshot(path=str(path), full_page=True)
                                report["screenshots"].append(path.name)
                    # Offline/stale states and retry preserve configuration drafts.
                    navigate(page, "providers")
                    page.locator('#route-form [name="model"]').fill("keep-draft")
                    context.set_offline(True)
                    page.locator("#providers-refresh").click()
                    expect(page.locator(".load-error")).to_be_visible()
                    expect(page.locator('#route-form [name="model"]')).to_have_value("keep-draft")
                    context.set_offline(False)
                    page.locator(".load-error button").click()
                    expect(page.locator(".load-error")).to_have_count(0)
                    expect(page.locator('#route-form [name="model"]')).to_have_value("keep-draft")
                    report["checks"].append("5 viewport widths x 5 pages; offline/retry preserves mapping draft")
                    assert not js_errors, js_errors
                    assert not csp_errors, csp_errors
                    report["checks"].append("no JavaScript errors, CSP violations, missing icons, emoji, unnamed controls or page overflow")
                    report["browser"] = browser.version
                    browser.close()
                report["passed"] = True
                (OUT / "report.json").write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
                print(json.dumps(report, ensure_ascii=False, indent=2))
            finally:
                server.terminate()
                server.wait(timeout=10)


if __name__ == "__main__":
    run()
