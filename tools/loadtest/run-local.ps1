# Harbor concurrent-queue load test - local one-shot (no real upstream cost)
# Builds harbor + mock upstream, starts both, runs loadtest.py, prints queue stats, cleans up.
# Usage: powershell -ExecutionPolicy Bypass -File tools/loadtest/run-local.ps1
#   -With429   scenario B: mock returns 25% 429, observe requeue / Retry-After
#   -Stream    use streaming requests
#   -Raw       only scenario A (default already)

param(
    [switch]$With429,
    [switch]$Stream,
    [switch]$Raw
)

$ErrorActionPreference = "Stop"
$root = Split-Path -Parent (Split-Path -Parent $PSScriptRoot)   # -> harbor/
Set-Location $root

$mock = $null
$harbor = $null
$mockOut = Join-Path $root "tools\loadtest\mock.out.log"
$mockErr = Join-Path $root "tools\loadtest\mock.err.log"
$harborOut = Join-Path $root "tools\loadtest\harbor.out.log"
$harborErr = Join-Path $root "tools\loadtest\harbor.err.log"

function Wait-Port([int]$Port, [int]$Seconds = 15) {
    $deadline = (Get-Date).AddSeconds($Seconds)
    while ((Get-Date) -lt $deadline) {
        if (Get-NetTCPConnection -LocalPort $Port -State Listen -ErrorAction SilentlyContinue) { return $true }
        Start-Sleep -Milliseconds 300
    }
    return $false
}

function Stop-Proc([object]$p) {
    if ($p -and -not $p.HasExited) { Stop-Process -Id $p.Id -Force -ErrorAction SilentlyContinue }
}

try {
    Write-Host "==> build harbor.exe + mock.exe ..." -ForegroundColor Cyan
    go build -o harbor.exe .
    if ($LASTEXITCODE -ne 0) { throw "harbor build failed" }
    go build -o tools\smoke\mock.exe ./tools/smoke/mock
    if ($LASTEXITCODE -ne 0) { throw "mock build failed" }

    Write-Host "==> start mock upstream (127.0.0.1:18090, delay 600ms) ..." -ForegroundColor Cyan
    $mock = Start-Process -FilePath (Join-Path $root "tools\smoke\mock.exe") `
        -ArgumentList @("-addr", "127.0.0.1:18090", "-delay-ms", "600", "-fail-429-rate", "0") `
        -PassThru -WindowStyle Hidden -RedirectStandardOutput $mockOut -RedirectStandardError $mockErr
    if (-not (Wait-Port 18090)) { throw "mock 18090 not ready" }

    Write-Host "==> start harbor (127.0.0.1:8080, provider concurrency=2) ..." -ForegroundColor Cyan
    $harbor = Start-Process -FilePath (Join-Path $root "harbor.exe") `
        -ArgumentList @("--config", "tools\loadtest\harbor-loadtest.yaml") `
        -PassThru -WindowStyle Hidden -RedirectStandardOutput $harborOut -RedirectStandardError $harborErr
    if (-not (Wait-Port 8080)) { throw "harbor 8080 not ready" }

    $common = @("--base-url", "http://127.0.0.1:8080/v1",
                "--key", "hr-loadtest-00000000",
                "--admin-token", "loadtest-admin",
                "--model", "smoke-1")
    if ($Stream) { $common += "--stream" }

    Write-Host "`n========== SCENARIO A: 12 concurrent x 48 requests (upstream cap=2) -> expect queueing ==========" -ForegroundColor Green
    python tools/loadtest/loadtest.py @common --concurrency 12 --total 48 --json tools\loadtest\result-a.json
    if ($LASTEXITCODE -ne 0) { throw "scenario A failed" }

    if ($With429) {
        Write-Host "`n========== SCENARIO B (mock restart, 25% 429): observe requeue / Retry-After ==========" -ForegroundColor Green
        Stop-Proc $mock
        $mock = Start-Process -FilePath (Join-Path $root "tools\smoke\mock.exe") `
            -ArgumentList @("-addr", "127.0.0.1:18090", "-delay-ms", "600", "-fail-429-rate", "0.25") `
            -PassThru -WindowStyle Hidden -RedirectStandardOutput $mockOut -RedirectStandardError $mockErr
        Start-Sleep -Milliseconds 800
        if (-not (Wait-Port 18090 5)) { throw "mock restart failed" }
        python tools/loadtest/loadtest.py @common --concurrency 12 --total 48 --json tools\loadtest\result-b.json
        if ($LASTEXITCODE -ne 0) { throw "scenario B failed" }
    }

    Write-Host "`ndone. results: tools\loadtest\result-a.json (and result-b.json with -With429)" -ForegroundColor Cyan
}
finally {
    Stop-Proc $harbor
    Stop-Proc $mock
    Write-Host "cleaned up harbor/mock processes."
}
