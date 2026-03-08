param(
    [string]$LocalUrl = "http://127.0.0.1:8787",
    [string]$LogFile = ".\cf-quick.log",
    [switch]$AutoStartOrigin = $true,
    [string]$OriginStartCommand = "python .\run_all_services.py"
)

$ErrorActionPreference = "Stop"

$cloudflaredCommand = Get-Command cloudflared -ErrorAction SilentlyContinue
if (-not $cloudflaredCommand) {
    $fallbackCandidates = @(
        "C:\Program Files\cloudflared\cloudflared.exe",
        "C:\Program Files (x86)\cloudflared\cloudflared.exe",
        "$env:USERPROFILE\cloudflared.exe"
    )
    $cloudflaredExe = $fallbackCandidates | Where-Object { Test-Path $_ } | Select-Object -First 1
    if (-not $cloudflaredExe) {
        Write-Host "cloudflared is not installed or not in PATH." -ForegroundColor Red
        Write-Host "Install from: https://developers.cloudflare.com/cloudflare-one/connections/connect-networks/downloads/"
        exit 1
    }
}
else {
    $cloudflaredExe = $cloudflaredCommand.Source
}

$scriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$logPath = Join-Path $scriptDir $LogFile

if (-not $env:TRADERS_VIEW_AUTH_ENABLED) {
    $env:TRADERS_VIEW_AUTH_ENABLED = "1"
}

if ($env:TRADERS_VIEW_AUTH_ENABLED -ne "0") {
    if (-not $env:TRADERS_VIEW_USER) {
        $env:TRADERS_VIEW_USER = Read-Host "Enter Traders-View username"
    }
    if (-not $env:TRADERS_VIEW_PASS) {
        $securePassword = Read-Host "Enter Traders-View password" -AsSecureString
        $bstr = [Runtime.InteropServices.Marshal]::SecureStringToBSTR($securePassword)
        try {
            $env:TRADERS_VIEW_PASS = [Runtime.InteropServices.Marshal]::PtrToStringBSTR($bstr)
        }
        finally {
            [Runtime.InteropServices.Marshal]::ZeroFreeBSTR($bstr)
        }
    }
}

function Test-TcpPort {
    param(
        [string]$ComputerName,
        [int]$Port,
        [int]$TimeoutMs = 1200
    )

    $client = New-Object System.Net.Sockets.TcpClient
    try {
        $asyncResult = $client.BeginConnect($ComputerName, $Port, $null, $null)
        if (-not $asyncResult.AsyncWaitHandle.WaitOne($TimeoutMs, $false)) {
            return $false
        }
        $client.EndConnect($asyncResult)
        return $true
    }
    catch {
        return $false
    }
    finally {
        $client.Close()
    }
}

$uri = [System.Uri]$LocalUrl
$originHost = $uri.Host
$originPort = $uri.Port

if (-not (Test-TcpPort -ComputerName $originHost -Port $originPort)) {
    if ($AutoStartOrigin) {
        Write-Host "Origin $LocalUrl is not running. Starting local Traders-View services..." -ForegroundColor Yellow
        Start-Process -FilePath "powershell" -WorkingDirectory $scriptDir -ArgumentList "-NoExit", "-Command", $OriginStartCommand | Out-Null

        $maxOriginChecks = 30
        $originReady = $false
        for ($j = 0; $j -lt $maxOriginChecks; $j++) {
            Start-Sleep -Seconds 1
            if (Test-TcpPort -ComputerName $originHost -Port $originPort) {
                $originReady = $true
                break
            }
        }

        if (-not $originReady) {
            Write-Host "Origin is still unavailable at $LocalUrl. Please check run_all_services.py output." -ForegroundColor Red
            exit 1
        }
        Write-Host "Origin is now reachable at $LocalUrl" -ForegroundColor Green
    }
    else {
        Write-Host "Origin is down at $LocalUrl. Start the Traders-View app first, then run this script again." -ForegroundColor Red
        exit 1
    }
}

Write-Host "Starting Cloudflare quick tunnel..." -ForegroundColor Cyan
Write-Host "Origin: $LocalUrl"
Write-Host "Log:    $logPath"
Write-Host "Press Ctrl+C to stop the tunnel."
Write-Host ""

if (Test-Path $logPath) {
    Remove-Item $logPath -Force
}

$cloudflaredArgs = @(
    "tunnel"
    "--url", $LocalUrl
    "--protocol", "quic"
    "--loglevel", "info"
    "--logfile", $logPath
)

Start-Process -FilePath $cloudflaredExe -ArgumentList $cloudflaredArgs -NoNewWindow

$maxChecks = 30
for ($i = 0; $i -lt $maxChecks; $i++) {
    Start-Sleep -Seconds 1
    if (Test-Path $logPath) {
        $urlLine = Select-String -Path $logPath -Pattern "https://.*trycloudflare.com" -SimpleMatch:$false | Select-Object -Last 1
        if ($urlLine) {
            if ($urlLine.Line -match "https://[a-z0-9\-]+\.trycloudflare\.com") {
                $publicUrl = $Matches[0]
                Write-Host "Tunnel URL: $publicUrl" -ForegroundColor Green
                Write-Host ""
                break
            }
        }
    }
}

Write-Host "Streaming cloudflared logs (Ctrl+C to stop):" -ForegroundColor Yellow
Get-Content -Path $logPath -Wait