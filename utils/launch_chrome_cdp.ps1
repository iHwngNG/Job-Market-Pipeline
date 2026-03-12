$Port = 9222
$ChromePath = "C:\Program Files\Google\Chrome\Application\chrome.exe"
$UserDataDir = "C:\chrome-cdp-profile"

Write-Host "Checking port $Port..."

$PortActive = Test-NetConnection localhost -Port $Port -WarningAction SilentlyContinue
if ($PortActive.TcpTestSucceeded) {
    Write-Host "Already running."
    exit 0
}

$Proc = Get-Process chrome -ErrorAction SilentlyContinue
if ($Proc) {
    Write-Host "Restarting Chrome..."
    Stop-Process -Name chrome -Force
    Start-Sleep -Seconds 2
}

Write-Host "Starting Chrome..."
Start-Process $ChromePath "--remote-debugging-port=$Port --user-data-dir=$UserDataDir"

$Timeout = 10
while ($Timeout -gt 0) {
    Write-Host "." -NoNewline
    $Check = Test-NetConnection localhost -Port $Port -WarningAction SilentlyContinue
    if ($Check.TcpTestSucceeded) {
        Write-Host "Ready!"
        exit 0
    }
    Start-Sleep -Seconds 1
    $Timeout = $Timeout - 1
}

Write-Host "Failed"
exit 1
