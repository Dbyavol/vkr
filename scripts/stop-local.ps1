$ErrorActionPreference = "SilentlyContinue"

$ports = @(8040, 8050, 8060, 8070, 8080, 8090, 5173)
$pids = @()

foreach ($port in $ports) {
    $lines = netstat -ano | Select-String -Pattern ":$port\s+.*LISTENING\s+(\d+)"
    foreach ($line in $lines) {
        $text = $line.ToString()
        if ($text -match "LISTENING\s+(\d+)") {
            $pids += [int]$Matches[1]
        }
    }
}

$pids |
    Sort-Object -Unique |
    Where-Object { $_ -gt 0 } |
    ForEach-Object {
        Stop-Process -Id $_ -Force
    }

Write-Host "Local stack processes stopped."
