$ErrorActionPreference = "Stop"

$root = Split-Path -Parent $PSScriptRoot
Set-Location $root

function Wait-Http {
    param(
        [string]$Url,
        [string]$Name,
        [int]$Attempts = 60
    )

    for ($i = 1; $i -le $Attempts; $i++) {
        try {
            $response = Invoke-RestMethod -Uri $Url -TimeoutSec 3
            Write-Host "$Name is ready: $($response.status)"
            return
        }
        catch {
            Start-Sleep -Seconds 2
        }
    }
    throw "$Name did not become ready: $Url"
}

docker compose up --build -d

Wait-Http "http://localhost:8040/health" "auth-service"
Wait-Http "http://localhost:8050/health" "orchestrator-service"
Wait-Http "http://localhost:8060/health" "import-service"
Wait-Http "http://localhost:8070/health" "storage-service"
Wait-Http "http://localhost:8080/health" "comparative-analysis-service"
Wait-Http "http://localhost:8090/health" "preprocessing-service"

$login = Invoke-RestMethod `
    -Uri "http://localhost:8040/api/v1/auth/login" `
    -Method Post `
    -ContentType "application/json" `
    -Body (@{ email = "admin@example.com"; password = "admin12345" } | ConvertTo-Json)

if (-not $login.access_token) {
    throw "Auth smoke test failed: token was not returned."
}

$dashboard = Invoke-RestMethod `
    -Uri "http://localhost:8050/api/v1/system/dashboard" `
    -Headers @{ Authorization = "Bearer $($login.access_token)" }

if (-not $dashboard.services) {
    throw "Dashboard smoke test failed."
}

Write-Host "Docker smoke test completed successfully."
