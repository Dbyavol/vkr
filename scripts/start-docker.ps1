$ErrorActionPreference = "Stop"

$root = Split-Path -Parent $PSScriptRoot
Set-Location $root

if (Test-Path (Join-Path $PSScriptRoot "stop-local.ps1")) {
    & (Join-Path $PSScriptRoot "stop-local.ps1") | Out-Null
}

docker compose up --build -d

Write-Host "Docker stack started."
Write-Host "Frontend: http://localhost:5173"
Write-Host "Orchestrator docs: http://localhost:8050/docs"
Write-Host "MinIO console: http://localhost:9001"
Write-Host "Demo admin: admin@example.com / admin12345"
