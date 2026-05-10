$ErrorActionPreference = "Stop"

$root = Split-Path -Parent $PSScriptRoot
Set-Location $root

try {
    docker info | Out-Null
}
catch {
    throw "Docker Desktop engine is not running. Start Docker Desktop and wait until the engine becomes available, then run .\scripts\start-docker.ps1 again."
}

docker compose down --remove-orphans

docker compose up --build -d

Write-Host "Docker stack started."
Write-Host "Frontend: http://localhost:5173"
Write-Host "Backend docs: http://localhost:8050/docs"
Write-Host "MinIO console: http://localhost:9001"
