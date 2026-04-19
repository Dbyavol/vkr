param(
    [switch]$SkipInstall,
    [switch]$NoFrontend
)

$ErrorActionPreference = "Stop"
$root = Split-Path -Parent $PSScriptRoot
$venv = Join-Path $root ".venv"
$python = Join-Path $venv "Scripts\\python.exe"
$pip = Join-Path $venv "Scripts\\pip.exe"

& (Join-Path $PSScriptRoot "stop-local.ps1") | Out-Null

if (-not (Test-Path $python)) {
    py -m venv $venv
}

if (-not $SkipInstall) {
    & $python -m pip install --upgrade pip
    & $pip install -r (Join-Path $root "requirements-local.txt")
}

$services = @(
    @{
        Name = "auth-service"
        Port = 8040
        AppDir = "services/auth-service"
        Env = @{
            "AUTH_DATABASE_URL" = "sqlite:///./auth.db"
            "AUTH_JWT_SECRET" = "local-dev-secret"
            "AUTH_BOOTSTRAP_ADMIN_EMAIL" = "admin@example.com"
            "AUTH_BOOTSTRAP_ADMIN_PASSWORD" = "admin12345"
        }
    },
    @{
        Name = "comparative-analysis-service"
        Port = 8080
        AppDir = "services/comparative-analysis-service"
        Env = @{}
    },
    @{
        Name = "import-service"
        Port = 8060
        AppDir = "services/import-service"
        Env = @{}
    },
    @{
        Name = "preprocessing-service"
        Port = 8090
        AppDir = "services/preprocessing-service"
        Env = @{}
    },
    @{
        Name = "storage-service"
        Port = 8070
        AppDir = "services/storage-service"
        Env = @{
            "STORAGE_DATABASE_URL" = "sqlite:///./storage.db"
            "STORAGE_LOCAL_STORAGE_DIR" = "./local_storage"
        }
    },
    @{
        Name = "orchestrator-service"
        Port = 8050
        AppDir = "services/orchestrator-service"
        Env = @{
            "ORCHESTRATOR_IMPORT_SERVICE_URL" = "http://localhost:8060"
            "ORCHESTRATOR_PREPROCESSING_SERVICE_URL" = "http://localhost:8090"
            "ORCHESTRATOR_ANALYSIS_SERVICE_URL" = "http://localhost:8080"
            "ORCHESTRATOR_STORAGE_SERVICE_URL" = "http://localhost:8070"
            "ORCHESTRATOR_AUTH_SERVICE_URL" = "http://localhost:8040"
            "ORCHESTRATOR_CORS_ORIGINS" = "http://localhost:5173,http://127.0.0.1:5173"
        }
    }
)

foreach ($svc in $services) {
    $appDir = Join-Path $root $svc.AppDir
    $envAssignments = @()
    foreach ($entry in $svc.Env.GetEnumerator()) {
        $envAssignments += "`$env:$($entry.Key)='$($entry.Value)'"
    }
    $commandParts = @()
    if ($envAssignments.Count -gt 0) {
        $commandParts += ($envAssignments -join "; ")
    }
    $commandParts += "& '$python' -m uvicorn --app-dir '$appDir' app.main:app --host 0.0.0.0 --port $($svc.Port)"
    $command = $commandParts -join "; "
    Start-Process powershell -WorkingDirectory $root -ArgumentList "-NoExit", "-Command", $command | Out-Null
}

if (-not $NoFrontend) {
    $frontendDir = Join-Path $root "frontend"
    $npmExists = Get-Command npm -ErrorAction SilentlyContinue
    if ($npmExists) {
        Start-Process powershell -WorkingDirectory $frontendDir -ArgumentList "-NoExit", "-Command", "npm install; npm run dev -- --host 0.0.0.0 --port 5173" | Out-Null
    }
    else {
        Write-Host "npm was not found; frontend was skipped."
    }
}

Write-Host "Local stack started."
Write-Host "Frontend: http://localhost:5173"
Write-Host "Orchestrator: http://localhost:8050/docs"
