# Universal Comparative Analysis System

Web-oriented modular monolith for universal comparative analysis of structured datasets.

The system supports:

- dataset upload and preview for CSV/XLSX/JSON;
- data profiling, quality scoring, preprocessing recommendations and encoding;
- weighted-coefficient ranking and target-object analog search;
- user registration, authorization and roles;
- projects, comparison history and scenario versions;
- result explanations, sensitivity metrics and DOCX/HTML/JSON reports;
- PostgreSQL-backed backend and S3-compatible file storage through MinIO.

## Main Runtime

Docker Compose is the primary runtime for development and testing.

```powershell
.\scripts\start-docker.ps1
```

Stop the stack:

```powershell
.\scripts\stop-docker.ps1
```

Run Docker smoke checks:

```powershell
.\scripts\test-docker.ps1
```

The smoke script builds and starts the stack, waits for backend and frontend health endpoints, logs in as the demo admin and checks the system dashboard.

Both `start-docker.ps1` and `test-docker.ps1` first run:

```powershell
docker compose down --remove-orphans
```

This guarantees that old containers from a previous run do not affect the next launch.

## URLs

- Frontend: [http://localhost:5173](http://localhost:5173)
- Backend API docs: [http://localhost:8050/docs](http://localhost:8050/docs)
- MinIO console: [http://localhost:9001](http://localhost:9001)

Demo admin:

```text
admin@example.com / admin12345
```

## Docker Troubleshooting

If you see an error like:

```text
open //./pipe/dockerDesktopLinuxEngine: The system cannot find the file specified
```

Docker CLI is installed, but Docker Desktop Linux Engine is not running. Open Docker Desktop, wait until the engine is fully started, then run:

```powershell
.\scripts\start-docker.ps1
```

If Docker Desktop is already open, restart Docker Desktop from the tray menu and try again.

## Docker Services

- `frontend`: React/Vite user interface.
- `backend`: unified FastAPI application with auth, storage, import, preprocessing and analysis modules.
- `backend-db`: PostgreSQL database for the modular monolith.
- `minio`: S3-compatible object storage for uploaded datasets and result artifacts.

## Project Files

- [backend](backend): modular monolith backend.
- [docker-compose.yml](docker-compose.yml): full Docker stack.
- [scripts/start-docker.ps1](scripts/start-docker.ps1): build and start the stack.
- [scripts/stop-docker.ps1](scripts/stop-docker.ps1): stop the stack.
- [scripts/test-docker.ps1](scripts/test-docker.ps1): Docker smoke checks.
- [docs/IMPROVEMENT_ROADMAP.md](docs/IMPROVEMENT_ROADMAP.md): improvement backlog and completion status.
- [test-dataset.csv](test-dataset.csv): small dataset for manual UI testing.
