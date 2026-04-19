# Universal Comparative Analysis System

Web-oriented microservice system for universal comparative analysis of structured datasets.

The system supports:

- dataset upload and preview for CSV/XLSX/JSON;
- data profiling, quality scoring, preprocessing recommendations and encoding;
- weighted-coefficient ranking and target-object analog search;
- user registration, authorization and roles;
- projects, comparison history and scenario versions;
- result explanations, sensitivity metrics and DOCX/HTML/JSON reports;
- PostgreSQL-backed services and S3-compatible file storage through MinIO.

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

The smoke script builds and starts the stack, waits for service health endpoints, logs in as the demo admin and checks the orchestrator dashboard.

## URLs

- Frontend: [http://localhost:5173](http://localhost:5173)
- Orchestrator API docs: [http://localhost:8050/docs](http://localhost:8050/docs)
- Auth API docs: [http://localhost:8040/docs](http://localhost:8040/docs)
- Import API docs: [http://localhost:8060/docs](http://localhost:8060/docs)
- Storage API docs: [http://localhost:8070/docs](http://localhost:8070/docs)
- Analysis API docs: [http://localhost:8080/docs](http://localhost:8080/docs)
- Preprocessing API docs: [http://localhost:8090/docs](http://localhost:8090/docs)
- MinIO console: [http://localhost:9001](http://localhost:9001)

Demo admin:

```text
admin@example.com / admin12345
```

## Docker Services

- `frontend`: React/Vite user interface.
- `orchestrator-service`: single backend entry point for the frontend.
- `auth-service`: users, roles, login and registration.
- `storage-service`: metadata, files, projects and comparison history.
- `import-service`: CSV/XLSX/JSON parsing and preview.
- `preprocessing-service`: profiling, quality scoring and data transformations.
- `comparative-analysis-service`: ranking, analog search and explainability.
- `auth-db`: PostgreSQL database for authorization.
- `storage-db`: PostgreSQL database for storage metadata.
- `minio`: S3-compatible object storage for uploaded datasets and result artifacts.

## Local Scripts

The previous non-Docker local scripts are kept only as a fallback for development/debugging. The expected full-system workflow is Docker Compose.

Before starting Docker, `start-docker.ps1` also calls `stop-local.ps1` to close old local processes on the project ports.

## Project Files

- [docker-compose.yml](docker-compose.yml): full Docker stack.
- [scripts/start-docker.ps1](scripts/start-docker.ps1): build and start the stack.
- [scripts/stop-docker.ps1](scripts/stop-docker.ps1): stop the stack.
- [scripts/test-docker.ps1](scripts/test-docker.ps1): Docker smoke checks.
- [docs/IMPROVEMENT_ROADMAP.md](docs/IMPROVEMENT_ROADMAP.md): improvement backlog and completion status.
- [test-dataset.csv](test-dataset.csv): small dataset for manual UI testing.
