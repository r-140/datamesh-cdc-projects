# CI/CD Pipeline

## Workflow Stages

### 1. Lint & Format
- **Ruff** — linting and import sorting
- **Black** — code formatting
- **MyPy** — static type checking

### 2. Unit Tests
- pytest with coverage reporting
- Excludes integration tests (`-m "not integration"`)
- Uploads coverage to Codecov

### 3. Schema Compatibility Checks
- Validates Avro schemas
- Runs simulation mode to verify logic

### 4. Integration Tests
- Starts full Docker Compose stack
- Runs integration tests with Testcontainers
- Collects logs on failure

### 5. Build & Push
- Builds Docker image
- Pushes to GitHub Container Registry (GHCR)
- Tags: branch, semver, short SHA

### 6. Security Scan
- **Trivy** vulnerability scanner
- Uploads SARIF results to GitHub Security tab

## Triggers

| Event | Runs |
|-------|------|
| Push to `main` | All stages |
| Push to `develop` | Lint + Unit Tests + Schema Compat |
| Pull Request to `main` | All except deploy |
