# CI/CD Pipeline

## Overview

This project uses **GitHub Actions** for continuous integration. The pipeline validates dbt models and tests on every pull request.

## GitHub Actions Workflow

`.github/workflows/dbt-ci.yml`:

```yaml
name: dbt CI

on:
  pull_request:
    paths:
      - 'dbt_datamesh/**'
      - 'postgres-dwh/init.sql'
      - 'scripts/init-dwh.sql'

jobs:
  dbt-test:
    runs-on: ubuntu-latest
    services:
      postgres:
        image: postgres:15-alpine
        env:
          POSTGRES_USER: dwh
          POSTGRES_PASSWORD: dwh
          POSTGRES_DB: datamesh_dwh
        ports:
          - 5434:5432
        options: >-
          --health-cmd pg_isready
          --health-interval 10s
          --health-timeout 5s
          --health-retries 5

    steps:
      - uses: actions/checkout@v4

      - uses: actions/setup-python@v5
        with:
          python-version: '3.12'

      - name: Install dependencies
        run: |
          pip install dbt-postgres
          cd dbt_datamesh && dbt deps

      - name: Seed DWH
        run: |
          PGPASSWORD=dwh psql -h localhost -p 5434 -U dwh -d datamesh_dwh -f scripts/init-dwh.sql

      - name: dbt run
        run: cd dbt_datamesh && dbt run --target ci

      - name: dbt test
        run: cd dbt_datamesh && dbt test --target ci

      - name: Upload dbt artifacts
        uses: actions/upload-artifact@v4
        if: always()
        with:
          name: dbt-artifacts
          path: dbt_datamesh/target/
```

## CI Profile

`dbt_datamesh/profiles.yml`:

```yaml
datamesh:
  target: dev
  outputs:
    dev:
      type: postgres
      host: localhost
      port: 5434
      user: dwh
      password: dwh
      dbname: datamesh_dwh
      schema: raw
      threads: 4
    ci:
      type: postgres
      host: localhost
      port: 5434
      user: dwh
      password: dwh
      dbname: datamesh_dwh
      schema: raw_ci
      threads: 4
```

## PR Checklist

Before submitting a pull request:

- [ ] `dbt run` passes without errors
- [ ] `dbt test` — all tests PASS
- [ ] Schema changes are documented (if `init-dwh.sql` was modified)
- [ ] New fields are extracted in Silver models
- [ ] Tests added for new fields
- [ ] Review from DWH owner (if DDL changes)

## Schema Change Workflow

### Adding a New Field

1. Update source database schema (if needed)
2. Update `scripts/init-dwh.sql` (if DWH schema changes)
3. Update Silver models to extract new field
4. Add tests in `models/schema.yml`
5. Open PR → CI runs automatically

### Handling Breaking Changes

Schema-on-Read means the CDC pipeline won't break, but dbt tests will catch issues:

1. Source schema changes (e.g., DROP COLUMN)
2. dbt test fails in CI
3. Fix Silver model or restore source column
4. Re-run CI

## Local CI Simulation

Test CI locally before pushing:

```bash
# Start local Postgres (same as CI)
docker run -d --name postgres-ci -p 5434:5432   -e POSTGRES_USER=dwh -e POSTGRES_PASSWORD=dwh -e POSTGRES_DB=datamesh_dwh   postgres:15-alpine

# Run dbt with CI target
cd dbt_datamesh
dbt run --target ci
dbt test --target ci

# Cleanup
docker rm -f postgres-ci
```

## Deployment

### Manual Deployment

```bash
# 1. Deploy infrastructure
make up

# 2. Run dbt models
cd dbt_datamesh && dbt run

# 3. Verify
python scripts/data_generator.py --mode verify
```

### Automated Deployment (Optional)

For production environments, extend the CI pipeline:

```yaml
# Add to .github/workflows/dbt-ci.yml
  deploy:
    needs: dbt-test
    runs-on: ubuntu-latest
    if: github.ref == 'refs/heads/main'
    steps:
      - name: Deploy to production
        run: |
          # Add your deployment steps here
          echo "Deploying dbt models..."
```

## Versioning

- **dbt models**: Version controlled in Git
- **DWH schema**: Version controlled via `init-dwh.sql`
- **Connector configs**: Version controlled in `scripts/setup-connectors.sh`
- **Grafana dashboards**: Version controlled in `grafana/dashboards/`

## Rollback Strategy

If a deployment causes issues:

1. Revert the PR in GitHub
2. Re-run `dbt run` with previous version
3. If DWH schema changed, restore from backup or re-run `init-dwh.sql`
