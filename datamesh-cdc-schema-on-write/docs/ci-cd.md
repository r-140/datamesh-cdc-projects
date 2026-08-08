# CI/CD Pipeline

## GitHub Actions

```yaml
name: dbt CI

on:
  pull_request:
    paths:
      - 'dbt_datamesh/**'
      - 'postgres-dwh/init.sql'

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
          PGPASSWORD=dwh psql -h localhost -p 5434 -U dwh -d datamesh_dwh -f postgres-dwh/init.sql

      - name: dbt run
        run: cd dbt_datamesh && dbt run --target ci

      - name: dbt test
        run: cd dbt_datamesh && dbt test --target ci
```

## Профиль CI

```yaml
# dbt_datamesh/profiles.yml
datamesh_trino:
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

## Правила для PR

- [ ] `dbt run` проходит без ошибок
- [ ] `dbt test` — 19/19 PASS
- [ ] Schema изменений согласовано (если меняли `init.sql`)
- [ ] Ревью от DWH-owner (если DDL-изменения)
