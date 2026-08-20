# CI/CD

## Current Checks

The workflow in `.github/workflows/ci-cd.yml` runs from the repository root but sets `datamesh-cdc-hybrid` as its working directory. It performs:

1. editable installation with development dependencies;
2. unit tests with coverage;
3. Python bytecode compilation;
4. Docker Compose configuration validation.

Run the equivalent checks locally:

```bash
python -m pip install -e '.[dev]'
pytest tests --cov=datamesh_cdc
python -m compileall -q src scripts/run_demo.py
docker compose config -q
```

## Silver Change Workflow

A Silver contract change should be delivered atomically as:

- a database migration;
- projection-contract change;
- writer/upsert change;
- unit tests for valid and invalid representations;
- optional Bronze backfill;
- documentation update.

Deploy DWH migrations before consumer code that writes new columns. For incompatible changes, use expand-and-contract: add the new representation, backfill and migrate consumers, then remove the old one.

## Recommended Additional Gates

For a production repository, add:

- container image scanning;
- ephemeral end-to-end Compose tests;
- migration validation against a previous DWH version;
- contract fixtures taken from real Schema Registry versions;
- replay tests for quarantined events;
- deployment health checks and rollback automation.
