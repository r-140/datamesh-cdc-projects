# Hybrid dbt Project

Run from the hybrid project root:

```bash
make dbt-debug
make dbt-build
```

dbt reads typed `silver.*` tables for business transformations and reads `bronze`/`governance` metadata for evolution visibility. See [`../docs/dbt.md`](../docs/dbt.md) for the design and comparison with the sibling projects.
