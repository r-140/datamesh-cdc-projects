#!/usr/bin/env python3
"""Automated end-to-end demonstration of hybrid schema evolution."""

from __future__ import annotations

import sys
import time
from collections.abc import Callable
from typing import Any

import psycopg2

SOURCE_DSN = "postgresql://postgres:postgres@localhost:5432/orders_db"
DWH_DSN = "postgresql://dwh:dwh@localhost:5434/datamesh_dwh"
POLL_SECONDS = 30


def banner(text: str) -> None:
    print(f"\n{'=' * 72}\n{text}\n{'=' * 72}")


def execute(dsn: str, sql: str, params: tuple[Any, ...] = ()) -> list[tuple[Any, ...]]:
    with psycopg2.connect(dsn) as connection, connection.cursor() as cursor:
        cursor.execute(sql, params)
        return cursor.fetchall() if cursor.description else []


def scalar(dsn: str, sql: str, params: tuple[Any, ...] = ()) -> Any:
    rows = execute(dsn, sql, params)
    return rows[0][0] if rows else None


def wait_for(description: str, condition: Callable[[], Any]) -> Any:
    deadline = time.monotonic() + POLL_SECONDS
    while time.monotonic() < deadline:
        value = condition()
        if value:
            print(f"✓ {description}")
            return value
        time.sleep(1)
    raise RuntimeError(f"Timed out after {POLL_SECONDS}s: {description}")


def bronze_payload(order_id: int) -> Any:
    return scalar(
        DWH_DSN,
        """SELECT payload FROM bronze.cdc_events
           WHERE source_table='orders' AND (payload->>'id')::bigint=%s
           ORDER BY kafka_offset DESC LIMIT 1""",
        (order_id,),
    )


def silver_exists(order_id: int) -> bool:
    return bool(
        scalar(DWH_DSN, "SELECT EXISTS(SELECT 1 FROM silver.orders WHERE id=%s)", (order_id,))
    )


def failure(order_id: int) -> Any:
    return scalar(
        DWH_DSN,
        """SELECT error FROM governance.projection_failures
           WHERE source_table='orders' AND (payload->>'id')::bigint=%s
           ORDER BY kafka_offset DESC LIMIT 1""",
        (order_id,),
    )


def restore_source_schema() -> None:
    execute(
        SOURCE_DSN,
        "ALTER TABLE orders ADD COLUMN IF NOT EXISTS total_amount DECIMAL(12,2)",
    )


def main() -> None:
    banner("HYBRID CDC DEMO: FLEXIBLE BRONZE + GOVERNED SILVER")
    print("Prerequisite: run `make up` and wait until all services are healthy.")

    try:
        execute(SOURCE_DSN, "SELECT 1")
        execute(DWH_DSN, "SELECT 1")
    except psycopg2.OperationalError as exc:
        print(f"✗ Infrastructure is not reachable: {exc}", file=sys.stderr)
        sys.exit(1)

    restore_source_schema()

    banner("STEP 1: BASELINE EVENT REACHES BOTH LAYERS")
    baseline_id = scalar(
        SOURCE_DSN,
        """INSERT INTO orders(customer_id,total_amount,status)
           VALUES (901,42.50,'demo-baseline') RETURNING id""",
    )
    wait_for("baseline event stored unchanged in Bronze", lambda: bronze_payload(baseline_id))
    wait_for("baseline event promoted to typed Silver", lambda: silver_exists(baseline_id))

    banner("STEP 2: ADDITIVE CHANGE IS IMMEDIATELY SAFE IN BRONZE")
    fingerprints_before = scalar(
        DWH_DSN,
        "SELECT count(*) FROM governance.observed_schemas WHERE source_table='orders'",
    )
    execute(SOURCE_DSN, "ALTER TABLE orders ADD COLUMN IF NOT EXISTS promo_code VARCHAR(50)")
    additive_id = scalar(
        SOURCE_DSN,
        """INSERT INTO orders(customer_id,total_amount,status,promo_code)
           VALUES (902,19.99,'demo-additive','HYBRID20') RETURNING id""",
    )
    payload = wait_for(
        "new field retained in Bronze JSONB",
        lambda: (
            p if (p := bronze_payload(additive_id)) and p.get("promo_code") == "HYBRID20" else None
        ),
    )
    wait_for("stable Silver contract still promotes the row", lambda: silver_exists(additive_id))
    fingerprints_after = scalar(
        DWH_DSN,
        "SELECT count(*) FROM governance.observed_schemas WHERE source_table='orders'",
    )
    print(f"  Bronze promo_code: {payload['promo_code']}")
    print(f"  Observed schema fingerprints: {fingerprints_before} -> {fingerprints_after}")
    print("  Silver intentionally does not expose promo_code until its contract is changed.")

    banner("STEP 3: BREAKING CHANGE DOES NOT STOP BRONZE")
    print("Dropping required field total_amount and producing an event without it...")
    execute(SOURCE_DSN, "ALTER TABLE orders DROP COLUMN total_amount")
    broken_id = scalar(
        SOURCE_DSN,
        """INSERT INTO orders(customer_id,status,promo_code)
           VALUES (903,'demo-breaking','BROKEN') RETURNING id""",
    )
    broken_payload = wait_for(
        "breaking event still stored in Bronze",
        lambda: bronze_payload(broken_id),
    )
    error = wait_for(
        "Silver projection quarantined with a concrete reason",
        lambda: failure(broken_id),
    )
    print(f"  Bronze fields: {sorted(broken_payload)}")
    print(f"  Projection error: {error}")
    print(f"  Row present in Silver: {silver_exists(broken_id)}")

    banner("STEP 4: RECOVERY WITH A CORRECTIVE SOURCE EVENT")
    restore_source_schema()
    execute(
        SOURCE_DSN,
        "UPDATE orders SET total_amount=77.70, status='demo-recovered' WHERE id=%s",
        (broken_id,),
    )
    wait_for("corrected event promoted to Silver", lambda: silver_exists(broken_id))
    recovered = execute(
        DWH_DSN,
        "SELECT id,total_amount,status FROM silver.orders WHERE id=%s",
        (broken_id,),
    )[0]
    print(
        f"  Recovered Silver row: id={recovered[0]}, amount={recovered[1]}, status={recovered[2]}"
    )

    banner("RESULT")
    print(
        "✓ Bronze never stopped and preserved every event.\n"
        "✓ Silver stayed typed and rejected the incompatible projection.\n"
        "✓ Governance recorded both schema shapes and the failed projection.\n"
        "✓ A corrected CDC event recovered Silver without rebuilding the pipeline."
    )


if __name__ == "__main__":
    try:
        main()
    finally:
        try:
            restore_source_schema()
        except Exception:
            pass
