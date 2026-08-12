#!/usr/bin/env python3
"""
CDC Breaking Change Demo — Schema-on-Read

In schema-on-read, breaking changes do NOT crash the pipeline.
Bad messages land in DWH as JSONB, but downstream (dbt tests / silver models)
catch the schema drift.

Usage:
    python breaking_change_demo.py --table orders --column total_amount
    python breaking_change_demo.py --table customers --column email
"""

import argparse
import json
import subprocess
import sys
import time
import urllib.request
import urllib.error

import psycopg2

# ─── Config ──────────────────────────────────────────────────────────

CONNECTORS = {
    "orders": {
        "db": {"host": "localhost", "port": 5432, "user": "postgres", "password": "postgres", "dbname": "orders_db"},
        "connector": "orders-cdc-connector",
        "topic": "orders-server.public.orders",
        "dwh_table": "raw.orders_cdc",
        "insert_cols": "customer_id, total_amount, status",
        "insert_vals": "(1, 999.99, 'completed')",
        "insert_cols_after_drop": "customer_id, status",
        "insert_vals_after_drop": "(1, 'completed')",
        "jsonb_field": "total_amount",
    },
    "customers": {
        "db": {"host": "localhost", "port": 5433, "user": "postgres", "password": "postgres", "dbname": "customers_db"},
        "connector": "customers-cdc-connector",
        "topic": "customers-server.public.customers",
        "dwh_table": "raw.customers_cdc",
        "insert_cols": "full_name, email, country",
        "insert_vals": "('Test User', 'test@demo.com', 'US')",
        "insert_cols_after_drop": "full_name, country",
        "insert_vals_after_drop": "('Test User', 'US')",
        "jsonb_field": "email",
    },
}

KAFKA_CONNECT_URL = "http://localhost:8083"

# ─── Helpers ─────────────────────────────────────────────────────────

def log_step(title):
    print()
    print("=" * 70)
    print(f"  {title}")
    print("=" * 70)

def pg_conn(cfg):
    return psycopg2.connect(**cfg)

def pg_execute(cfg, query, params=None):
    with pg_conn(cfg) as conn:
        with conn.cursor() as cur:
            cur.execute(query, params)
            if cur.description:
                return cur.fetchall()
            conn.commit()
            return None

def get_connector_status(name):
    try:
        req = urllib.request.Request(f"{KAFKA_CONNECT_URL}/connectors/{name}/status")
        with urllib.request.urlopen(req, timeout=5) as resp:
            return json.loads(resp.read().decode("utf-8"))
    except Exception:
        return None

def print_connector_status():
    print("\nConnector Status:")
    for name in ["orders-cdc-connector", "customers-cdc-connector"]:
        st = get_connector_status(name)
        if st:
            cstate = st.get("connector", {}).get("state", "UNKNOWN")
            print(f"  {name:30s} → {cstate}")
            for t in st.get("tasks", []):
                print(f"    {t.get('id','task-0')} → {t.get('state', 'UNKNOWN')}")
        else:
            print(f"  {name:30s} → NOT FOUND")

def get_dwh_count(table):
    try:
        dwh_cfg = {"host": "localhost", "port": 5434, "user": "dwh", "password": "dwh", "dbname": "datamesh_dwh"}
        rows = pg_execute(dwh_cfg, f"SELECT COUNT(*) FROM {table}")
        return rows[0][0] if rows else 0
    except:
        return 0

def check_consumer_alive():
    """Check if cdc_consumer.py process is running."""
    try:
        result = subprocess.run(
            ["pgrep", "-f", "cdc_consumer.py"],
            capture_output=True, text=True
        )
        return result.returncode == 0
    except:
        return False

def check_jsonb_field(table, field, record_id):
    """Check if a field exists in the latest JSONB payload."""
    try:
        dwh_cfg = {"host": "localhost", "port": 5434, "user": "dwh", "password": "dwh", "dbname": "datamesh_dwh"}
        rows = pg_execute(dwh_cfg, f"""
            SELECT payload->>'{field}' as val
            FROM {table}
            WHERE id = %s
        """, (record_id,))
        return rows[0][0] if rows else None
    except:
        return None

def simulate_silver_query(table, field):
    """Simulate what a silver model would do: extract typed field from JSONB."""
    try:
        dwh_cfg = {"host": "localhost", "port": 5434, "user": "dwh", "password": "dwh", "dbname": "datamesh_dwh"}
        # This is what dbt silver model does
        rows = pg_execute(dwh_cfg, f"""
            SELECT
                id,
                (payload->>'{field}')::numeric as {field}
            FROM {table}
            ORDER BY id DESC
            LIMIT 3
        """)
        return rows
    except Exception as e:
        return str(e)

# ─── Main ────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="CDC Breaking Change Demo — Schema-on-Read")
    parser.add_argument("--table", required=True, choices=["orders", "customers"])
    parser.add_argument("--column", required=True, help="Column to drop")
    args = parser.parse_args()

    cfg = CONNECTORS[args.table]
    col = args.column

    # ── Step 1: Baseline ──────────────────────────────────────────────
    log_step("STEP 1: BASELINE — Schema-on-Read Pipeline")
    print_connector_status()
    baseline_count = get_dwh_count(cfg["dwh_table"])
    print(f"\n  DWH baseline: {baseline_count} rows in {cfg['dwh_table']}")

    if not check_consumer_alive():
        print("\n⚠ CDC Consumer is NOT running. Start it with: make consumer")
        sys.exit(1)
    print("  CDC Consumer: RUNNING ✓")

    # ── Step 2: Schema ──────────────────────────────────────────────
    log_step(f"STEP 2: Current schema of {args.table}")
    rows = pg_execute(cfg["db"], f"""
        SELECT column_name
        FROM information_schema.columns
        WHERE table_name = '{args.table}' AND table_schema = 'public'
        ORDER BY ordinal_position
    """)
    for r in rows:
        print(f"  {r[0]}")

    # ── Step 3: Baseline insert ─────────────────────────────────────
    log_step("STEP 3: Generate baseline data")
    print(f"\n>>> Inserting into {args.table}...")
    try:
        pg_execute(cfg["db"],
            f"INSERT INTO {args.table} ({cfg['insert_cols']}) VALUES {cfg['insert_vals']}")
        print("✓ Insert successful")
    except Exception as e:
        print(f"✗ Insert failed: {e}")
    time.sleep(3)
    new_count = get_dwh_count(cfg["dwh_table"])
    print(f"  DWH after insert: {new_count} rows (+{new_count - baseline_count})")

    # ── Step 4: BREAKING CHANGE ─────────────────────────────────────
    log_step(f"STEP 4: BREAKING CHANGE — DROP COLUMN {col}")
    print(f"\n>>> EXECUTING: ALTER TABLE {args.table} DROP COLUMN {col};")
    try:
        pg_execute(cfg["db"], f"ALTER TABLE {args.table} DROP COLUMN {col}")
        print("✓ Column dropped successfully")
    except Exception as e:
        print(f"✗ Failed to drop column: {e}")
        sys.exit(1)

    # ── Step 5: Insert AFTER drop (WITHOUT the dropped column) ───────
    log_step("STEP 5: Insert after breaking change (schema mismatch)")
    print(f"\n>>> Inserting into {args.table} WITHOUT column '{col}'...")
    try:
        pg_execute(cfg["db"],
            f"INSERT INTO {args.table} ({cfg['insert_cols_after_drop']}) VALUES {cfg['insert_vals_after_drop']}")
        print("✓ Insert succeeded at DB level — event written to WAL")
        print(f"  (Debezium serializes → Kafka → Consumer writes JSONB)")
        print(f"  → In schema-on-read, the message lands in DWH even without '{col}'!")
    except Exception as e:
        print(f"✗ Insert failed: {e}")

    # ── Step 6: Monitor pipeline ──────────────────────────────────────
    log_step("STEP 6: Monitor pipeline (Schema-on-Read behavior)")
    print("""
  In schema-on-read:
    - Consumer stays RUNNING (JSONB accepts any schema)
    - DWH continues to receive new rows
    - The 'bad' message is in DWH, but the field is NULL/missing in JSONB
    - Downstream (dbt tests / silver models) will catch the drift
""")

    prev_count = get_dwh_count(cfg["dwh_table"])
    for i in range(1, 4):
        print(f"\nCheck {i}/3 (waiting 5s)...")
        time.sleep(5)
        curr_count = get_dwh_count(cfg["dwh_table"])
        print(f"  DWH rows: {curr_count} (delta: {curr_count - prev_count})")
        prev_count = curr_count

    print_connector_status()

    # ── Step 7: Check JSONB payload ───────────────────────────────────
    log_step("STEP 7: Inspect JSONB payload")
    latest_id = pg_execute({"host": "localhost", "port": 5434, "user": "dwh", "password": "dwh", "dbname": "datamesh_dwh"},
        f"SELECT id FROM {cfg['dwh_table']} ORDER BY id DESC LIMIT 1")[0][0]

    field_value = check_jsonb_field(cfg["dwh_table"], cfg["jsonb_field"], latest_id)
    print(f"\n  Latest record ID: {latest_id}")
    print(f"  JSONB field '{cfg['jsonb_field']}': {field_value if field_value is not None else 'NULL / MISSING'}")

    if field_value is None:
        print(f"\n  ⚠️  FIELD IS MISSING IN JSONB!")
        print(f"     This is the breaking change — downstream queries will fail.")
    else:
        print(f"\n  ✓ Field present: {field_value}")

    # ── Step 8: Simulate downstream impact (silver model) ─────────────
    log_step("STEP 8: Simulate downstream impact (dbt silver model)")
    print(f"\n  Simulating: SELECT (payload->>'{cfg['jsonb_field']}')::numeric FROM {cfg['dwh_table']}")
    result = simulate_silver_query(cfg["dwh_table"], cfg["jsonb_field"])
    if isinstance(result, str):
        print(f"\n  🚨 SILVER MODEL WOULD FAIL:")
        print(f"     {result}")
    else:
        print(f"\n  Result:")
        for row in result:
            print(f"    id={row[0]}, {cfg['jsonb_field']}={row[1]}")
        if any(r[1] is None for r in result):
            print(f"\n  ⚠️  NULL values detected — dbt 'not_null' test would FAIL!")

    # ── Step 9: Recovery ──────────────────────────────────────────────
    log_step("STEP 9: RECOVERY — Restore column, downstream fixed")
    print(f"\n>>> RESTORING COLUMN:")
    print(f"    ALTER TABLE {args.table} ADD COLUMN {col} DECIMAL(12,2);")
    try:
        pg_execute(cfg["db"], f"ALTER TABLE {args.table} ADD COLUMN {col} DECIMAL(12,2)")
        print("✓ Column restored")
    except Exception as e:
        print(f"✗ Failed to restore column: {e}")

    print(f"\n>>> Inserting new valid data...")
    try:
        pg_execute(cfg["db"],
            f"INSERT INTO {args.table} ({cfg['insert_cols']}) VALUES {cfg['insert_vals']}")
        print("✓ Valid insert successful")
    except Exception as e:
        print(f"✗ Valid insert failed: {e}")

    print("\nWaiting 10s for pipeline to catch up...")
    time.sleep(10)
    final_count = get_dwh_count(cfg["dwh_table"])
    print(f"  DWH final count: {final_count} rows")

    # ── Step 10: Final state ──────────────────────────────────────────
    log_step("STEP 10: FINAL STATE & PRODUCTION TAKEAWAYS")
    print("""
╔══════════════════════════════════════════════════════════════════════╗
║  PRODUCTION TAKEAWAYS — Schema-on-Read                               ║
║                                                                      ║
║  1. Pipeline NEVER crashes on schema changes                         ║
║     → JSONB accepts any payload                                      ║
║                                                                      ║
║  2. Breaking changes surface DOWNSTREAM (dbt tests, silver models)   ║
║     → not_null(total_amount) FAILS                                   ║
║     → typed extraction (payload->>'x')::numeric returns NULL         ║
║                                                                      ║
║  3. Data is NOT lost — bad message is in DWH, just incomplete        ║
║                                                                      ║
║  4. Recovery: fix source schema → new messages are complete          ║
║     → Backfill: UPDATE raw.orders_cdc SET payload = ...             ║
║     → Or: accept NULLs in silver model with COALESCE                 ║
║                                                                      ║
║  VS Schema-on-Write:                                                 ║
║     - SoW: DLQ catches bad message, DWH stays clean                  ║
║     - SoR: DWH gets everything, tests catch drift                    ║
╚══════════════════════════════════════════════════════════════════════╝
""")

if __name__ == "__main__":
    main()
