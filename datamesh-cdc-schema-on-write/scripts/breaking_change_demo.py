#!/usr/bin/env python3
"""
CDC Breaking Change Demo — Production DLQ Behavior

In production, connectors use DLQ (Dead Letter Queue) so they don't crash
on bad messages. Instead, bad messages are quarantined and the pipeline
stays RUNNING.

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
        "sink": "orders-jdbc-sink",
        "topic": "orders-server.public.orders",
        "dlq_topic": "dlq-orders-jdbc-sink",
        "dwh_table": "raw.orders_cdc",
        "insert_cols": "customer_id, total_amount, status",
        "insert_vals": "(1, 999.99, 'completed')",
        "insert_cols_after_drop": "customer_id, status",
        "insert_vals_after_drop": "(1, 'completed')",
    },
    "customers": {
        "db": {"host": "localhost", "port": 5433, "user": "postgres", "password": "postgres", "dbname": "customers_db"},
        "connector": "customers-cdc-connector",
        "sink": "customers-jdbc-sink",
        "topic": "customers-server.public.customers",
        "dlq_topic": "dlq-customers-jdbc-sink",
        "dwh_table": "raw.customers_cdc",
        "insert_cols": "full_name, email, country",
        "insert_vals": "('Test User', 'test@demo.com', 'US')",
        "insert_cols_after_drop": "full_name, country",
        "insert_vals_after_drop": "('Test User', 'US')",
    },
}

KAFKA_CONNECT_URL = "http://localhost:8083"
PROMETHEUS_URL = "http://localhost:9090"

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

def connect_api(method, path, data=None):
    url = f"{KAFKA_CONNECT_URL}{path}"
    req = urllib.request.Request(url, method=method)
    if data:
        req.add_header("Content-Type", "application/json")
        req.data = json.dumps(data).encode("utf-8")
    try:
        with urllib.request.urlopen(req) as resp:
            return resp.status, resp.read().decode("utf-8")
    except urllib.error.HTTPError as e:
        return e.code, e.read().decode("utf-8")

def get_connector_status(name):
    status, body = connect_api("GET", f"/connectors/{name}/status")
    if status == 200:
        return json.loads(body)
    return None

def print_connector_status():
    print("\nConnector Status:")
    for name in ["orders-cdc-connector", "customers-cdc-connector",
                 "orders-jdbc-sink", "customers-jdbc-sink"]:
        st = get_connector_status(name)
        if st:
            cstate = st.get("connector", {}).get("state", "UNKNOWN")
            print(f"  {name:30s} → {cstate}")
            for t in st.get("tasks", []):
                tstate = t.get("state", "UNKNOWN")
                terr = t.get("trace", "")
                print(f"    {t.get('id','task-0')} → {tstate}")
                if terr and tstate == "FAILED":
                    short = terr.replace("\n", " ")[:200]
                    print(f'      Error: "{short}..."')
        else:
            print(f"  {name:30s} → NOT FOUND")

def check_prometheus_alert(alert_name, connector_label=None):
    try:
        req = urllib.request.Request(f"{PROMETHEUS_URL}/api/v1/alerts")
        with urllib.request.urlopen(req, timeout=5) as resp:
            data = json.loads(resp.read().decode("utf-8"))
            alerts = data.get("data", {}).get("alerts", [])
            firing = []
            for a in alerts:
                if a.get("state") != "firing":
                    continue
                labels = a.get("labels", {})
                if labels.get("alertname") == alert_name:
                    if connector_label is None or labels.get("connector") == connector_label:
                        firing.append(a)
            return firing
    except Exception as e:
        print(f"  ⚠ Could not query Prometheus: {e}")
        return []

def get_dwh_count(table):
    try:
        dwh_cfg = {"host": "localhost", "port": 5434, "user": "dwh", "password": "dwh", "dbname": "datamesh_dwh"}
        rows = pg_execute(dwh_cfg, f"SELECT COUNT(*) FROM {table}")
        return rows[0][0] if rows else 0
    except:
        return 0

def create_dlq_topic(topic):
    """Create DLQ topic if it doesn't exist."""
    print(f">>> Ensuring DLQ topic exists: {topic}")
    try:
        result = subprocess.run(
            ["docker", "exec", "kafka", "kafka-topics",
             "--bootstrap-server", "localhost:29092",
             "--create", "--if-not-exists",
             "--topic", topic,
             "--partitions", "1",
             "--replication-factor", "1"],
            capture_output=True, text=True, timeout=10
        )
        if result.returncode == 0 or "already exists" in result.stderr.lower():
            print(f"✓ DLQ topic {topic} ready")
            return True
        else:
            print(f"⚠ Could not create DLQ topic: {result.stderr.strip()}")
            return False
    except Exception as e:
        print(f"⚠ Error creating DLQ topic: {e}")
        return False

def consume_dlq(topic, max_messages=5):
    """Read messages from DLQ topic."""
    try:
        result = subprocess.run(
            ["docker", "exec", "kafka", "kafka-console-consumer",
             "--bootstrap-server", "localhost:29092",
             "--topic", topic,
             "--from-beginning",
             "--max-messages", str(max_messages),
             "--timeout-ms", "5000"],
            capture_output=True, text=True, timeout=15
        )
        return result.stdout.strip()
    except Exception as e:
        return f"Error reading DLQ: {e}"

# ─── Main ────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="CDC Breaking Change Demo with DLQ")
    parser.add_argument("--table", required=True, choices=["orders", "customers"])
    parser.add_argument("--column", required=True, help="Column to drop")
    args = parser.parse_args()

    cfg = CONNECTORS[args.table]
    col = args.column

    # ── Pre-flight: ensure DLQ topic exists ───────────────────────────
    create_dlq_topic(cfg["dlq_topic"])

    # ── Step 1: Baseline ──────────────────────────────────────────────
    log_step("STEP 1: BASELINE — All connectors healthy")
    print_connector_status()
    baseline_count = get_dwh_count(cfg["dwh_table"])
    print(f"\n  DWH baseline: {baseline_count} rows in {cfg['dwh_table']}")

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
    print_connector_status()
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
        print(f"  (Debezium serializes → Kafka message lacks '{col}' → JDBC Sink sends to DLQ)")
    except Exception as e:
        print(f"✗ Insert failed: {e}")
        print("  (Unexpected — insert without dropped column should pass)")

    # ── Step 6: Monitor pipeline (Production DLQ behavior) ──────────────
    log_step("STEP 6: Monitor pipeline (Production DLQ behavior)")
    print("""
  In production with DLQ enabled:
    - Connector stays RUNNING (does not crash)
    - Bad message goes to DLQ topic
    - DWH stops receiving new data
    - Alert CDC_SinkStall fires after 2 minutes
""")

    prev_count = get_dwh_count(cfg["dwh_table"])
    dlq_seen = False
    for i in range(1, 7):
        print(f"\nCheck {i}/6 (waiting 5s)...")
        time.sleep(5)
        st = get_connector_status(cfg["sink"])
        curr_count = get_dwh_count(cfg["dwh_table"])
        if st:
            cstate = st.get("connector", {}).get("state", "UNKNOWN")
            tasks = st.get("tasks", [])
            tstate = tasks[0].get("state", "UNKNOWN") if tasks else "UNKNOWN"
            print(f"  {cfg['sink']}: connector={cstate}, task={tstate}")
            print(f"  DWH rows: {curr_count} (delta: {curr_count - prev_count})")

            # Check if message landed in DLQ
            if not dlq_seen and curr_count == prev_count:
                dlq_content = consume_dlq(cfg["dlq_topic"], 1)
                if dlq_content and "Error" not in dlq_content:
                    dlq_seen = True
                    print(f"\n  ✓ Bad message quarantined in DLQ: {cfg['dlq_topic']}")
                    print(f"    DLQ content preview: {dlq_content[:200]}...")
        prev_count = curr_count

    print_connector_status()

    # ── Step 7: DLQ Contents ──────────────────────────────────────────
    log_step("STEP 7: DLQ Contents")
    dlq_content = consume_dlq(cfg["dlq_topic"], 3)
    if dlq_content and "Error" not in dlq_content:
        print(f"\n✓ DLQ topic '{cfg['dlq_topic']}' contains quarantined messages:")
        print(f"  {dlq_content[:500]}...")
    else:
        print(f"\n⚠ DLQ topic '{cfg['dlq_topic']}' not found or empty")
        print("  (May need a few more seconds for Kafka to create the topic)")

    # ── Step 8: Check Prometheus alerts ─────────────────────────────────────
    log_step("STEP 8: Check Prometheus alerts")
    alerts = check_prometheus_alert("CDC_SinkStall", cfg["sink"])
    if alerts:
        print("\n🚨 ALERT FIRING!")
        for a in alerts:
            labels = a.get("labels", {})
            print(f"  Alert: {labels.get('alertname')}")
            print(f"  Connector: {labels.get('connector', 'N/A')}")
            print(f"  Severity: {labels.get('severity', 'N/A')}")
    else:
        print("\n⚠ No CDC_SinkStall alert firing yet")
        print("  (JMX metrics need 2+ minutes of stall to trigger)")
        print("  Check manually:")
        print(f"    - Grafana: http://localhost:3000/alerting/list")
        print(f"    - Prometheus: http://localhost:9090/alerts")

    # ── Step 9: Recovery — Restore column, pipeline resumes ────────────
    log_step("STEP 9: RECOVERY — Restore column, pipeline resumes")
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
    print_connector_status()
    final_count = get_dwh_count(cfg["dwh_table"])
    print(f"\n  DWH final count: {final_count} rows")

    # ── Step 10: Final state ──────────────────────────────────────────
    log_step("STEP 10: FINAL STATE & PRODUCTION TAKEAWAYS")
    print("""
╔══════════════════════════════════════════════════════════════════════╗
║  PRODUCTION TAKEAWAYS — Schema-on-Write with DLQ                     ║
║                                                                      ║
║  1. DLQ prevents connector crash — pipeline stays RUNNING            ║
║  2. Bad messages are quarantined, not lost                           ║
║  3. DWH stops growing → alert fires → ops investigates               ║
║  4. Recovery: fix schema → new data flows automatically            ║
║  5. DLQ messages can be replayed or analyzed later                 ║
║                                                                      ║
║  HOW TO FIX THE BAD MESSAGE IN PROD:                               ║
║    a) Fix schema (add column back)                                   ║
║    b) Consume DLQ, fix data, re-produce to source topic            ║
║    c) Or: skip the message and accept data loss                      ║
║                                                                      ║
║  WHY NOT JUST RESTART?                                               ║
║    Restarting doesn't help — the bad message is still in Kafka.    ║
║    Without DLQ: connector crashes in a loop.                         ║
║    With DLQ: message is skipped, pipeline continues.                 ║
╚══════════════════════════════════════════════════════════════════════╝
""")

if __name__ == "__main__":
    main()
