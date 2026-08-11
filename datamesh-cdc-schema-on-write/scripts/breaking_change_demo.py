#!/usr/bin/env python3
"""
CDC Breaking Change Demo — Simulate a breaking schema change and watch the pipeline FAIL.

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

def docker_exec(cmd, capture=True):
    """Run a command inside the kafka container."""
    full = ["docker", "exec", "kafka"] + cmd
    if capture:
        result = subprocess.run(full, capture_output=True, text=True)
        return result.stdout, result.stderr, result.returncode
    else:
        subprocess.run(full)
        return "", "", 0

def delete_kafka_topic(topic):
    """Delete and recreate a Kafka topic to purge bad messages."""
    print(f">>> Deleting Kafka topic: {topic}")
    out, err, rc = docker_exec([
        "kafka-topics", "--bootstrap-server", "localhost:29092",
        "--delete", "--topic", topic
    ])
    if rc == 0 or "does not exist" in err.lower():
        print(f"✓ Topic {topic} deleted (or did not exist)")
    else:
        print(f"⚠ Could not delete topic: {err.strip() or out.strip()}")
    time.sleep(2)

def delete_connector(name):
    """DELETE a connector from Kafka Connect."""
    print(f">>> Deleting connector: {name}")
    status, body = connect_api("DELETE", f"/connectors/{name}")
    if status in (200, 204, 404):
        print(f"✓ Connector {name} deleted")
    else:
        print(f"⚠ Could not delete connector: HTTP {status}")
    time.sleep(1)

def recreate_sink_connector(table, topic):
    """Recreate the JDBC sink connector so it starts fresh."""
    sink_name = f"{table}-jdbc-sink"
    dwh_table = CONNECTORS[table]["dwh_table"]

    config = {
        "name": sink_name,
        "config": {
            "connector.class": "io.confluent.connect.jdbc.JdbcSinkConnector",
            "tasks.max": "1",
            "topics": topic,
            "connection.url": "jdbc:postgresql://postgres-dwh:5432/datamesh_dwh",
            "connection.user": "dwh",
            "connection.password": "dwh",
            "auto.create": "true",
            "auto.evolve": "true",
            "insert.mode": "upsert",
            "pk.mode": "record_key",
            "pk.fields": "id",
            "delete.enabled": "true",
            "table.name.format": dwh_table,
            "value.converter": "io.confluent.connect.avro.AvroConverter",
            "value.converter.schema.registry.url": "http://schema-registry:8081",
            "key.converter": "io.confluent.connect.avro.AvroConverter",
            "key.converter.schema.registry.url": "http://schema-registry:8081"
        }
    }

    print(f">>> Recreating sink connector: {sink_name}")
    status, body = connect_api("POST", "/connectors", config)
    if status == 201:
        print(f"✓ Sink connector {sink_name} recreated")
    else:
        print(f"⚠ Failed to recreate sink: HTTP {status} — {body}")

# ─── Main ────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="CDC Breaking Change Demo")
    parser.add_argument("--table", required=True, choices=["orders", "customers"])
    parser.add_argument("--column", required=True, help="Column to drop")
    args = parser.parse_args()

    cfg = CONNECTORS[args.table]
    col = args.column

    # ── Step 1: Baseline ──────────────────────────────────────────────
    log_step("STEP 1: BASELINE — All connectors healthy")
    print_connector_status()

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
    time.sleep(2)
    print_connector_status()

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
        print("  (Debezium will now try to serialize with old Avro schema and FAIL)")
    except Exception as e:
        print(f"✗ Insert failed: {e}")
        print("  (This is unexpected — the insert without dropped column should pass)")

    # ── Step 6: Monitor connector (should show FAILED) ──────────────
    log_step("STEP 6: Monitor connector status (should show FAILED)")
    failed = False
    for i in range(1, 7):
        print(f"\nCheck {i}/6 (waiting 5s)...")
        time.sleep(5)
        st = get_connector_status(cfg["connector"])
        if st:
            cstate = st.get("connector", {}).get("state", "UNKNOWN")
            tasks = st.get("tasks", [])
            tstate = tasks[0].get("state", "UNKNOWN") if tasks else "UNKNOWN"
            print(f"  {cfg['connector']}: connector={cstate}, task={tstate}")
            if tstate == "FAILED" or cstate == "FAILED":
                failed = True
                print_connector_status()
                break
        else:
            print("  Could not fetch status")

    if not failed:
        print("\n⚠ Source connector did not fail within 30s.")
        print("  Checking if SINK connector failed instead...")
        st = get_connector_status(cfg["sink"])
        if st:
            for t in st.get("tasks", []):
                if t.get("state") == "FAILED":
                    failed = True
                    print(f"  ✓ SINK connector {cfg['sink']} is FAILED (expected)")
                    print_connector_status()
                    break

    if not failed:
        print("\n⚠ Neither source nor sink connector failed.")
        print("  This can happen if Debezium has not yet processed the WAL entry.")
        print("  The pipeline may still fail on the next poll cycle.")
        print_connector_status()

    # ── Step 7: Prometheus alerts ─────────────────────────────────────
    log_step("STEP 7: Check Prometheus alerts")
    alerts = check_prometheus_alert("CDC_ConnectorTaskFailed", cfg["connector"])
    if alerts:
        print("\n🚨 ALERT FIRING!")
        for a in alerts:
            labels = a.get("labels", {})
            print(f"  Alert: {labels.get('alertname')}")
            print(f"  Connector: {labels.get('connector', 'N/A')}")
            print(f"  Severity: {labels.get('severity', 'N/A')}")
            print(f"  State: {a.get('state')}")
    else:
        print("\n⚠ No CDC_ConnectorTaskFailed alert firing yet")
        print("  Check manually:")
        print(f"    - Grafana: http://localhost:3000/alerting/list")
        print(f"    - Prometheus: http://localhost:9090/alerts")
        print(f"    - Connector logs: docker compose logs kafka-connect --tail=50")

    # ── Step 8: Recovery ────────────────────────────────────────────
    log_step("STEP 8: RECOVERY — Restore column, clean Kafka, recreate sink")

    # 8a. Restore column
    print(f"\n>>> RESTORING COLUMN:")
    print(f"    ALTER TABLE {args.table} ADD COLUMN {col} DECIMAL(12,2);")
    try:
        pg_execute(cfg["db"], f"ALTER TABLE {args.table} ADD COLUMN {col} DECIMAL(12,2)")
        print("✓ Column restored")
    except Exception as e:
        print(f"✗ Failed to restore column: {e}")

    # 8b. Delete bad messages from Kafka topic
    print(f"\n>>> CLEANUP: Removing bad messages from Kafka topic {cfg['topic']}")
    delete_kafka_topic(cfg["topic"])

    # 8c. Drop the corrupted DWH table so sink recreates it cleanly
    print(f">>> CLEANUP: Dropping corrupted DWH table {cfg['dwh_table']}")
    try:
        dwh_cfg = {"host": "localhost", "port": 5434, "user": "dwh", "password": "dwh", "dbname": "datamesh_dwh"}
        pg_execute(dwh_cfg, f"DROP TABLE IF EXISTS {cfg['dwh_table']}")
        print(f"✓ Dropped {cfg['dwh_table']}")
    except Exception as e:
        print(f"⚠ Could not drop DWH table: {e}")

    # 8d. Restart source connector
    print(f"\n>>> Restarting source connector: {cfg['connector']}")
    status, body = connect_api("POST", f"/connectors/{cfg['connector']}/restart?includeTasks=true&onlyFailed=false")
    if status in (200, 202, 204):
        print("✓ Source restart triggered")
    else:
        print(f"✗ Source restart failed: HTTP {status} — {body}")

    # 8e. Delete and recreate sink connector
    print(f"\n>>> Recreating sink connector: {cfg['sink']}")
    delete_connector(cfg["sink"])
    time.sleep(2)
    recreate_sink_connector(args.table, cfg["topic"])

    print("\nWaiting 10s for recovery...")
    time.sleep(10)
    print_connector_status()

    # 8f. Verify with clean insert
    print(f"\n>>> Verifying with clean insert...")
    try:
        pg_execute(cfg["db"],
            f"INSERT INTO {args.table} ({cfg['insert_cols']}) VALUES {cfg['insert_vals']}")
        print("✓ Clean insert successful")
    except Exception as e:
        print(f"✗ Clean insert failed: {e}")

    time.sleep(5)
    print_connector_status()

    # 8g. Final verification
    print(f"\n>>> Final data verification...")
    time.sleep(3)
    try:
        dwh_cfg = {"host": "localhost", "port": 5434, "user": "dwh", "password": "dwh", "dbname": "datamesh_dwh"}
        rows = pg_execute(dwh_cfg, f"SELECT COUNT(*) FROM {cfg['dwh_table']}")
        if rows:
            print(f"✓ DWH table {cfg['dwh_table']}: {rows[0][0]} rows")
    except Exception as e:
        print(f"✗ Could not query DWH: {e}")

    print("\n✓ Demo complete. Pipeline restored.")

if __name__ == "__main__":
    main()
