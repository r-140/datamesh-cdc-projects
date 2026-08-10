#!/usr/bin/env python3
"""
Breaking Change Demo — Schema Evolution Failure
================================================

Simulates a breaking schema change (DROP COLUMN) on the source database
and demonstrates how the CDC pipeline detects and alerts on the failure.

Usage:
    python scripts/breaking_change_demo.py --table orders --column total_amount
    python scripts/breaking_change_demo.py --table customers --column email

What happens:
    1. Shows current connector status (all RUNNING)
    2. Generates some data to establish baseline
    3. Executes ALTER TABLE ... DROP COLUMN (breaking change!)
    4. Generates new data (insert fails due to schema mismatch)
    5. Monitors connector status — shows FAILED state
    6. Shows Prometheus alert firing (CDC_Connector_Down)
    7. Restores column and restarts connector (recovery)

Requirements:
    - All containers running (make up)
    - Prometheus scraping Kafka Connect JMX (:7071)
"""

import argparse
import subprocess
import sys
import time
import urllib.request
import json

KAFKA_CONNECT_URL = "http://localhost:8083"
PROMETHEUS_URL = "http://localhost:9090"


class Colors:
    HEADER = "\033[95m"
    OKBLUE = "\033[94m"
    OKCYAN = "\033[96m"
    OKGREEN = "\033[92m"
    WARNING = "\033[93m"
    FAIL = "\033[91m"
    BOLD = "\033[1m"
    END = "\033[0m"


def print_banner():
    print(f"""
{Colors.FAIL}{Colors.BOLD}
   ____  _  _   ___  _   _  ____  ___  _   _ 
  | __ )| || | / _ \| | | |/ ___|/ _ \| \ | |
  |  _ \| || || | | | | | | |  _| | | |  \| |
  | |_) |__   _| |_| | |_| | |_| | |_| | |\  |
  |____/   |_|  \___/ \___/ \____|\___/|_| \_|
{Colors.END}
  {Colors.WARNING}CDC Breaking Change Demo — Watch the pipeline FAIL{Colors.END}
""")


def print_section(title):
    print(f"\n{Colors.BOLD}{'='*70}{Colors.END}")
    print(f"{Colors.OKBLUE}{title:^70}{Colors.END}")
    print(f"{Colors.BOLD}{'='*70}{Colors.END}")


def get_connector_status(name):
    try:
        req = urllib.request.Request(f"{KAFKA_CONNECT_URL}/connectors/{name}/status")
        with urllib.request.urlopen(req, timeout=5) as resp:
            return json.loads(resp.read().decode())
    except Exception as e:
        return {"error": str(e)}


def print_connector_status():
    connectors = [
        "orders-cdc-connector",
        "customers-cdc-connector",
        "orders-jdbc-sink",
        "customers-jdbc-sink",
    ]
    print(f"\n{Colors.BOLD}Connector Status:{Colors.END}")
    for name in connectors:
        status = get_connector_status(name)
        if "error" in status:
            print(f"  {name:30s} → {Colors.FAIL}ERROR: {status['error']}{Colors.END}")
        else:
            state = status.get("connector", {}).get("state", "UNKNOWN")
            color = Colors.OKGREEN if state == "RUNNING" else Colors.FAIL if state == "FAILED" else Colors.WARNING
            print(f"  {name:30s} → {color}{state}{Colors.END}")
            # Check tasks
            for task in status.get("tasks", []):
                t_state = task.get("state", "UNKNOWN")
                t_color = Colors.OKGREEN if t_state == "RUNNING" else Colors.FAIL if t_state == "FAILED" else Colors.WARNING
                print(f"    task-{task.get('id', '?')} → {t_color}{t_state}{Colors.END}")


def check_prometheus_alert():
    """Check if CDC_Connector_Down alert is firing."""
    try:
        req = urllib.request.Request(
            f"{PROMETHEUS_URL}/api/v1/alerts"
        )
        with urllib.request.urlopen(req, timeout=5) as resp:
            data = json.loads(resp.read().decode())
            alerts = data.get("data", {}).get("alerts", [])
            cdc_alerts = [a for a in alerts if "CDC_Connector_Down" in a.get("labels", {}).get("alertname", "")]
            return cdc_alerts
    except Exception as e:
        print(f"{Colors.WARNING}Could not query Prometheus: {e}{Colors.END}")
        return []


def get_table_columns(db, table):
    result = subprocess.run(
        ["docker", "exec", f"postgres-{db}", "psql", "-U", "postgres", "-d", f"{db}_db", "-c",
         f"SELECT column_name FROM information_schema.columns WHERE table_name = '{table}' ORDER BY ordinal_position;"],
        capture_output=True, text=True, timeout=10,
    )
    return result.stdout


def drop_column(db, table, column):
    print(f"\n{Colors.FAIL}{Colors.BOLD}>>> EXECUTING BREAKING CHANGE:{Colors.END}")
    print(f"{Colors.FAIL}    ALTER TABLE {table} DROP COLUMN {column};{Colors.END}\n")
    result = subprocess.run(
        ["docker", "exec", f"postgres-{db}", "psql", "-U", "postgres", "-d", f"{db}_db", "-c",
         f"ALTER TABLE {table} DROP COLUMN {column};"],
        capture_output=True, text=True, timeout=10,
    )
    if result.returncode == 0:
        print(f"{Colors.OKGREEN}✓ Column dropped successfully{Colors.END}")
    else:
        print(f"{Colors.FAIL}✗ Error: {result.stderr}{Colors.END}")
        sys.exit(1)


def add_column_back(db, table, column, col_type):
    print(f"\n{Colors.OKCYAN}>>> RESTORING COLUMN:{Colors.END}")
    print(f"{Colors.OKCYAN}    ALTER TABLE {table} ADD COLUMN {column} {col_type};{Colors.END}\n")
    result = subprocess.run(
        ["docker", "exec", f"postgres-{db}", "psql", "-U", "postgres", "-d", f"{db}_db", "-c",
         f"ALTER TABLE {table} ADD COLUMN {column} {col_type};"],
        capture_output=True, text=True, timeout=10,
    )
    if result.returncode == 0:
        print(f"{Colors.OKGREEN}✓ Column restored{Colors.END}")
    else:
        print(f"{Colors.FAIL}✗ Error: {result.stderr}{Colors.END}")


def insert_test_data(db, table):
    print(f"\n{Colors.OKCYAN}>>> Inserting test data into {db}.{table}...{Colors.END}")
    if table == "orders":
        result = subprocess.run(
            ["docker", "exec", f"postgres-{db}", "psql", "-U", "postgres", "-d", f"{db}_db", "-c",
             "INSERT INTO orders (customer_id, total_amount, status) VALUES (1, 999.99, 'pending') RETURNING id;"],
            capture_output=True, text=True, timeout=10,
        )
    else:
        result = subprocess.run(
            ["docker", "exec", f"postgres-{db}", "psql", "-U", "postgres", "-d", f"{db}_db", "-c",
             "INSERT INTO customers (email, full_name, country) VALUES ('test@demo.com', 'Test User', 'US') RETURNING id;"],
            capture_output=True, text=True, timeout=10,
        )
    if result.returncode == 0:
        print(f"{Colors.OKGREEN}✓ Insert successful:{Colors.END} {result.stdout.strip()}")
    else:
        print(f"{Colors.FAIL}✗ Insert FAILED (expected after breaking change):{Colors.END}")
        print(f"   {result.stderr.strip()}")
    return result.returncode == 0


def restart_connector(name):
    print(f"\n{Colors.OKCYAN}>>> Restarting connector: {name}{Colors.END}")
    try:
        req = urllib.request.Request(
            f"{KAFKA_CONNECT_URL}/connectors/{name}/restart?includeTasks=true&onlyFailed=true",
            method="POST",
        )
        with urllib.request.urlopen(req, timeout=10) as resp:
            print(f"{Colors.OKGREEN}✓ Restart triggered (HTTP {resp.status}){Colors.END}")
    except Exception as e:
        print(f"{Colors.WARNING}⚠ Restart failed: {e}{Colors.END}")


def main():
    parser = argparse.ArgumentParser(description="CDC Breaking Change Demo")
    parser.add_argument("--table", "-t", required=True, choices=["orders", "customers"],
                        help="Table to break")
    parser.add_argument("--column", "-c", required=True,
                        help="Column to drop (breaking change)")
    parser.add_argument("--no-restore", action="store_true",
                        help="Skip column restoration (manual cleanup needed)")
    parser.add_argument("--wait", "-w", type=int, default=30,
                        help="Seconds to wait for alert to fire (default: 30)")

    args = parser.parse_args()

    db = args.table  # orders -> postgres-orders, customers -> postgres-customers
    table = args.table
    column = args.column
    col_type = "DECIMAL(12,2)" if table == "orders" and column == "total_amount" else "VARCHAR(255)"

    print_banner()

    # Step 1: Baseline
    print_section("STEP 1: BASELINE — All connectors healthy")
    print_connector_status()

    # Step 2: Show current schema
    print_section(f"STEP 2: Current schema of {table}")
    print(get_table_columns(db, table))

    # Step 3: Generate some data
    print_section("STEP 3: Generate baseline data")
    insert_test_data(db, table)
    time.sleep(3)
    print_connector_status()

    # Step 4: BREAKING CHANGE
    print_section(f"STEP 4: BREAKING CHANGE — DROP COLUMN {column}")
    drop_column(db, table, column)

    # Step 5: Try to insert data (will fail or cause schema mismatch)
    print_section("STEP 5: Attempt insert after breaking change")
    success = insert_test_data(db, table)
    if success:
        print(f"{Colors.WARNING}⚠ Insert succeeded — Debezium may not have picked up the change yet{Colors.END}")
    else:
        print(f"{Colors.OKGREEN}✓ Insert failed as expected — schema mismatch detected{Colors.END}")

    # Step 6: Monitor connector failure
    print_section("STEP 6: Monitor connector status (should show FAILED)")
    for i in range(5):
        print(f"\n{Colors.BOLD}Check {i+1}/5 (waiting 5s)...{Colors.END}")
        time.sleep(5)
        print_connector_status()

    # Step 7: Check Prometheus alert
    print_section("STEP 7: Check Prometheus alerts")
    alerts = check_prometheus_alert()
    if alerts:
        print(f"{Colors.FAIL}{Colors.BOLD}🚨 ALERT FIRING!{Colors.END}")
        for alert in alerts:
            print(f"  Alert: {alert['labels'].get('alertname')}")
            print(f"  Connector: {alert['labels'].get('connector', 'N/A')}")
            print(f"  Severity: {alert['labels'].get('severity')}")
            print(f"  State: {alert['state']}")
            print(f"  Description: {alert['annotations'].get('description', 'N/A')[:100]}...")
    else:
        print(f"{Colors.WARNING}⚠ No CDC_Connector_Down alert firing yet{Colors.END}")
        print(f"  {Colors.OKCYAN}Check manually:{Colors.END} http://localhost:9090/alerts")
        print(f"  {Colors.OKCYAN}Or check connector logs:{Colors.END} docker compose logs kafka-connect --tail=50")

    # Step 8: Recovery (optional)
    if not args.no_restore:
        print_section("STEP 8: RECOVERY — Restore column and restart connector")
        add_column_back(db, table, column, col_type)
        restart_connector(f"{table}-cdc-connector")
        restart_connector(f"{table}-jdbc-sink")
        print(f"\n{Colors.OKCYAN}Waiting 10s for recovery...{Colors.END}")
        time.sleep(10)
        print_connector_status()
        print(f"\n{Colors.OKGREEN}{Colors.BOLD}✓ Demo complete. Pipeline restored.{Colors.END}")
    else:
        print_section("STEP 8: RECOVERY SKIPPED")
        print(f"{Colors.WARNING}Column NOT restored. Manual cleanup required:{Colors.END}")
        print(f"  docker exec postgres-{db} psql -U postgres -d {db}_db -c \"ALTER TABLE {table} ADD COLUMN {column} {col_type};\"")
        print(f"  curl -X POST http://localhost:8083/connectors/{table}-cdc-connector/restart?includeTasks=true")

    print(f"\n{Colors.BOLD}{'='*70}{Colors.END}")
    print(f"{Colors.OKCYAN}Grafana Alert Dashboard:{Colors.END} http://localhost:3000")
    print(f"{Colors.OKCYAN}Prometheus Alerts:{Colors.END}      http://localhost:9090/alerts")
    print(f"{Colors.OKCYAN}Kafka Connect REST:{Colors.END}   http://localhost:8083/connectors")


if __name__ == "__main__":
    main()
