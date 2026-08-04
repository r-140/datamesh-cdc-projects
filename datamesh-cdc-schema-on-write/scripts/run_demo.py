#!/usr/bin/env python3
"""
CDC Data Mesh -- Automated End-to-End Demo

Automates the step-by-step guide:
  1. Insert test data into PostgreSQL
  2. Verify Kafka topics
  3. Read Avro messages
  4. Inspect Schema Registry
  5. ALTER TABLE ADD COLUMN (compatible)
  6. Insert with new column
  7. Read updated messages
  8. Verify Schema Registry v2 + history
  9. ALTER TABLE DROP COLUMN (breaking)
  10. Try insert -> connector fails
  11. Check connector status (with retries)
  12. Fix: restore column + restart connector
  13. Verify recovery

Usage:
    python scripts/run_demo.py
"""

import json
import subprocess
import sys
import time
from typing import Optional

# -- Configuration -----------------------------------------------------
SR_URL = "http://localhost:8081"
KC_URL = "http://localhost:8083"
ORDERS_SUBJECT = "orders-server.public.orders-value"
TOPIC = "orders-server.public.orders"
CONNECTOR = "orders-cdc-connector"

# -- Colours -----------------------------------------------------------
class C:
    GREEN = "\033[92m"
    RED = "\033[91m"
    YELLOW = "\033[93m"
    BLUE = "\033[94m"
    CYAN = "\033[96m"
    BOLD = "\033[1m"
    END = "\033[0m"


def banner(text: str):
    print(f"\n{C.BOLD}{C.BLUE}{'=' * 70}{C.END}")
    print(f"{C.BOLD}{C.BLUE}{text.center(70)}{C.END}")
    print(f"{C.BOLD}{C.BLUE}{'=' * 70}{C.END}")


def ok(msg: str):
    print(f"{C.GREEN}\u2713{C.END} {msg}")


def err(msg: str):
    print(f"{C.RED}\u2717{C.END} {msg}", file=sys.stderr)


def info(msg: str):
    print(f"{C.CYAN}\u2139{C.END} {msg}")


def warn(msg: str):
    print(f"{C.YELLOW}\u26a0{C.END} {msg}")

# -- Helpers -----------------------------------------------------------
def run(cmd: list[str], capture=True, check=True) -> subprocess.CompletedProcess:
    """Run a shell command via subprocess."""
    result = subprocess.run(
        cmd,
        capture_output=capture,
        text=True,
        check=False
    )
    if check and result.returncode != 0:
        err(f"Command failed: {' '.join(cmd)}")
        if result.stderr:
            print(result.stderr, file=sys.stderr)
        raise RuntimeError(f"Command failed: {' '.join(cmd)}")
    return result


def docker_exec(container: str, cmd: str) -> str:
    """Run a command inside a Docker container."""
    result = run(["docker", "exec", container, "bash", "-c", cmd])
    return result.stdout.strip()


def psql(db_container: str, db: str, sql: str) -> str:
    """Execute SQL via docker exec psql."""
    return docker_exec(
        db_container,
        f'psql -U postgres -d {db} -c "{sql}"'
    )


def curl_json(url: str, method: str = "GET", data: Optional[str] = None) -> dict:
    """Simple HTTP request returning JSON."""
    cmd = ["curl", "-s", "-X", method, url]
    if data:
        cmd += ["-H", "Content-Type: application/json", "-d", data]
    result = run(cmd)
    try:
        return json.loads(result.stdout) if result.stdout else {}
    except json.JSONDecodeError:
        return {"raw": result.stdout}


def wait_for_kafka_topic(topic: str, timeout: int = 30) -> bool:
    """Poll until topic appears in Kafka."""
    info(f"Waiting for topic '{topic}' to appear...")
    for i in range(timeout):
        out = docker_exec("kafka", "kafka-topics --bootstrap-server localhost:29092 --list")
        if topic in out:
            ok(f"Topic '{topic}' found after {i+1}s")
            return True
        time.sleep(1)
    err(f"Topic '{topic}' did not appear within {timeout}s")
    return False


def read_avro_message(topic: str, max_messages: int = 1) -> str:
    """Read Avro messages using kafka-avro-console-consumer."""
    result = run([
        "docker", "exec", "kafka", "kafka-avro-console-consumer",
        "--bootstrap-server", "localhost:29092",
        "--topic", topic,
        "--from-beginning",
        "--property", "schema.registry.url=http://schema-registry:8081",
        "--max-messages", str(max_messages),
        "--timeout-ms", "5000"
    ], capture=True, check=False)
    return result.stdout.strip()


def show_schema_history(subject: str):
    """Fetch and display all schema versions with field diffs."""
    versions = curl_json(f"{SR_URL}/subjects/{subject}/versions")
    if not isinstance(versions, list) or not versions:
        warn(f"No versions found for {subject}")
        return

    info(f"Schema history for '{subject}':")
    print(f"\n  {'Ver':<5} {'ID':<6} {'Fields':<50} {'Changes':<30}")
    print(f"  {'-'*5} {'-'*6} {'-'*50} {'-'*30}")

    prev_fields = set()
    for ver in sorted(versions):
        schema_data = curl_json(f"{SR_URL}/subjects/{subject}/versions/{ver}")
        sid = schema_data.get("id", "?")
        raw_schema = schema_data.get("schema", "{}")
        try:
            parsed = json.loads(raw_schema)
            fields = [f["name"] for f in parsed.get("fields", [])]
        except Exception:
            fields = []

        field_str = ", ".join(fields)
        if len(field_str) > 47:
            field_str = field_str[:44] + "..."

        curr_set = set(fields)
        added = curr_set - prev_fields
        removed = prev_fields - curr_set
        if prev_fields:
            if added and removed:
                note = f"+{list(added)[0]}, -{list(removed)[0]}"
            elif added:
                note = f"+{', +'.join(list(added)[:2])}"
            elif removed:
                note = f"-{', -'.join(list(removed)[:2])}"
            else:
                note = "no change"
        else:
            note = "initial"
        prev_fields = curr_set

        print(f"  {ver:<5} {sid:<6} {field_str:<50} {note:<30}")
    print()


def cleanup_tables():
    """Clean tables and reset sequences for a fresh demo run."""
    banner("STEP 0: Clean Up Tables")
    try:
        psql("postgres-orders", "orders_db",
             "DROP TABLE IF EXISTS orders CASCADE; "
             "CREATE TABLE orders (id SERIAL PRIMARY KEY, customer_id INT NOT NULL, "
             "total_amount NUMERIC(10,2) NOT NULL, status VARCHAR(50) DEFAULT 'pending', "
             "created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP);")
        ok("Recreated orders table (clean schema)")
    except Exception as e:
        warn(f"Could not recreate orders: {e}")
    try:
        psql("postgres-customers", "customers_db",
             "DELETE FROM customers; ALTER SEQUENCE customers_id_seq RESTART WITH 1;")
        ok("Cleaned customers table")
    except Exception as e:
        warn(f"Could not clean customers: {e}")

# -- Main Demo ---------------------------------------------------------
def main():
    print(f"{C.BOLD}{C.CYAN}")
    print(r"   ____  ____   ____    ____  __  __ ____   ___  _  __")
    print(r"  / ___||  _ \ |  _ \  |  _ \|  \/  |  _ \ / _ \| |/ /")
    print(r"  \___ \| | | || | | | | | | | |\/| | |_) | | | | ' / ")
    print(r"   ___) | |_| || |_| | | |_| | |  | |  __/| |_| | . \ ")
    print(r"  |____/|____/ |____/  |____/|_|  |_|_|    \___/|_|\_\\")
    print(f"{C.END}")
    info("Starting automated CDC Data Mesh demo...")

    # -- Step 0: Clean tables -----------------------------------------
    cleanup_tables()

    # -- Step 1: Insert test data -------------------------------------
    banner("STEP 1: Insert Test Data into PostgreSQL")
    try:
        out = psql("postgres-orders", "orders_db",
                   "INSERT INTO orders (customer_id, total_amount, status) "
                   "VALUES (1, 150.00, 'confirmed') RETURNING id;")
        ok("Inserted order (id=1)")
        print(out)
    except Exception as e:
        warn(f"Insert may have failed (already exists?): {e}")

    try:
        out = psql("postgres-customers", "customers_db",
                   "INSERT INTO customers (email, full_name, country) "
                   "VALUES ('alice@example.com', 'Alice Smith', 'US') RETURNING id;")
        ok("Inserted customer (id=1)")
        print(out)
    except Exception as e:
        warn(f"Insert may have failed (already exists?): {e}")

    # -- Step 2: Verify topics ----------------------------------------
    banner("STEP 2: Verify Kafka Topics")
    if wait_for_kafka_topic(TOPIC):
        topics = docker_exec("kafka", "kafka-topics --bootstrap-server localhost:29092 --list")
        info("Available topics:")
        for t in topics.splitlines():
            print(f"    - {t}")

    # -- Step 3: Read Avro message ------------------------------------
    banner("STEP 3: Read Avro Message from Kafka")
    info("Reading from topic 'orders-server.public.orders'...")
    msg = read_avro_message(TOPIC, max_messages=1)
    if msg:
        ok("Message received:")
        try:
            parsed = json.loads(msg)
            print(json.dumps(parsed, indent=2))
        except json.JSONDecodeError:
            print(msg)
    else:
        warn("No message received yet. Debezium may still be snapshotting.")
        info("Waiting 5s and retrying...")
        time.sleep(5)
        msg = read_avro_message(TOPIC, max_messages=1)
        if msg:
            ok("Message received after retry:")
            print(msg)
        else:
            err("Still no message. Check connector status manually.")

    # -- Step 4: Inspect Schema Registry -----------------------------
    banner("STEP 4: Inspect Schema Registry")
    subjects = curl_json(f"{SR_URL}/subjects")
    if subjects:
        ok(f"Found {len(subjects)} subject(s):")
        for s in subjects:
            print(f"    - {s}")

    schema_info = curl_json(f"{SR_URL}/subjects/{ORDERS_SUBJECT}/versions/latest")
    if "id" in schema_info:
        ok(f"Schema '{ORDERS_SUBJECT}' latest version:")
        print(f"    Version: {schema_info.get('version')}")
        print(f"    Schema ID: {schema_info.get('id')}")
        try:
            schema_json = json.loads(schema_info.get('schema', '{}'))
            fields = [f["name"] for f in schema_json.get("fields", [])]
            print(f"    Fields: {fields}")
        except Exception:
            pass
    else:
        warn(f"Subject '{ORDERS_SUBJECT}' not found yet.")

    # -- Step 5: Compatible change -- ADD COLUMN ----------------------
    banner("STEP 5: ALTER TABLE ADD COLUMN (Compatible Change)")
    psql("postgres-orders", "orders_db",
         "ALTER TABLE orders ADD COLUMN IF NOT EXISTS promo_code VARCHAR(50) DEFAULT NULL;")
    ok("Added column 'promo_code' to orders table")

    # -- Step 6: Insert with new column -------------------------------
    banner("STEP 6: Insert Record with New Column")
    psql("postgres-orders", "orders_db",
         "INSERT INTO orders (customer_id, total_amount, status, promo_code) "
         "VALUES (2, 99.99, 'pending', 'SUMMER2024');")
    ok("Inserted order with promo_code='SUMMER2024'")

    # -- Step 7: Read updated message ---------------------------------
    banner("STEP 7: Read Updated Message")
    info("Waiting 3s for Debezium to capture the change...")
    time.sleep(3)
    msg = read_avro_message(TOPIC, max_messages=1)
    if msg and "SUMMER2024" in msg:
        ok("New message contains 'promo_code':")
        print(msg)
    elif msg:
        ok("Message received (may need to consume more to see latest):")
        print(msg)
    else:
        warn("No new message. Try reading with --max-messages 10")

    # -- Step 8: Verify Schema Registry v2 + history -----------------
    banner("STEP 8: Verify Schema Registry Evolved to Version 2")
    versions = curl_json(f"{SR_URL}/subjects/{ORDERS_SUBJECT}/versions")
    if isinstance(versions, list):
        ok(f"Schema versions for '{ORDERS_SUBJECT}': {versions}")
        if len(versions) >= 2:
            ok("Version 2 was created -- schema evolved successfully!")
        else:
            warn("Only version 1 found. Schema may not have evolved yet.")

    v2_schema = curl_json(f"{SR_URL}/subjects/{ORDERS_SUBJECT}/versions/2")
    if "schema" in v2_schema:
        try:
            s = json.loads(v2_schema["schema"])
            fields = [f["name"] for f in s.get("fields", [])]
            ok(f"Schema v2 fields: {fields}")
            if "promo_code" in fields:
                ok("'promo_code' present in schema v2 -- BACKWARD compatible change accepted!")
        except Exception:
            pass

    show_schema_history(ORDERS_SUBJECT)

    # -- Step 9: Breaking change -- DROP COLUMN ----------------------
    banner("STEP 9: ALTER TABLE DROP COLUMN (Breaking Change)")
    warn("Dropping 'total_amount' -- this is BREAKING for BACKWARD compatibility!")
    psql("postgres-orders", "orders_db",
         "ALTER TABLE orders DROP COLUMN IF EXISTS total_amount;")
    ok("Dropped column 'total_amount'")

    # -- Step 10: Try insert after breaking change -------------------
    banner("STEP 10: Try Insert After Breaking Change")
    psql("postgres-orders", "orders_db",
         "INSERT INTO orders (customer_id, status, promo_code) "
         "VALUES (3, 'shipped', 'WINTER2024');")
    ok("SQL INSERT succeeded (but Debezium may fail to propagate)")

    info("Waiting 10s for Debezium to attempt schema registration...")
    time.sleep(10)

    # -- Step 11: Check connector status (with retries) --------------
    banner("STEP 11: Check Connector Status")
    failed = False
    final_status = {}
    for attempt in range(1, 4):
        status = curl_json(f"{KC_URL}/connectors/{CONNECTOR}/status")
        final_status = status
        tasks = status.get("tasks", [])
        failed = any(t.get("state") == "FAILED" for t in tasks)
        if failed:
            break
        info(f"Attempt {attempt}: still RUNNING, checking logs...")
        logs = run(["docker", "logs", "kafka-connect", "--tail", "30"], capture=True, check=False)
        if "409" in logs.stdout or "incompatible" in logs.stdout.lower():
            warn("Found schema compatibility errors in logs (connector may be retrying):")
            print(logs.stdout[-800:])
            break
        if attempt < 3:
            info("Waiting 5s before next check...")
            time.sleep(5)

    print(json.dumps(final_status, indent=2))

    tasks = final_status.get("tasks", [])
    failed = any(t.get("state") == "FAILED" for t in tasks)
    if failed:
        err(f"Connector '{CONNECTOR}' has FAILED task(s)!")
        for t in tasks:
            if t.get("state") == "FAILED":
                print(f"\n{C.RED}Task {t['id']} trace:{C.END}")
                print(t.get("trace", "No trace")[:500])
    else:
        ok(f"Connector '{CONNECTOR}' tasks are healthy (but check logs above for 409 errors).")

    # -- Step 12: Fix -- restore column + restart --------------------
    banner("STEP 12: Fix Breaking Change")
    info("Restoring column 'total_amount'...")
    psql("postgres-orders", "orders_db",
         "ALTER TABLE orders ADD COLUMN IF NOT EXISTS total_amount NUMERIC(10,2) DEFAULT 0;")
    ok("Column 'total_amount' restored")

    info("Restarting connector task...")
    restart = curl_json(f"{KC_URL}/connectors/{CONNECTOR}/tasks/0/restart", method="POST")
    ok("Restart command sent")

    info("Waiting 5s for connector to recover...")
    time.sleep(5)

    # -- Step 13: Verify recovery ------------------------------------
    banner("STEP 13: Verify Recovery")
    psql("postgres-orders", "orders_db",
         "INSERT INTO orders (customer_id, total_amount, status, promo_code) "
         "VALUES (4, 49.99, 'delivered', 'FALL2024');")
    ok("Inserted order after recovery")

    time.sleep(3)
    msg = read_avro_message(TOPIC, max_messages=1)
    if msg and "FALL2024" in msg:
        ok("Recovery successful! New message captured:")
        print(msg)
    else:
        warn("Message not yet visible. Check logs: docker logs -f kafka-connect")

    status = curl_json(f"{KC_URL}/connectors/{CONNECTOR}/status")
    tasks = status.get("tasks", [])
    if all(t.get("state") == "RUNNING" for t in tasks):
        ok(f"Connector '{CONNECTOR}' is RUNNING -- demo complete!")
    else:
        warn("Connector may still be recovering. Check status manually.")

    # -- Summary ------------------------------------------------------
    banner("DEMO SUMMARY")
    print(f"""
{C.BOLD}What we demonstrated:{C.END}

  1. {C.GREEN}\u2713{C.END} Debezium captures INSERT from PostgreSQL -> Kafka
  2. {C.GREEN}\u2713{C.END} Schema Registry auto-registers Avro schema v1
  3. {C.GREEN}\u2713{C.END} ALTER TABLE ADD COLUMN -> Schema Registry accepts v2 (BACKWARD compat)
  4. {C.GREEN}\u2713{C.END} New field appears in Kafka messages automatically
  5. {C.RED}\u2717{C.END} ALTER TABLE DROP COLUMN -> Schema Registry rejects (409 Conflict)
  6. {C.RED}\u2717{C.END} Connector task FAILS -- strict schema-on-write protects consumers
  7. {C.GREEN}\u2713{C.END} Restore column + restart -> pipeline recovers

{C.BOLD}Key takeaway:{C.END} Schema-on-Write with BACKWARD compatibility prevents
accidental breaking changes from reaching downstream consumers.
""")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print(f"\n{C.YELLOW}Demo interrupted by user.{C.END}")
        sys.exit(1)
    except Exception as e:
        err(f"Demo failed: {e}")
        sys.exit(1)
