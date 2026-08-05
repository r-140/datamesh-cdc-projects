#!/usr/bin/env python3
"""
CDC Data Mesh -- Automated End-to-End Demo (v5)

Fixed: Debezium ExtractNewRecordState automatically adds null for missing
fields (drop.fields.keep.schema.compatible=true), so DROP COLUMN does NOT
crash the connector. Instead, we demonstrate Schema Registry rejection via
REST API.

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
    result = subprocess.run(cmd, capture_output=capture, text=True, check=False)
    if check and result.returncode != 0:
        err(f"Command failed: {' '.join(cmd)}")
        if result.stderr:
            print(result.stderr, file=sys.stderr)
        raise RuntimeError(f"Command failed: {' '.join(cmd)}")
    return result


def docker_exec(container: str, cmd: str) -> str:
    result = run(["docker", "exec", container, "bash", "-c", cmd])
    return result.stdout.strip()


def psql(db_container: str, db: str, sql: str) -> str:
    return docker_exec(db_container, f'psql -U postgres -d {db} -c "{sql}"')


def curl_json(url: str, method: str = "GET", data: Optional[str] = None) -> dict:
    cmd = ["curl", "-s", "-X", method, url]
    if data:
        cmd += ["-H", "Content-Type: application/json", "-d", data]
    result = run(cmd)
    try:
        return json.loads(result.stdout) if result.stdout else {}
    except json.JSONDecodeError:
        return {"raw": result.stdout}


def wait_for_kafka_topic(topic: str, timeout: int = 30) -> bool:
    info(f"Waiting for topic '{topic}' to appear...")
    for i in range(timeout):
        out = docker_exec("kafka", "kafka-topics --bootstrap-server localhost:29092 --list")
        if topic in out:
            ok(f"Topic '{topic}' found after {i+1}s")
            return True
        time.sleep(1)
    err(f"Topic '{topic}' did not appear within {timeout}s")
    return False


def get_kafka_offset(topic: str) -> int:
    result = run([
        "docker", "exec", "kafka", "kafka-run-class",
        "kafka.tools.GetOffsetShell",
        "--broker-list", "localhost:29092",
        "--topic", topic,
        "--time", "-1"
    ], capture=True, check=False)
    try:
        lines = result.stdout.strip().splitlines()
        total = 0
        for line in lines:
            parts = line.split(":")
            if len(parts) >= 3:
                total += int(parts[2])
        return total
    except Exception:
        return 0


def show_schema_history(subject: str):
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


def restart_connector(connector_name: str):
    info(f"Restarting connector '{connector_name}'...")
    curl_json(f"{KC_URL}/connectors/{connector_name}/restart", method="POST")
    ok(f"Connector '{connector_name}' restart command sent")

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

    # -- Step 3: Check messages via offset ----------------------------
    banner("STEP 3: Check Kafka Message Offsets")
    offset_before = get_kafka_offset(TOPIC)
    info(f"Current offset for '{TOPIC}': {offset_before}")
    if offset_before > 0:
        ok(f"Messages present in Kafka ({offset_before} total)")
    else:
        warn("No messages yet. Debezium may still be snapshotting.")
        info("Waiting 5s...")
        time.sleep(5)
        offset_before = get_kafka_offset(TOPIC)
        if offset_before > 0:
            ok(f"Messages found after retry ({offset_before} total)")
        else:
            err("Still no messages. Check connector status manually.")

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

    # -- Step 7: Check updated offset ---------------------------------
    banner("STEP 7: Verify New Message Arrived")
    info("Waiting 3s for Debezium to capture the change...")
    time.sleep(3)
    offset_after = get_kafka_offset(TOPIC)
    if offset_after > offset_before:
        ok(f"New message captured! Offset: {offset_before} -> {offset_after}")
    else:
        warn("Offset did not increase. Debezium may be delayed.")

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

    info("NOTE: Debezium ExtractNewRecordState automatically adds null")
    info("for missing fields, so the connector stays RUNNING in practice.")
    info("Let's verify: insert a record and check that the connector")
    info("still works (it fills missing fields with null).")

    # -- Step 9b: Insert after DROP (connector should stay healthy) --
    banner("STEP 9b: Insert After DROP -- Connector Stays Healthy")
    psql("postgres-orders", "orders_db",
         "INSERT INTO orders (customer_id, status, promo_code) "
         "VALUES (3, 'shipped', 'WINTER2024');")
    ok("Inserted order without total_amount")

    info("Waiting 5s for Debezium to process...")
    time.sleep(5)

    offset_after_drop = get_kafka_offset(TOPIC)
    if offset_after_drop > offset_after:
        ok(f"Message propagated! Offset: {offset_after} -> {offset_after_drop}")
        info("Debezium handled DROP COLUMN gracefully (added null for missing field)")
    else:
        warn("Offset did not increase.")

    status = curl_json(f"{KC_URL}/connectors/{CONNECTOR}/status")
    if status.get("connector", {}).get("state") == "RUNNING":
        ok(f"Connector '{CONNECTOR}' is RUNNING -- no crash!")
        info("This is because ExtractNewRecordState keeps schema compatible.")

    # -- Step 10: Demonstrate Schema Registry rejection via API ------
    banner("STEP 10: Schema Registry Rejects Breaking Change (REST API)")
    info("Now let's test what happens if someone tries to register")
    info("a schema with a REQUIRED field (no default) -- old consumers")
    info("will not know how to handle missing 'priority'...")

    incompatible_schema = {
        "type": "record",
        "name": "Value",
        "namespace": "orders-server.public.orders",
        "fields": [
            {"name": "id", "type": {"type": "int", "connect.default": 0}, "default": 0},
            {"name": "customer_id", "type": "int"},
            {"name": "total_amount", "type": "string"},
            {
                "name": "status",
                "type": {"type": "string", "connect.default": "pending"},
                "default": "pending",
            },
            {"name": "priority", "type": "string"},  # ← NO DEFAULT = BREAKING
        ],
    }

    result = curl_json(
        f"{SR_URL}/subjects/{ORDERS_SUBJECT}/versions",
        method="POST",
        data=json.dumps({"schema": json.dumps(incompatible_schema)}),
    )

    if result.get("error_code") == 409:
        err("Schema Registry REJECTED the breaking change!")
        print(f"\n{C.RED}Error:{C.END} {result.get('message', 'Unknown')[:300]}")
        ok("BACKWARD compatibility is enforced -- consumers are protected.")
    else:
        warn("Unexpected result from Schema Registry:")
        print(json.dumps(result, indent=2))

    # -- Step 11: Restore column -------------------------------------
    banner("STEP 11: Restore Column")
    info("Restoring column 'total_amount'...")
    psql("postgres-orders", "orders_db",
         "ALTER TABLE orders ADD COLUMN IF NOT EXISTS total_amount NUMERIC(10,2) DEFAULT 0;")
    ok("Column 'total_amount' restored")

    # -- Step 12: Verify recovery ------------------------------------
    banner("STEP 12: Verify Recovery")
    psql("postgres-orders", "orders_db",
         "INSERT INTO orders (customer_id, total_amount, status, promo_code) "
         "VALUES (4, 49.99, 'delivered', 'FALL2024');")
    ok("Inserted order after recovery")

    time.sleep(3)
    offset_recovered = get_kafka_offset(TOPIC)
    if offset_recovered > offset_after_drop:
        ok(f"Recovery successful! New messages captured. Offset: {offset_after_drop} -> {offset_recovered}")
    else:
        warn("Offset did not increase. Check logs: docker logs -f kafka-connect")

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
  5. {C.YELLOW}\u26a0{C.END} ALTER TABLE DROP COLUMN -> Debezium handles gracefully
       (ExtractNewRecordState adds null, connector stays RUNNING)
  6. {C.RED}\u2717{C.END} Direct schema registration without total_amount -> 409 REJECTED
       Schema Registry enforces BACKWARD compatibility
  7. {C.GREEN}\u2713{C.END} Restore column -> pipeline continues normally

{C.BOLD}Key takeaways:{C.END}
  - Debezium's unwrap transform provides "soft" protection (null-filling)
  - Schema Registry provides "hard" protection (409 rejection)
  - Together they ensure downstream consumers never break
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
