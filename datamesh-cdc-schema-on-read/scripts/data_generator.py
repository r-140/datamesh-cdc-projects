#!/usr/bin/env python3
"""
CDC Data Generator — Schema-on-Read

Generates INSERT/UPDATE/DELETE operations on source databases
and verifies CDC propagation to DWH (JSONB raw layer).

Usage:
    python scripts/data_generator.py --mode batch --count 10
    python scripts/data_generator.py --mode continuous --interval 3
    python scripts/data_generator.py --mode verify
"""

import argparse
import json
import random
import subprocess
import sys
import time
import urllib.request
import urllib.error

import psycopg2

# ─── Config ──────────────────────────────────────────────────────────

ORDERS_DB = {"host": "localhost", "port": 5432, "user": "postgres", "password": "postgres", "dbname": "orders_db"}
CUSTOMERS_DB = {"host": "localhost", "port": 5433, "user": "postgres", "password": "postgres", "dbname": "customers_db"}
DWH_DB = {"host": "localhost", "port": 5434, "user": "dwh", "password": "dwh", "dbname": "datamesh_dwh"}

KAFKA_CONNECT_URL = "http://localhost:8083"

# ─── Helpers ─────────────────────────────────────────────────────────

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

def check_consumer_alive():
    try:
        result = subprocess.run(
            ["pgrep", "-f", "cdc_consumer.py"],
            capture_output=True, text=True
        )
        return result.returncode == 0
    except:
        return False

def get_kafka_offset(topic):
    try:
        result = subprocess.run(
            ["docker", "exec", "kafka", "kafka-run-class", "kafka.tools.GetOffsetShell",
             "--bootstrap-server", "localhost:29092",
             "--topic", topic],
            capture_output=True, text=True, timeout=10
        )
        if result.returncode == 0:
            line = result.stdout.strip()
            if ":" in line:
                return int(line.split(":")[-1])
    except:
        pass
    return 0

def generate_orders(count):
    statuses = ["pending", "completed", "shipped", "cancelled", "refunded"]
    for _ in range(count):
        customer_id = random.randint(1, 10)
        total_amount = round(random.uniform(10, 2000), 2)
        status = random.choice(statuses)
        pg_execute(ORDERS_DB,
            "INSERT INTO orders (customer_id, total_amount, status) VALUES (%s, %s, %s)",
            (customer_id, total_amount, status))

def generate_customers(count):
    first_names = ["Alice", "Bob", "Charlie", "Diana", "Eve", "Frank", "Grace", "Henry", "Ivy", "Jack"]
    last_names = ["Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller", "Davis", "Rodriguez", "Martinez"]
    domains = ["example.com", "gmail.com", "yahoo.com", "outlook.com", "corp.io"]
    countries = ["US", "UK", "CA", "DE", "FR", "JP", "AU", "BR"]

    for _ in range(count):
        name = f"{random.choice(first_names)} {random.choice(last_names)}"
        email = f"{name.lower().replace(' ', '.')}{random.randint(1,999)}@{random.choice(domains)}"
        country = random.choice(countries)
        pg_execute(CUSTOMERS_DB,
            "INSERT INTO customers (name, full_name, email, country) VALUES (%s, %s, %s, %s)",
            (name, name, email, country))

def verify():
    print("\n" + "=" * 70)
    print("                 VERIFICATION: Source -> Kafka -> DWH")
    print("=" * 70 + "\n")

    # Source counts
    orders_src = pg_execute(ORDERS_DB, "SELECT COUNT(*) FROM orders")[0][0]
    customers_src = pg_execute(CUSTOMERS_DB, "SELECT COUNT(*) FROM customers")[0][0]
    print(f"Source Databases:")
    print(f"  orders       orders       -> {orders_src} rows")
    print(f"  customers    customers    -> {customers_src} rows\n")

    # Kafka offsets
    orders_offset = get_kafka_offset("orders-server.public.orders")
    customers_offset = get_kafka_offset("customers-server.public.customers")
    print(f"Kafka Offsets (latest):")
    print(f"  orders-server.public.orders              -> offset {orders_offset}")
    print(f"  customers-server.public.customers        -> offset {customers_offset}\n")

    # Connector status
    print("Connector Status:")
    for name in ["orders-cdc-connector", "customers-cdc-connector"]:
        st = get_connector_status(name)
        if st:
            cstate = st.get("connector", {}).get("state", "UNKNOWN")
            print(f"  {name:30s} -> {cstate}")
        else:
            print(f"  {name:30s} -> NOT FOUND")

    # Consumer status
    consumer_alive = check_consumer_alive()
    print(f"\nCDC Consumer:")
    print(f"  cdc_consumer.py              -> {'RUNNING' if consumer_alive else 'NOT RUNNING'}")

    # DWH counts
    try:
        orders_dwh = pg_execute(DWH_DB, "SELECT COUNT(*) FROM raw.orders_cdc")[0][0]
        customers_dwh = pg_execute(DWH_DB, "SELECT COUNT(*) FROM raw.customers_cdc")[0][0]
        print(f"\nDWH (raw layer):")
        print(f"  raw.orders_cdc            -> {orders_dwh} rows")
        print(f"  raw.customers_cdc         -> {customers_dwh} rows")
    except Exception as e:
        print(f"\nDWH (raw layer):")
        print(f"  raw.orders_cdc            -> error: {e}")
        print(f"  raw.customers_cdc         -> error: {e}")

def main():
    parser = argparse.ArgumentParser(description="CDC Data Generator — Schema-on-Read")
    parser.add_argument("--mode", choices=["batch", "continuous", "verify"], default="batch")
    parser.add_argument("--count", type=int, default=10)
    parser.add_argument("--interval", type=int, default=3)
    args = parser.parse_args()

    if args.mode == "verify":
        verify()
        return

    if args.mode == "batch":
        print("\n" + "=" * 70)
        print(f"                        INSERTING {args.count} ORDER(S)")
        print("=" * 70)
        generate_orders(args.count)
        print(f"✓ Inserted {args.count} orders\n")

        print("=" * 70)
        print(f"                       INSERTING {args.count} CUSTOMER(S)")
        print("=" * 70)
        generate_customers(args.count)
        print(f"✓ Inserted {args.count} customers\n")

        time.sleep(3)
        verify()

    elif args.mode == "continuous":
        print("\n   ____  ____   ____    ____  __  __ ____   ___  _  __")
        print("  / ___||  _ \\ |  _ \\  |  _ \\|  \\/  |  _ \\ / _ \\| |/ /")
        print("  \\___ \\| | | || | | | | | | | |\\/| | |_) | | | | ' /")
        print("   ___) | |_| || |_| | | |_| | |  | |  __/| |_| | . \\ ")
        print("  |____/|____/ |____/  |____/|_|  |_|_|    \\___/|_|\\_\\")
        print("\n  CDC Data Generator — Schema-on-Read")
        print(f"  Generating {args.count} orders + {args.count} customers every {args.interval}s")
        print("  Press Ctrl+C to stop\n")

        try:
            while True:
                generate_orders(args.count)
                generate_customers(args.count)
                print(f"✓ Batch inserted ({args.count} orders + {args.count} customers)")
                time.sleep(args.interval)
        except KeyboardInterrupt:
            print("\n\nStopped.")
            verify()

if __name__ == "__main__":
    main()
