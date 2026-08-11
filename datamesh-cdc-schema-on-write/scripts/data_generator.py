#!/usr/bin/env python3
"""
Data Generator for Data Mesh CDC Platform
==========================================

Generates INSERT / UPDATE / DELETE operations on source PostgreSQL databases
and monitors CDC propagation through Kafka to DWH in real-time.

Usage:
    python scripts/data_generator.py --mode batch --count 10
    python scripts/data_generator.py --mode continuous --interval 2
    python scripts/data_generator.py --mode delete --table orders
    python scripts/data_generator.py --mode update --table customers --count 5

Modes:
    batch       - Insert N random records, then show CDC propagation
    continuous  - Stream inserts every N seconds (Ctrl+C to stop)
    update      - Update N existing records
    delete      - Delete N random records
    mixed       - Random mix of insert/update/delete
    verify      - Show current state: source vs DWH vs Kafka offsets
"""

import argparse
import random
import subprocess
import sys
import time
from datetime import datetime
from typing import Optional

import psycopg2
from psycopg2.extras import RealDictCursor

# Configuration

KAFKA_CONNECT_URL = "http://localhost:8083"
KAFKA_BROKER = "localhost:29092"

DB_CONFIG = {
    "orders": {
        "host": "localhost", "port": 5432, "dbname": "orders_db",
        "user": "postgres", "password": "postgres",
    },
    "customers": {
        "host": "localhost", "port": 5433, "dbname": "customers_db",
        "user": "postgres", "password": "postgres",
    },
    "dwh": {
        "host": "localhost", "port": 5434, "dbname": "datamesh_dwh",
        "user": "dwh", "password": "dwh",
    },
}

COUNTRIES = ["US", "UK", "DE", "FR", "ES", "IT", "CA", "AU", "JP", "BR"]
ORDER_STATUSES = ["pending", "completed", "shipped", "cancelled", "refunded"]
FIRST_NAMES = ["Alice", "Bob", "Charlie", "Diana", "Eve", "Frank", "Grace", "Henry",
               "Ivy", "Jack", "Kate", "Leo", "Mia", "Noah", "Olivia", "Paul",
               "Quinn", "Rachel", "Sam", "Tina", "Uma", "Victor", "Wendy", "Xander",
               "Yara", "Zack", "Emma", "Liam", "Sophia", "James"]
LAST_NAMES = ["Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller",
              "Davis", "Rodriguez", "Martinez", "Hernandez", "Lopez", "Gonzalez",
              "Wilson", "Anderson", "Thomas", "Taylor", "Moore", "Jackson", "Martin"]


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
    print(Colors.OKCYAN + Colors.BOLD)
    print("   ____  ____   ____    ____  __  __ ____   ___  _  __")
    print("  / ___||  _ \\ |  _ \\  |  _ \\|  \\/  |  _ \\ / _ \\| |/ /")
    print("  \\___ \\| | | || | | | | | | | |\\/| | |_) | | | | ' / ")
    print("   ___) | |_| || |_| | | |_| | |  | |  __/| |_| | . \\ ")
    print("  |____/|____/ |____/  |____/|_|  |_|_|    \\___/|_|\\_\\")
    print(Colors.END)
    print("  " + Colors.OKGREEN + "CDC Data Generator - Watch changes flow in real-time" + Colors.END)
    print()


def print_section(title: str):
    print(f"\n{Colors.BOLD}{'='*70}{Colors.END}")
    print(f"{Colors.OKBLUE}{title:^70}{Colors.END}")
    print(f"{Colors.BOLD}{'='*70}{Colors.END}")


def print_success(msg: str):
    print(f"{Colors.OKGREEN}✓{Colors.END} {msg}")


def print_warning(msg: str):
    print(f"{Colors.WARNING}⚠{Colors.END} {msg}")


def print_error(msg: str):
    print(f"{Colors.FAIL}✗{Colors.END} {msg}")


def get_connection(db_name: str):
    cfg = DB_CONFIG[db_name]
    return psycopg2.connect(
        host=cfg["host"], port=cfg["port"], dbname=cfg["dbname"],
        user=cfg["user"], password=cfg["password"],
    )


def execute(db_name: str, query: str, params=None, fetch=False):
    conn = get_connection(db_name)
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(query, params)
            result = cur.fetchall() if fetch else cur.rowcount
            conn.commit()
            return result
    finally:
        conn.close()


def random_email(first: str, last: str) -> str:
    domain = random.choice(["gmail.com", "yahoo.com", "outlook.com", "example.com", "corp.io"])
    return f"{first.lower()}.{last.lower()}{random.randint(1,999)}@{domain}"


def generate_customer():
    first = random.choice(FIRST_NAMES)
    last = random.choice(LAST_NAMES)
    return {
        "email": random_email(first, last),
        "full_name": f"{first} {last}",
        "country": random.choice(COUNTRIES),
    }


def generate_order(max_customer_id: int = 10):
    return {
        "customer_id": random.randint(1, max(1, max_customer_id)),
        "total_amount": round(random.uniform(10.0, 2000.0), 2),
        "status": random.choice(ORDER_STATUSES),
    }


def insert_customers(count: int = 1) -> list:
    print_section(f"INSERTING {count} CUSTOMER(S)")
    inserted = []
    for _ in range(count):
        cust = generate_customer()
        result = execute(
            "customers",
            "INSERT INTO customers (email, full_name, country) VALUES (%(email)s, %(full_name)s, %(country)s) RETURNING id, email, full_name, country, created_at",
            cust, fetch=True,
        )
        if result:
            inserted.append(dict(result[0]))
            print_success(f"Customer #{result[0]['id']}: {result[0]['full_name']} ({result[0]['email']})")
    return inserted


def insert_orders(count: int = 1, max_customer_id: int = 10) -> list:
    print_section(f"INSERTING {count} ORDER(S)")
    inserted = []
    for _ in range(count):
        order = generate_order(max_customer_id)
        result = execute(
            "orders",
            "INSERT INTO orders (customer_id, total_amount, status) VALUES (%(customer_id)s, %(total_amount)s, %(status)s) RETURNING id, customer_id, total_amount, status, created_at",
            order, fetch=True,
        )
        if result:
            inserted.append(dict(result[0]))
            print_success(f"Order #{result[0]['id']}: ${result[0]['total_amount']} - {result[0]['status']} (customer={result[0]['customer_id']})")
    return inserted


def update_customers(count: int = 1) -> list:
    print_section(f"UPDATING {count} CUSTOMER(S)")
    updated = []
    rows = execute("customers", "SELECT id FROM customers ORDER BY RANDOM() LIMIT %s", (count,), fetch=True)
    if not rows:
        print_warning("No customers to update")
        return updated
    for row in rows:
        new_country = random.choice([c for c in COUNTRIES if c != "US"])
        result = execute(
            "customers",
            "UPDATE customers SET country = %s WHERE id = %s RETURNING id, email, full_name, country",
            (new_country, row["id"]), fetch=True,
        )
        if result:
            updated.append(dict(result[0]))
            print_success(f"Customer #{result[0]['id']}: country -> {result[0]['country']}")
    return updated


def update_orders(count: int = 1) -> list:
    print_section(f"UPDATING {count} ORDER(S)")
    updated = []
    rows = execute("orders", "SELECT id FROM orders ORDER BY RANDOM() LIMIT %s", (count,), fetch=True)
    if not rows:
        print_warning("No orders to update")
        return updated
    for row in rows:
        new_status = random.choice([s for s in ORDER_STATUSES if s != "pending"])
        result = execute(
            "orders",
            "UPDATE orders SET status = %s WHERE id = %s RETURNING id, customer_id, total_amount, status, created_at",
            (new_status, row["id"]), fetch=True,
        )
        if result:
            updated.append(dict(result[0]))
            print_success(f"Order #{result[0]['id']}: status -> {result[0]['status']}")
    return updated


def delete_customers(count: int = 1) -> list:
    print_section(f"DELETING {count} CUSTOMER(S)")
    deleted = []
    rows = execute("customers", "SELECT id, email, full_name FROM customers ORDER BY RANDOM() LIMIT %s", (count,), fetch=True)
    if not rows:
        print_warning("No customers to delete")
        return deleted
    for row in rows:
        execute("customers", "DELETE FROM customers WHERE id = %s", (row["id"],))
        deleted.append(dict(row))
        print_success(f"Deleted customer #{row['id']}: {row['full_name']} ({row['email']})")
    return deleted


def delete_orders(count: int = 1) -> list:
    print_section(f"DELETING {count} ORDER(S)")
    deleted = []
    rows = execute("orders", "SELECT id, total_amount, status FROM orders ORDER BY RANDOM() LIMIT %s", (count,), fetch=True)
    if not rows:
        print_warning("No orders to delete")
        return deleted
    for row in rows:
        execute("orders", "DELETE FROM orders WHERE id = %s", (row["id"],))
        deleted.append(dict(row))
        print_success(f"Deleted order #{row['id']}: ${row['total_amount']} - {row['status']}")
    return deleted


def get_kafka_offsets():
    topics = ["orders-server.public.orders", "customers-server.public.customers"]
    offsets = {}
    for topic in topics:
        try:
            out = subprocess.run(
                ["docker", "exec", "kafka", "kafka-run-class", "kafka.tools.GetOffsetShell",
                 "--broker-list", KAFKA_BROKER, "--topic", topic, "--time", "-1"],
                capture_output=True, text=True, timeout=10,
            )
            if out.returncode == 0 and out.stdout.strip():
                parts = out.stdout.strip().split(":")
                if len(parts) >= 3:
                    offsets[topic] = int(parts[-1])
        except Exception as e:
            offsets[topic] = f"error: {e}"
    return offsets


def get_connector_status():
    connectors = {}
    names = ["orders-cdc-connector", "customers-cdc-connector", "orders-jdbc-sink", "customers-jdbc-sink"]
    for name in names:
        try:
            import urllib.request, json
            req = urllib.request.Request(f"{KAFKA_CONNECT_URL}/connectors/{name}/status")
            with urllib.request.urlopen(req, timeout=5) as resp:
                data = json.loads(resp.read().decode())
                connectors[name] = data.get("connector", {}).get("state", "UNKNOWN")
        except Exception as e:
            connectors[name] = f"ERROR: {e}"
    return connectors


def show_source_counts():
    print(f"\n{Colors.BOLD}Source Databases:{Colors.END}")
    for db, table in [("orders", "orders"), ("customers", "customers")]:
        try:
            count = execute(db, f"SELECT COUNT(*) as cnt FROM {table}", fetch=True)
            print(f"  {db:12s} {table:12s} -> {count[0]['cnt']} rows")
        except Exception as e:
            print(f"  {db:12s} {table:12s} -> {Colors.FAIL}error: {e}{Colors.END}")


def show_dwh_counts():
    print(f"\n{Colors.BOLD}DWH (raw layer):{Colors.END}")
    for table in ["raw.orders_cdc", "raw.customers_cdc"]:
        try:
            count = execute("dwh", f"SELECT COUNT(*) as cnt FROM {table}", fetch=True)
            print(f"  {table:25s} -> {count[0]['cnt']} rows")
        except Exception as e:
            print(f"  {table:25s} -> {Colors.FAIL}error: {e}{Colors.END}")


def show_kafka_state():
    print(f"\n{Colors.BOLD}Kafka Offsets (latest):{Colors.END}")
    offsets = get_kafka_offsets()
    for topic, offset in offsets.items():
        print(f"  {topic:40s} -> offset {offset}")


def show_connector_state():
    print(f"\n{Colors.BOLD}Connector Status:{Colors.END}")
    statuses = get_connector_status()
    for name, state in statuses.items():
        color = Colors.OKGREEN if state == "RUNNING" else Colors.FAIL if "ERROR" in str(state) else Colors.WARNING
        print(f"  {name:30s} -> {color}{state}{Colors.END}")


def verify_all():
    print_section("VERIFICATION: Source -> Kafka -> DWH")
    show_source_counts()
    show_kafka_state()
    show_connector_state()
    show_dwh_counts()


def wait_for_cdc_sync(max_wait: int = 15):
    print(f"\n{Colors.OKCYAN}Waiting up to {max_wait}s for CDC sync...{Colors.END}")
    for i in range(max_wait):
        time.sleep(1)
        print(f"  ... {i+1}s", end="\r")
    print()
    show_dwh_counts()


def run_mixed(count: int = 10):
    print_section(f"MIXED MODE - {count} random operations")
    ops = ["insert_order", "insert_customer", "update_order", "update_customer", "delete_order", "delete_customer"]
    for i in range(count):
        op = random.choice(ops)
        print(f"\n{Colors.BOLD}[{i+1}/{count}] {op.upper()}{Colors.END}")
        if op == "insert_order": insert_orders(1)
        elif op == "insert_customer": insert_customers(1)
        elif op == "update_order": update_orders(1)
        elif op == "update_customer": update_customers(1)
        elif op == "delete_order": delete_orders(1)
        elif op == "delete_customer": delete_customers(1)
        time.sleep(0.5)


def run_continuous(interval: float = 2.0, table: Optional[str] = None):
    print_section(f"CONTINUOUS MODE - generating every {interval}s (Ctrl+C to stop)")
    print(f"{Colors.WARNING}Press Ctrl+C to stop{Colors.END}\n")
    try:
        while True:
            timestamp = datetime.now().strftime("%H:%M:%S")
            print(f"{Colors.OKCYAN}[{timestamp}]{Colors.END}", end=" ")
            if table == "orders" or table is None:
                insert_orders(random.randint(1, 3))
            if table == "customers" or table is None:
                insert_customers(random.randint(1, 2))
            if random.random() < 0.3:
                update_orders(random.randint(1, 2))
            if random.random() < 0.2:
                update_customers(1)
            time.sleep(interval)
    except KeyboardInterrupt:
        print(f"\n\n{Colors.OKGREEN}Stopped by user{Colors.END}")
        verify_all()


def main():
    parser = argparse.ArgumentParser(
        description="Data Generator for Data Mesh CDC Platform",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s --mode batch --count 20
  %(prog)s --mode continuous --interval 3
  %(prog)s --mode update --table orders --count 5
  %(prog)s --mode delete --table customers --count 3
  %(prog)s --mode mixed --count 15
  %(prog)s --mode verify
        """,
    )
    parser.add_argument("--mode", "-m", required=True,
                        choices=["batch", "continuous", "insert", "update", "delete", "mixed", "verify"],
                        help="Operation mode")
    parser.add_argument("--table", "-t", choices=["orders", "customers", "both"],
                        default="both", help="Target table (default: both)")
    parser.add_argument("--count", "-c", type=int, default=10,
                        help="Number of operations (default: 10)")
    parser.add_argument("--interval", "-i", type=float, default=2.0,
                        help="Interval in seconds for continuous mode (default: 2.0)")
    parser.add_argument("--wait-cdc", "-w", action="store_true",
                        help="Wait and show DWH sync after operations")
    parser.add_argument("--no-banner", action="store_true",
                        help="Skip banner")

    args = parser.parse_args()

    if not args.no_banner:
        print_banner()

    if args.mode == "verify":
        verify_all()
        return

    if args.mode == "batch":
        if args.table in ("orders", "both"): insert_orders(args.count)
        if args.table in ("customers", "both"): insert_customers(args.count)
    elif args.mode == "insert":
        if args.table in ("orders", "both"): insert_orders(args.count)
        if args.table in ("customers", "both"): insert_customers(args.count)
    elif args.mode == "update":
        if args.table in ("orders", "both"): update_orders(args.count)
        if args.table in ("customers", "both"): update_customers(args.count)
    elif args.mode == "delete":
        if args.table in ("orders", "both"): delete_orders(args.count)
        if args.table in ("customers", "both"): delete_customers(args.count)
    elif args.mode == "mixed":
        run_mixed(args.count)
    elif args.mode == "continuous":
        table = None if args.table == "both" else args.table
        run_continuous(args.interval, table)
        return

    if args.wait_cdc:
        wait_for_cdc_sync()

    verify_all()


if __name__ == "__main__":
    main()
