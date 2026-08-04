#!/usr/bin/env python3
"""
Advanced Schema Evolution Simulator

Runs multiple scenarios (A-H) against the local Schema Registry and prints
a formatted report showing compatibility checks, pipeline actions, and schema IDs.

Usage:
    python scripts/schema_evolution_simulator.py
"""

import json
import sys
import os
import requests
from typing import Dict, List, Any, Optional
from pathlib import Path

# Add src to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

from datamesh_cdc.schema_evolution import SchemaEvolutionManager, SchemaEvolutionError
from datamesh_cdc.pipeline_manager import PipelineManager


SR_URL = os.environ.get("SCHEMA_REGISTRY_URL", "http://localhost:8081")
STATE_FILE = "/tmp/datamesh_advanced_sim.json"
SIM_SUBJECT = "sim.advanced.orders-value"


def print_banner(text: str):
    print("\n" + "=" * 70)
    print(text.center(70))
    print("=" * 70)


def print_table(headers: List[str], rows: List[List[str]]):
    """Simple ASCII table printer."""
    col_widths = [len(h) for h in headers]
    for row in rows:
        for i, cell in enumerate(row):
            col_widths[i] = max(col_widths[i], len(str(cell)))

    sep = "+-" + "-+-".join("-" * w for w in col_widths) + "-+"
    header_row = "| " + " | ".join(h.ljust(w) for h, w in zip(headers, col_widths)) + " |"
    data_rows = [
        "| " + " | ".join(str(cell).ljust(w) for cell, w in zip(row, col_widths)) + " |"
        for row in rows
    ]

    print(sep)
    print(header_row)
    print(sep)
    for r in data_rows:
        print(r)
    print(sep)


def cleanup():
    """Remove simulation state and old SR subjects."""
    # Remove state file
    for f in [STATE_FILE, "/tmp/datamesh_simulate.json", "state.json"]:
        Path(f).unlink(missing_ok=True)

    # Soft + hard delete of simulation subjects
    for subject in [SIM_SUBJECT, "sim.orders-value", "sim.customers-value"]:
        try:
            requests.delete(f"{SR_URL}/subjects/{subject}")
        except Exception:
            pass
        try:
            requests.delete(f"{SR_URL}/subjects/{subject}?permanent=true")
        except Exception:
            pass
    print("[cleanup] State and old subjects removed.")


def run_scenario(
    name: str,
    manager: PipelineManager,
    schema_v1: Dict[str, Any],
    schema_v2: Dict[str, Any],
    pipelines: List[Dict[str, Any]]
) -> Dict[str, Any]:
    """
    Run a single scenario:
      1. Register v1
      2. Create pipelines
      3. Apply v2
      4. Collect results
    """
    results = {
        "scenario": name,
        "v1_registered": False,
        "v1_schema_id": None,
        "v2_registered": False,
        "v2_schema_id": None,
        "pipelines": []
    }

    # Register v1
    try:
        sid = manager.schema_manager.register_schema(SIM_SUBJECT, schema_v1)
        results["v1_registered"] = True
        results["v1_schema_id"] = sid
    except Exception as e:
        results["v1_error"] = str(e)
        return results

    # Create pipelines
    pipeline_ids = []
    for p in pipelines:
        pid = p["id"]
        try:
            manager.create_pipeline(
                pipeline_id=pid,
                source_topic="sim.advanced.orders",
                sink_table=f"raw.{pid}",
                domain="orders",
                opt_in_schema_evolution=(p["mode"] == "opt-in"),
                consumed_fields=p.get("consumed_fields", [])
            )
        except ValueError:
            pass  # already exists
        pipeline_ids.append(pid)

    # Apply v2 to each pipeline
    for pid in pipeline_ids:
        try:
            res = manager.handle_schema_change(pid, schema_v2)
            results["pipelines"].append({
                "pipeline_id": pid,
                "mode": next(p["mode"] for p in pipelines if p["id"] == pid),
                "action": res.get("action", "UNKNOWN"),
                "schema_id": res.get("schema_id"),
                "reason": res.get("reason", "")
            })
        except Exception as e:
            results["pipelines"].append({
                "pipeline_id": pid,
                "mode": next(p["mode"] for p in pipelines if p["id"] == pid),
                "action": "ERROR",
                "reason": str(e)
            })

    # Check if v2 was actually registered
    try:
        latest = manager.schema_manager.get_latest_schema(SIM_SUBJECT)
        if latest:
            results["v2_registered"] = True
    except Exception:
        pass

    return results


def main():
    print_banner("ADVANCED SCHEMA EVOLUTION SIMULATOR")
    cleanup()

    manager = PipelineManager(schema_registry_url=SR_URL, state_file=STATE_FILE)

    # Common base schema
    base_fields = [
        {"name": "id", "type": "long"},
        {"name": "customer_id", "type": "long"},
        {"name": "total_amount", "type": "double"},
        {"name": "status", "type": "string"}
    ]

    schema_v1 = {
        "type": "record",
        "name": "Order",
        "namespace": "sim.advanced.orders",
        "fields": base_fields.copy()
    }

    all_results: List[Dict[str, Any]] = []

    # ─────────────────────────────────────────────────────────────────
    # SCENARIO A: Field Type Change (Breaking)
    # ─────────────────────────────────────────────────────────────────
    print_banner("SCENARIO A: Field Type Change (double → string)")
    schema_v2_a = {
        "type": "record", "name": "Order", "namespace": "sim.advanced.orders",
        "fields": [
            {"name": "id", "type": "long"},
            {"name": "customer_id", "type": "long"},
            {"name": "total_amount", "type": "string"},  # BREAKING
            {"name": "status", "type": "string"}
        ]
    }
    res = run_scenario("A", manager, schema_v1, schema_v2_a, [
        {"id": "orders-to-analytics", "mode": "opt-in"},
        {"id": "orders-to-reporting", "mode": "opt-out", "consumed_fields": ["total_amount"]}
    ])
    all_results.append(res)
    print(f"v1 schema ID: {res['v1_schema_id']}")
    print_table(
        ["Pipeline", "Mode", "Action", "Reason"],
        [[p["pipeline_id"], p["mode"], p["action"], p.get("reason", "")[:50]] for p in res["pipelines"]]
    )

    # ─────────────────────────────────────────────────────────────────
    # SCENARIO B: Field Rename (Breaking)
    # ─────────────────────────────────────────────────────────────────
    print_banner("SCENARIO B: Field Rename (total_amount → new_amount)")
    schema_v2_b = {
        "type": "record", "name": "Order", "namespace": "sim.advanced.orders",
        "fields": [
            {"name": "id", "type": "long"},
            {"name": "customer_id", "type": "long"},
            {"name": "new_amount", "type": "double"},  # RENAMED
            {"name": "status", "type": "string"}
        ]
    }
    res = run_scenario("B", manager, schema_v1, schema_v2_b, [
        {"id": "orders-to-analytics", "mode": "opt-in"},
        {"id": "orders-to-reporting", "mode": "opt-out", "consumed_fields": ["total_amount"]}
    ])
    all_results.append(res)
    print_table(
        ["Pipeline", "Mode", "Action", "Reason"],
        [[p["pipeline_id"], p["mode"], p["action"], p.get("reason", "")[:50]] for p in res["pipelines"]]
    )

    # ─────────────────────────────────────────────────────────────────
    # SCENARIO C: Add Required Field (Breaking)
    # ─────────────────────────────────────────────────────────────────
    print_banner("SCENARIO C: Add Required Field (no default)")
    schema_v2_c = {
        "type": "record", "name": "Order", "namespace": "sim.advanced.orders",
        "fields": [
            {"name": "id", "type": "long"},
            {"name": "customer_id", "type": "long"},
            {"name": "total_amount", "type": "double"},
            {"name": "status", "type": "string"},
            {"name": "priority", "type": "string"}  # NO DEFAULT
        ]
    }
    res = run_scenario("C", manager, schema_v1, schema_v2_c, [
        {"id": "orders-to-analytics", "mode": "opt-in"},
        {"id": "orders-to-reporting", "mode": "opt-out", "consumed_fields": ["status"]}
    ])
    all_results.append(res)
    print_table(
        ["Pipeline", "Mode", "Action", "Reason"],
        [[p["pipeline_id"], p["mode"], p["action"], p.get("reason", "")[:50]] for p in res["pipelines"]]
    )

    # ─────────────────────────────────────────────────────────────────
    # SCENARIO D: Add Optional Field (Compatible)
    # ─────────────────────────────────────────────────────────────────
    print_banner("SCENARIO D: Add Optional Field (promo_code)")
    schema_v2_d = {
        "type": "record", "name": "Order", "namespace": "sim.advanced.orders",
        "fields": [
            {"name": "id", "type": "long"},
            {"name": "customer_id", "type": "long"},
            {"name": "total_amount", "type": "double"},
            {"name": "status", "type": "string"},
            {"name": "promo_code", "type": ["null", "string"], "default": None}
        ]
    }
    res = run_scenario("D", manager, schema_v1, schema_v2_d, [
        {"id": "orders-to-analytics", "mode": "opt-in"},
        {"id": "orders-to-reporting", "mode": "opt-out", "consumed_fields": ["total_amount"]}
    ])
    all_results.append(res)
    print_table(
        ["Pipeline", "Mode", "Action", "Reason"],
        [[p["pipeline_id"], p["mode"], p["action"], p.get("reason", "")[:50]] for p in res["pipelines"]]
    )

    # ─────────────────────────────────────────────────────────────────
    # SCENARIO E: Nested Record
    # ─────────────────────────────────────────────────────────────────
    print_banner("SCENARIO E: Nested Record (customer object)")
    schema_v2_e = {
        "type": "record", "name": "Order", "namespace": "sim.advanced.orders",
        "fields": [
            {"name": "id", "type": "long"},
            {"name": "customer", "type": {
                "type": "record", "name": "Customer",
                "fields": [
                    {"name": "id", "type": "long"},
                    {"name": "email", "type": "string"}
                ]
            }},
            {"name": "total_amount", "type": "double"},
            {"name": "status", "type": "string"}
        ]
    }
    res = run_scenario("E", manager, schema_v1, schema_v2_e, [
        {"id": "orders-to-analytics", "mode": "opt-in"},
        {"id": "orders-to-reporting", "mode": "opt-out", "consumed_fields": ["customer_id"]}
    ])
    all_results.append(res)
    print_table(
        ["Pipeline", "Mode", "Action", "Reason"],
        [[p["pipeline_id"], p["mode"], p["action"], p.get("reason", "")[:50]] for p in res["pipelines"]]
    )

    # ─────────────────────────────────────────────────────────────────
    # SCENARIO F: Enum Type
    # ─────────────────────────────────────────────────────────────────
    print_banner("SCENARIO F: Enum Type (OrderStatus)")
    schema_v2_f = {
        "type": "record", "name": "Order", "namespace": "sim.advanced.orders",
        "fields": [
            {"name": "id", "type": "long"},
            {"name": "customer_id", "type": "long"},
            {"name": "total_amount", "type": "double"},
            {"name": "status", "type": {
                "type": "enum", "name": "OrderStatus",
                "symbols": ["PENDING", "CONFIRMED", "SHIPPED", "CANCELLED"]
            }}
        ]
    }
    res = run_scenario("F", manager, schema_v1, schema_v2_f, [
        {"id": "orders-to-analytics", "mode": "opt-in"},
        {"id": "orders-to-reporting", "mode": "opt-out", "consumed_fields": ["status"]}
    ])
    all_results.append(res)
    print_table(
        ["Pipeline", "Mode", "Action", "Reason"],
        [[p["pipeline_id"], p["mode"], p["action"], p.get("reason", "")[:50]] for p in res["pipelines"]]
    )

    # ─────────────────────────────────────────────────────────────────
    # SCENARIO G: Multiple Pipelines on One Topic
    # ─────────────────────────────────────────────────────────────────
    print_banner("SCENARIO G: Multiple Pipelines (remove status)")
    schema_v2_g = {
        "type": "record", "name": "Order", "namespace": "sim.advanced.orders",
        "fields": [
            {"name": "id", "type": "long"},
            {"name": "customer_id", "type": "long"},
            {"name": "total_amount", "type": "double"}
            # status REMOVED
        ]
    }
    res = run_scenario("G", manager, schema_v1, schema_v2_g, [
        {"id": "orders-to-ml", "mode": "opt-out", "consumed_fields": ["id", "status"]},
        {"id": "orders-to-bi", "mode": "opt-out", "consumed_fields": ["total_amount"]},
        {"id": "orders-to-archive", "mode": "opt-in", "consumed_fields": []}
    ])
    all_results.append(res)
    print_table(
        ["Pipeline", "Mode", "Action", "Reason"],
        [[p["pipeline_id"], p["mode"], p["action"], p.get("reason", "")[:50]] for p in res["pipelines"]]
    )

    # ─────────────────────────────────────────────────────────────────
    # SCENARIO H: Compatibility Level Switch
    # ─────────────────────────────────────────────────────────────────
    print_banner("SCENARIO H: Switch Compatibility to FULL")
    try:
        requests.put(
            f"{SR_URL}/config/{SIM_SUBJECT}",
            json={"compatibility": "FULL"}
        )
        print("[SR] Compatibility set to FULL")
    except Exception as e:
        print(f"[SR] Failed to set compatibility: {e}")

    schema_v2_h = {
        "type": "record", "name": "Order", "namespace": "sim.advanced.orders",
        "fields": [
            {"name": "id", "type": "long"},
            {"name": "customer_id", "type": "long"},
            {"name": "total_amount", "type": "double"},
            {"name": "status", "type": "string"},
            {"name": "priority", "type": ["null", "string"], "default": None}
        ]
    }
    res = run_scenario("H", manager, schema_v1, schema_v2_h, [
        {"id": "orders-to-analytics", "mode": "opt-in"}
    ])
    all_results.append(res)
    print_table(
        ["Pipeline", "Mode", "Action", "Reason"],
        [[p["pipeline_id"], p["mode"], p["action"], p.get("reason", "")[:50]] for p in res["pipelines"]]
    )

    # ─────────────────────────────────────────────────────────────────
    # FINAL SUMMARY
    # ─────────────────────────────────────────────────────────────────
    print_banner("SIMULATION SUMMARY")

    summary_rows = []
    for r in all_results:
        for p in r["pipelines"]:
            summary_rows.append([
                r["scenario"],
                p["pipeline_id"],
                p["mode"],
                p["action"],
                p.get("reason", "")[:40]
            ])

    print_table(
        ["Scenario", "Pipeline", "Mode", "Action", "Reason"],
        summary_rows
    )

    # Save full JSON report
    report_path = "/tmp/schema_evolution_report.json"
    with open(report_path, "w") as f:
        json.dump(all_results, f, indent=2, default=lambda o: o.value if hasattr(o, "value") else str(o))
    print(f"\n[report] Full JSON report saved to: {report_path}")

    # Domain stats
    stats = manager.get_domain_stats("orders")
    print("\n[domain-stats] orders:")
    print(json.dumps(stats, indent=2))


if __name__ == "__main__":
    main()
