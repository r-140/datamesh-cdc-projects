#!/usr/bin/env python3
"""
Check schema compatibility before deployment.
Runs in CI to ensure no breaking changes are merged.
"""

import argparse
import json
import sys
from pathlib import Path
from confluent_kafka.schema_registry import SchemaRegistryClient, Schema


def check_compatibility(registry_url: str, schemas_dir: str) -> bool:
    client = SchemaRegistryClient({"url": registry_url})
    schemas_path = Path(schemas_dir)

    all_passed = True
    for schema_file in schemas_path.glob("*.avsc"):
        subject = schema_file.stem
        schema_str = schema_file.read_text()

        try:
            # Check if subject exists
            versions = client.get_versions(subject)
            if versions:
                # Test compatibility against latest
                latest = client.get_latest_version(subject)
                is_compat = client.test_compatibility(
                    subject,
                    Schema(schema_str, schema_type="AVRO")
                )
                if is_compat:
                    print(f"PASS: {subject} is compatible")
                else:
                    print(f"FAIL: {subject} is NOT compatible with existing schema!")
                    all_passed = False
            else:
                print(f"INFO: {subject} is new (no existing schema)")
        except Exception as e:
            print(f"ERROR checking {subject}: {e}")
            all_passed = False

    return all_passed


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--registry", required=True)
    parser.add_argument("--schemas", required=True)
    args = parser.parse_args()

    passed = check_compatibility(args.registry, args.schemas)
    sys.exit(0 if passed else 1)
