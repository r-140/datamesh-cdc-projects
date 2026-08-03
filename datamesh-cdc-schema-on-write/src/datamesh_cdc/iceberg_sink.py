"""
Iceberg Sink — STRICT TYPED (Schema-on-Write).

Tables are created with explicit Avro-derived schemas.
Schema evolution supports ADD COLUMN and safe TYPE WIDENING only.
"""

import logging
from typing import Dict, Any

from pyiceberg.catalog import load_catalog
from pyiceberg.schema import Schema
from pyiceberg.types import (
    LongType, StringType, DoubleType, FloatType, IntegerType,
    BooleanType, BinaryType, NestedField
)
from pyiceberg.partitioning import PartitionSpec, PartitionField
from pyiceberg.transforms import DayTransform

logger = logging.getLogger(__name__)


class IcebergCDCSink:
    TYPE_MAPPING = {
        'string': StringType(), 'int': IntegerType(), 'long': LongType(),
        'float': FloatType(), 'double': DoubleType(),
        'boolean': BooleanType(), 'bytes': BinaryType(),
    }

    def __init__(self, catalog_uri: str = "http://localhost:8181",
                 warehouse: str = "s3://datamesh-warehouse/",
                 s3_endpoint: str = "http://localhost:9000",
                 s3_access_key: str = "minio", s3_secret_key: str = "minio123"):
        self.catalog = load_catalog("rest", **{
            "uri": catalog_uri, "warehouse": warehouse,
            "s3.endpoint": s3_endpoint, "s3.access-key-id": s3_access_key,
            "s3.secret-access-key": s3_secret_key, "s3.path-style-access": "true",
        })
        self.catalog_uri = catalog_uri
        logger.info(f"IcebergCDCSink initialized: {catalog_uri}")

    def create_cdc_table(self, table_name: str, avro_schema: dict, namespace: str = "raw") -> str:
        identifier = f"{namespace}.{table_name}"
        try:
            existing = self.catalog.load_table(identifier)
            logger.info(f"Table already exists: {identifier}")
            return identifier
        except Exception:
            pass

        schema = self._avro_to_iceberg_schema(avro_schema)
        cdc_fields = [
            NestedField(10001, "_cdc_op", StringType(), required=True),
            NestedField(10002, "_cdc_ts_ms", LongType(), required=True),
            NestedField(10003, "_cdc_source_ts_ms", LongType(), required=False),
        ]
        final_schema = Schema(*list(schema.fields) + cdc_fields)
        partition = PartitionSpec(PartitionField(source_id=10002, transform=DayTransform(), name="cdc_day", field_id=1001))

        table = self.catalog.create_table(identifier=identifier, schema=final_schema, partition_spec=partition)
        logger.info(f"Created CDC table: {identifier}")
        return identifier

    def apply_schema_evolution(self, table_name: str, new_avro_schema: dict, namespace: str = "raw") -> Dict[str, Any]:
        identifier = f"{namespace}.{table_name}"
        table = self.catalog.load_table(identifier)
        current_fields = {f.name: f for f in table.schema().fields}
        new_iceberg_schema = self._avro_to_iceberg_schema(new_avro_schema)
        new_fields = {f.name: f for f in new_iceberg_schema.fields}

        changes = {"added": [], "widened": [], "errors": []}

        for name, field in new_fields.items():
            if name not in current_fields:
                try:
                    with table.update_schema() as update:
                        update.add_column(name, field.field_type)
                    changes["added"].append(name)
                    logger.info(f"Added column: {name} ({field.field_type})")
                except Exception as e:
                    changes["errors"].append(f"Failed to add {name}: {e}")

        for name in current_fields:
            if name in new_fields:
                old_type = current_fields[name].field_type
                new_type = new_fields[name].field_type
                if self._is_safe_widening(old_type, new_type):
                    try:
                        with table.update_schema() as update:
                            update.update_column(name, new_type)
                        changes["widened"].append(f"{name}: {old_type} -> {new_type}")
                        logger.info(f"Widened column: {name}")
                    except Exception as e:
                        changes["errors"].append(f"Failed to widen {name}: {e}")

        return changes

    def create_scd2_view(self, table_name: str, namespace: str = "raw") -> str:
        return f"""
        CREATE OR REPLACE VIEW {namespace}.{table_name}_current AS
        SELECT * EXCEPT (rn)
        FROM (
            SELECT *, ROW_NUMBER() OVER (PARTITION BY id ORDER BY _cdc_ts_ms DESC, _cdc_op DESC) AS rn
            FROM {namespace}.{table_name}
            WHERE _cdc_op != 'd'
        )
        WHERE rn = 1
        """

    def _avro_to_iceberg_schema(self, avro_schema: dict) -> Schema:
        fields = []
        field_id = 1
        for field in avro_schema.get('fields', []):
            name = field['name']
            field_type = field['type']
            if isinstance(field_type, list):
                non_null_types = [t for t in field_type if t != 'null']
                iceberg_type = self.TYPE_MAPPING.get(non_null_types[0], StringType()) if non_null_types else StringType()
                nullable = 'null' in field_type
            else:
                iceberg_type = self.TYPE_MAPPING.get(field_type, StringType())
                nullable = False
            fields.append(NestedField(field_id, name, iceberg_type, required=not nullable))
            field_id += 1
        return Schema(*fields)

    def _is_safe_widening(self, old_type, new_type) -> bool:
        widening = {
            IntegerType: [LongType, FloatType, DoubleType],
            LongType: [FloatType, DoubleType],
            FloatType: [DoubleType],
        }
        return type(new_type) in widening.get(type(old_type), [])

    def get_table_metadata(self, table_name: str, namespace: str = "raw") -> Dict:
        identifier = f"{namespace}.{table_name}"
        table = self.catalog.load_table(identifier)
        return {
            "name": table_name, "location": table.location,
            "format_version": table.format_version,
            "current_schema": str(table.schema()),
            "partition_spec": str(table.spec()),
            "snapshots_count": len(table.snapshots()),
        }
