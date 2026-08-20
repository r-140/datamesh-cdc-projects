from decimal import Decimal
import pytest
from datamesh_cdc.hybrid_projection import ProjectionError, project


def test_additive_field_is_ignored_by_silver_contract():
    event = {"id": 1, "customer_id": 2, "total_amount": "12.50", "status": "paid", "coupon": "NEW"}
    assert project("orders", event) == {
        "id": 1,
        "customer_id": 2,
        "total_amount": Decimal("12.50"),
        "status": "paid",
    }
    assert event["coupon"] == "NEW"


def test_compatible_representation_is_coerced_on_write():
    row = project("orders", {"id": "1", "customer_id": "2", "total_amount": 12, "status": "paid"})
    assert row["id"] == 1 and row["total_amount"] == Decimal("12")


def test_breaking_event_fails_only_the_silver_projection():
    with pytest.raises(ProjectionError, match="customer_id: missing"):
        project("orders", {"id": 1, "total_amount": 12, "status": "paid"})
