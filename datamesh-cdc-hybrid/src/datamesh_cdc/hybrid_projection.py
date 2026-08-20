"""Projection from flexible Bronze events to governed Silver rows."""

from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from typing import Any, Callable, Mapping


class ProjectionError(ValueError):
    """An event is safe in Bronze but cannot satisfy the Silver contract."""


def as_int(value: Any) -> int:
    if isinstance(value, bool):
        raise ValueError("boolean is not an integer")
    return int(value)


def as_decimal(value: Any) -> Decimal:
    if isinstance(value, bool):
        raise ValueError("boolean is not a decimal")
    try:
        return Decimal(str(value))
    except InvalidOperation as exc:
        raise ValueError("not a decimal") from exc


def as_text(value: Any) -> str:
    if not isinstance(value, str):
        raise ValueError("not text")
    return value


@dataclass(frozen=True)
class Field:
    name: str
    converter: Callable[[Any], Any]
    required: bool = True


CONTRACTS = {
    "orders": (
        Field("id", as_int),
        Field("customer_id", as_int),
        Field("total_amount", as_decimal),
        Field("status", as_text),
    ),
    "customers": (
        Field("id", as_int),
        Field("email", as_text),
        Field("full_name", as_text),
        Field("country", as_text),
    ),
}


def project(table: str, payload: Mapping[str, Any]) -> dict[str, Any]:
    """Apply the Silver contract; extra source fields remain available in Bronze."""
    if table not in CONTRACTS:
        raise ProjectionError(f"no Silver contract for {table}")
    result, errors = {}, []
    for field in CONTRACTS[table]:
        value = payload.get(field.name)
        if value is None:
            if field.required:
                errors.append(f"{field.name}: missing")
            else:
                result[field.name] = None
            continue
        try:
            result[field.name] = field.converter(value)
        except (TypeError, ValueError) as exc:
            errors.append(f"{field.name}: {exc}")
    if errors:
        raise ProjectionError("; ".join(errors))
    return result
