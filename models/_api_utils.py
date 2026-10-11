"""Batch validation shared by query and control APIs."""

import math

from utils.primitives import _is_sequence


def _batch_field_values(value, count, name, batch_name="vehID"):
    """Broadcast a scalar batch field or validate an explicit sequence."""
    if _is_sequence(value):
        values = list(value)
        if len(values) != count:
            raise ValueError(f"{name} must have the same length as {batch_name}")
        return values
    return [value] * count


def _optional_vehicle_lengths(value, count, batch_name):
    """Broadcast and validate optional vehicle lengths in meters."""
    values = _batch_field_values(value, count, "length", batch_name=batch_name)
    normalized = []
    for index, item in enumerate(values):
        if item is None:
            normalized.append(None)
            continue
        if isinstance(item, bool):
            raise ValueError(
                f"length[{index}] must be a finite positive value in meters"
            )
        try:
            item = float(item)
        except (TypeError, ValueError) as exc:
            raise ValueError(
                f"length[{index}] must be a finite positive value in meters"
            ) from exc
        if not math.isfinite(item) or item <= 0.0:
            raise ValueError(
                f"length[{index}] must be a finite positive value in meters"
            )
        normalized.append(item)
    return normalized
