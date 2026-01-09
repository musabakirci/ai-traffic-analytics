from __future__ import annotations

from typing import Mapping


def estimate_co2_kg(
    counts: Mapping[str, int], factors: Mapping[str, float], bucket_seconds: int
) -> float:
    scale = bucket_seconds / 60.0
    total = 0.0
    for vehicle_type, count in counts.items():
        factor = factors.get(vehicle_type)
        if factor is None:
            continue
        total += count * factor * scale
    return total
