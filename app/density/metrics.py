from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class DensityResult:
    density_score: float
    density_level: str


def compute_density_score(
    total_vehicles: int,
    max_vehicles: int,
    low_max: float,
    medium_max: float,
) -> DensityResult:
    if max_vehicles <= 0:
        score = 0.0
    else:
        score = min(1.0, total_vehicles / max_vehicles)
    if score <= low_max:
        level = "low"
    elif score <= medium_max:
        level = "medium"
    else:
        level = "high"
    return DensityResult(density_score=score, density_level=level)
