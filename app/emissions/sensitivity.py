from __future__ import annotations


def sensitivity_interval(estimate: float, pct: float) -> tuple[float, float]:
    if pct < 0:
        raise ValueError("pct must be non-negative")
    delta = estimate * (pct / 100.0)
    return estimate - delta, estimate + delta
