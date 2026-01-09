from app.emissions.factors import estimate_co2_kg
from app.emissions.sensitivity import sensitivity_interval


def test_emissions_scaled_by_bucket_seconds():
    counts = {"car": 2, "bus": 1}
    factors = {"car": 0.2, "bus": 1.0}
    estimate = estimate_co2_kg(counts, factors, bucket_seconds=120)
    assert estimate == (2 * 0.2 + 1 * 1.0) * 2


def test_sensitivity_interval():
    low, high = sensitivity_interval(10.0, 10)
    assert low == 9.0
    assert high == 11.0
