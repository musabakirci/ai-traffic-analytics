from app.density.metrics import compute_density_score


def test_density_levels_boundaries():
    result_low = compute_density_score(total_vehicles=33, max_vehicles=100, low_max=0.33, medium_max=0.66)
    assert result_low.density_level == "low"

    result_medium = compute_density_score(total_vehicles=34, max_vehicles=100, low_max=0.33, medium_max=0.66)
    assert result_medium.density_level == "medium"

    result_high = compute_density_score(total_vehicles=67, max_vehicles=100, low_max=0.33, medium_max=0.66)
    assert result_high.density_level == "high"


def test_density_score_clamped():
    result = compute_density_score(total_vehicles=200, max_vehicles=100, low_max=0.33, medium_max=0.66)
    assert result.density_score == 1.0


def test_density_score_with_zero_max():
    result = compute_density_score(total_vehicles=5, max_vehicles=0, low_max=0.33, medium_max=0.66)
    assert result.density_score == 0.0
    assert result.density_level == "low"
