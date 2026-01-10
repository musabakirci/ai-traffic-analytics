from dataclasses import replace

import pytest

from app.common.config import AppConfig, validate_config


def test_validate_config_rejects_invalid_conf_threshold() -> None:
    config = AppConfig()
    bad_detector = replace(config.detector, confidence_threshold=1.5)
    bad_config = replace(config, detector=bad_detector)
    with pytest.raises(ValueError):
        validate_config(bad_config)


def test_validate_config_rejects_invalid_density_thresholds() -> None:
    config = AppConfig()
    bad_density = replace(config.density, low_max=0.8, medium_max=0.5)
    bad_config = replace(config, density=bad_density)
    with pytest.raises(ValueError):
        validate_config(bad_config)
