import logging
from pathlib import Path


def configure_logging(log_dir: str | None = None, level: int = logging.INFO) -> None:
    if log_dir:
        Path(log_dir).mkdir(parents=True, exist_ok=True)
    logging.basicConfig(
        level=level,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
