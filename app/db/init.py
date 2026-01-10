from __future__ import annotations

import argparse
import logging
from pathlib import Path

from sqlalchemy import inspect

from app.common.config import load_config
from app.common.logging import configure_logging
from app.db.base import Base, get_engine
import app.db.models  # <<< BU SATIR ŞART

logger = logging.getLogger(__name__)


def init_db(config_path: str) -> None:
    config = load_config(config_path)
    configure_logging(config.data_paths.logs_dir)
    engine = get_engine(config)
    Base.metadata.create_all(engine)
    inspector = inspect(engine)
    if "pipeline_runs" not in inspector.get_table_names():
        raise RuntimeError("Database init failed: pipeline_runs table was not created")
    logger.info("Database initialized at %s", config.db_url)


def main() -> int:
    parser = argparse.ArgumentParser(description="Initialize traffic database")
    parser.add_argument("--config", default="config.yaml", help="Path to config YAML")
    args = parser.parse_args()
    config_path = args.config
    try:
        if not Path(config_path).exists():
            raise FileNotFoundError(f"Config not found: {config_path}")
        init_db(config_path)
        return 0
    except Exception:
        logger.exception("Database init failed", extra={"config": config_path})
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
