from __future__ import annotations

import argparse
import logging
from datetime import datetime
from pathlib import Path
from app.common.config import load_config
from app.common.logging import configure_logging
from app.pipeline.orchestrator import run_pipeline

logger = logging.getLogger(__name__)


def parse_start_time(value: str | None) -> datetime | None:
    if not value:
        return None
    return datetime.fromisoformat(value)


def main() -> int:
    parser = argparse.ArgumentParser(description="Run traffic analysis pipeline")
    parser.add_argument("--video", required=True, help="Path to video file")
    parser.add_argument("--camera-id", required=True, help="Camera identifier")
    parser.add_argument("--config", default="config.yaml", help="Path to config YAML")
    parser.add_argument("--location", default=None, help="Camera location name")
    parser.add_argument("--latitude", type=float, default=None, help="Camera latitude")
    parser.add_argument("--longitude", type=float, default=None, help="Camera longitude")
    parser.add_argument("--notes", default=None, help="Camera notes")
    parser.add_argument(
        "--start-time",
        default=None,
        help="ISO-8601 UTC timestamp for first bucket",
    )
    args = parser.parse_args()

    try:
        config_path = args.config
        if not Path(config_path).exists():
            raise FileNotFoundError(f"Config not found: {config_path}")
        config = load_config(config_path)
        configure_logging(config.data_paths.logs_dir)

        run_pipeline(
            video_path=args.video,
            camera_id=args.camera_id,
            config=config,
            camera_location=args.location,
            camera_latitude=args.latitude,
            camera_longitude=args.longitude,
            camera_notes=args.notes,
            start_time=parse_start_time(args.start_time),
        )
        return 0
    except Exception:
        logger.exception(
            "Pipeline failed",
            extra={
                "video": args.video,
                "camera_id": args.camera_id,
                "config": args.config,
            },
        )
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
