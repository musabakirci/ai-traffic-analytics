# Akilli Trafik Yogunlugu ve Emisyon Analizi

## Problem Statement
Offline traffic videos often lack a consistent, reproducible pipeline to quantify vehicle volumes, estimate congestion, and approximate CO2 emissions. This project provides a CPU-friendly, modular pipeline that processes local video files (mp4/avi) to produce vehicle counts, density metrics, and emission estimates, storing results in a relational database and exposing a Streamlit dashboard.

## Architecture (MVP)
```
Video File
  -> Frame Sampler
     -> Detector (Dummy or YOLO)
        -> Frame-based Counting
           -> Density Metrics
              -> Emission Estimation
                 -> SQLite (SQLAlchemy)
                    -> Streamlit Dashboard
```

## Setup
1. Create a virtual environment and install dependencies:
   ```bash
   python -m venv .venv
   .venv\Scripts\activate
   pip install -r requirements.txt
   ```
2. Initialize the database:
   ```bash
   python -m app.db.init --config config.yaml
   ```

## Run the Pipeline
```bash
python -m app.pipeline.run --video data/videos/sample.mp4 --camera-id CAM_001 --config config.yaml
```

Optional camera metadata:
```bash
python -m app.pipeline.run --video data/videos/sample.mp4 --camera-id CAM_001 --location "Ankara Center" --latitude 39.93 --longitude 32.85 --notes "Test camera"
```

## Launch the Dashboard
```bash
python -m app.dashboard.app --config config.yaml
```

## Density and Emission Assumptions
- **Density score**: normalized by a per-camera maximum vehicle count per bucket.
  - `density_score = min(1, total_vehicles / max_vehicles_camera)`
  - Levels: low [0, 0.33], medium (0.33, 0.66], high (0.66, 1]
  - The max can be a rolling max from history or a configured fixed value.
- **Emissions**: factor-based estimation (kg CO2 per vehicle per minute).
  - `estimated_co2_kg = sum(count_type * factor_type) * (bucket_seconds / 60)`
  - Sensitivity analysis applies +/- percentage to yield min/max intervals.

## Configuration
- `config.yaml` controls sampling, bucket size, class mapping, emission factors, and DB paths.
- Optional environment overrides:
  - `TRAFFIC_AI_CONFIG` for config path
  - `TRAFFIC_AI_DB_URL` for a SQLAlchemy DB URL (e.g., PostgreSQL)

## Detector Options
- **DummyDetector** (default): deterministic, CPU-only, configurable random detections.
- **YOLODetector** (optional): requires `ultralytics` and a local weights file.
  - Update `config.yaml`:
    ```yaml
    detector:
      name: yolo
      model_path: C:/models/yolov8n.pt
    ```

## Database Schema
- `traffic_cameras`
- `vehicle_counts`
- `traffic_density`
- `emission_estimates`

(See `app/db/models.py` for details and indices.)

## Screenshots
- Dashboard overview (placeholder)
- Time series charts (placeholder)

## Limitations
- Frame-based counting can over/under-count without tracking.
- Emission factors are configurable placeholders, not ground-truth.
- Video timestamp alignment defaults to pipeline start time.

## Future Work
- Tracking-based counting (e.g., SORT/DeepSORT).
- Per-camera calibration and ROI masking.
- Real-world emission models and calibration datasets.
- PostgreSQL migrations and deployment hardening.

## Quality Gate: PASSED
- All unit tests are passing (config validation, bucketing, density,      emissions, database constraints).
