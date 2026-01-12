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

## Realtime Mode (v3)
The realtime layer adds a WebSocket stream for live detections and rolling metrics without
changing the offline pipeline or database flow.

1. Start the realtime WebSocket server:
   ```bash
   uvicorn app.realtime.server:app --host 0.0.0.0 --port 8000
   ```
2. Enable realtime in `config.yaml`:
   ```yaml
   realtime:
     enabled: true
     websocket_url: ws://localhost:8000/ws/live
   ```
3. Run the pipeline (detections will emit live events):
   ```bash
   python -m app.pipeline.run --video data/videos/sample.mp4 --camera-id CAM_001 --config config.yaml
   ```
4. Launch the realtime dashboard:
   ```bash
   python -m app.dashboard.realtime --config config.yaml
   ```

## Architecture & Design Decisions
- Offline, video-based processing avoids live CCTV/MOBESE integrations and aligns with privacy constraints.
- YOLO is optional and config-driven so CPU-only environments can run without external weights.
- DummyDetector provides deterministic tests and a safe fallback when YOLO is unavailable.
- Clear separation of concerns across ingestion, detection, aggregation, density, emissions, storage, analytics, and UI.
- CPU-first design relies on frame sampling and bucketed aggregation to keep compute predictable.

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

## Windows Quick Start (PowerShell)
```powershell
python -m venv .venv
.venv\Scripts\Activate.ps1
pip install -r requirements.txt
python -m app.db.init --config config.yaml
python -m app.pipeline.run --video data/videos/sample.mp4 --camera-id CAM_001 --config config.yaml
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

## Dashboard Insights
The dashboard summarizes YOLO-derived results with KPIs (vehicle totals, density, CO2) and
charts for vehicle counts over time, class breakdowns, emissions trends, and density category
distribution.

## How to Explain This Project in an Interview (2-3 minutes)
Start with the problem: cities need consistent traffic and emissions insights from video archives
without live integrations or privacy-invasive features. Then explain the solution: an offline
pipeline that samples frames, detects vehicles, aggregates counts into time buckets, derives
density and emission estimates, stores everything in SQLite, and serves analytics in Streamlit.
Highlight the architecture: modular detection (Dummy vs YOLO), isolated processing stages, and
config-driven behavior for reproducibility. Close with results and tradeoffs: CPU-first performance,
explainable metrics, and a clear path to tracking or improved emission models.

Key technical talking points:
- Config-driven pipeline with explicit interfaces and error handling.
- Optional YOLO + deterministic DummyDetector for testability and fallback.
- SQL-backed analytics that power KPIs and charts without recomputing metrics in the UI.
- Transparent density scoring and factor-based emissions with sensitivity bounds.

What makes it non-trivial:
- Balancing accuracy and performance on CPU-only environments.
- Designing for privacy while still producing actionable traffic metrics.
- Keeping detection, aggregation, and visualization independent for maintainability.

## Example CV Bullet Points
- Built an offline traffic analytics pipeline (Python, OpenCV/YOLO, SQLAlchemy) that converts video into vehicle counts, density metrics, and CO2 estimates with a Streamlit dashboard.
- Designed a modular detector interface with a deterministic DummyDetector fallback to keep CI stable and support CPU-only deployments.
- Implemented bucketed aggregation and SQL-driven analytics queries to deliver explainable KPIs and time-series insights without reprocessing raw frames.

## Optional Diagram Note
Place a compact architecture diagram directly under "Architecture (MVP)" to show the data flow from
video ingestion to detection, aggregation, storage, and dashboard visualization.

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
      type: yolo
      model_path: C:/models/yolov8n.pt
    ```

## Visual Vehicle Detection (YOLO)
To see bounding boxes in real time while processing a local video, enable YOLO with visualization:
```yaml
detector:
  type: yolo
  model_path: C:/models/yolov8n.pt
  conf_threshold: 0.25
  visualize: true
  visualize_every_n: 1
  display_resize_width: null
  save_annotated_video: false
  annotated_output_path: null
```
Install optional dependencies:
```bash
pip install ultralytics opencv-python
```
When enabled, the pipeline opens a window and shows vehicle labels with confidence scores. Press `q` to stop.
Performance tuning: increase `visualize_every_n` to skip display frames, or set
`display_resize_width` (e.g., 960) to resize only the visualization output while keeping
detections on the original frames.

To save an annotated .mp4 alongside (optional):
```yaml
detector:
  save_annotated_video: true
  annotated_output_path: data/videos/traffic_annotated.mp4
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
