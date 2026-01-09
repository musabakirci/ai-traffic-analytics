from sqlalchemy import create_engine, select, func
from sqlalchemy.orm import sessionmaker

from app.db.base import Base
from app.db.models import TrafficDensity, VehicleCount
from app.db.repositories import insert_density, insert_vehicle_counts, upsert_camera


def test_vehicle_counts_unique_constraint():
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)
    with Session() as session:
        upsert_camera(session, camera_id="CAM_001")
        rows = [
            {
                "camera_id": "CAM_001",
                "bucket_ts": "2024-01-01T00:00:00+00:00",
                "vehicle_type": "car",
                "count": 5,
                "source_video": "video.mp4",
            }
        ]
        insert_vehicle_counts(session, rows)
        insert_vehicle_counts(session, rows)
        total = session.execute(select(func.count()).select_from(VehicleCount)).scalar_one()
        assert total == 1


def test_density_unique_constraint():
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)
    with Session() as session:
        upsert_camera(session, camera_id="CAM_001")
        rows = [
            {
                "camera_id": "CAM_001",
                "bucket_ts": "2024-01-01T00:00:00+00:00",
                "total_vehicles": 10,
                "density_score": 0.5,
                "density_level": "medium",
                "bbox_occupancy": None,
                "source_video": "video.mp4",
            }
        ]
        insert_density(session, rows)
        insert_density(session, rows)
        total = session.execute(select(func.count()).select_from(TrafficDensity)).scalar_one()
        assert total == 1
