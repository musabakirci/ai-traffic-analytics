from __future__ import annotations

from sqlalchemy import create_engine
from sqlalchemy.orm import declarative_base, sessionmaker

from app.common.config import AppConfig

Base = declarative_base()


def get_engine(config: AppConfig):
    return create_engine(config.db_url, future=True)


def get_session_factory(engine):
    return sessionmaker(bind=engine, future=True)
