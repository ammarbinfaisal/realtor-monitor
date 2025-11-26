"""
SQLAlchemy ORM models for Realtor scraper
"""

from __future__ import annotations

import os
from datetime import datetime
from typing import Optional

from sqlalchemy import (
    Boolean,
    DateTime,
    Float,
    Integer,
    String,
    create_engine,
    func,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, sessionmaker


def get_database_url() -> str:
    """Get DATABASE_URL with validation"""
    url = os.environ.get("DATABASE_URL", "")

    if not url:
        raise RuntimeError(
            "DATABASE_URL environment variable is not set. "
            "Please set it to your PostgreSQL connection string."
        )

    # Railway uses postgres:// but SQLAlchemy needs postgresql://
    if url.startswith("postgres://"):
        url = url.replace("postgres://", "postgresql://", 1)

    return url


# Create engine and session factory
engine = None
SessionLocal = None


def init_engine():
    """Initialize the database engine"""
    global engine, SessionLocal
    if engine is None:
        engine = create_engine(get_database_url(), pool_pre_ping=True)
        SessionLocal = sessionmaker(bind=engine, autocommit=False, autoflush=False)
    return engine


def get_session():
    """Get a new database session"""
    if SessionLocal is None:
        init_engine()
    return SessionLocal()


class Base(DeclarativeBase):
    """Base class for all models"""

    pass


class ListingModel(Base):
    """Real estate listing"""

    __tablename__ = "listings"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    listing_url: Mapped[str] = mapped_column(String(500), unique=True, nullable=False)
    property_id: Mapped[Optional[str]] = mapped_column(String(50), index=True)
    address: Mapped[Optional[str]] = mapped_column(String(255))
    city: Mapped[Optional[str]] = mapped_column(String(100), index=True)
    county: Mapped[Optional[str]] = mapped_column(String(100), index=True)
    state_code: Mapped[Optional[str]] = mapped_column(String(10))
    postal_code: Mapped[Optional[str]] = mapped_column(String(20))
    price: Mapped[Optional[int]] = mapped_column(Integer)
    beds: Mapped[Optional[int]] = mapped_column(Integer)
    baths: Mapped[Optional[float]] = mapped_column(Float)
    sqft: Mapped[Optional[int]] = mapped_column(Integer)
    list_date: Mapped[Optional[str]] = mapped_column(String(50))

    # Septic/Well detection
    has_septic_system: Mapped[bool] = mapped_column(Boolean, default=False, index=True)
    has_private_well: Mapped[bool] = mapped_column(Boolean, default=False, index=True)
    septic_mentions: Mapped[Optional[list]] = mapped_column(JSONB, default=list)
    well_mentions: Mapped[Optional[list]] = mapped_column(JSONB, default=list)

    # Agent info
    agent_url: Mapped[Optional[str]] = mapped_column(String(500))
    agent_name: Mapped[Optional[str]] = mapped_column(String(255))
    agent_phone: Mapped[Optional[str]] = mapped_column(String(50))
    brokerage_name: Mapped[Optional[str]] = mapped_column(String(255))

    # Timestamps
    first_seen_at: Mapped[datetime] = mapped_column(
        DateTime, server_default=func.now(), nullable=False
    )
    last_seen_at: Mapped[datetime] = mapped_column(
        DateTime, server_default=func.now(), onupdate=func.now(), nullable=False
    )
    times_seen: Mapped[int] = mapped_column(Integer, default=1)
