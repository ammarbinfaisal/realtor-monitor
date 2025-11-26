"""
Database layer for Realtor scraper - SQLAlchemy
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta
from typing import Optional
from contextlib import contextmanager

from sqlalchemy import select, func, or_

from orm import init_engine, get_session, ListingModel
from models import Listing, DbStats

logger = logging.getLogger(__name__)


def init_database():
    """Initialize database - runs Alembic migrations"""
    import subprocess
    import sys

    logger.info("Running Alembic migrations...")
    try:
        result = subprocess.run(
            [sys.executable, "-m", "alembic", "upgrade", "head"],
            capture_output=True,
            text=True,
        )
        if result.returncode != 0:
            logger.error(f"Migration failed: {result.stderr}")
            raise RuntimeError(f"Migration failed: {result.stderr}")
        logger.info("Database migrations complete")
    except Exception as e:
        logger.error(f"Failed to run migrations: {e}")
        raise


@contextmanager
def get_db():
    """Get database session with automatic cleanup"""
    init_engine()
    session = get_session()
    try:
        yield session
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()


def _listing_model_to_pydantic(db_listing: ListingModel) -> Listing:
    """Convert SQLAlchemy model to Pydantic model"""
    return Listing(
        listing_url=db_listing.listing_url,
        property_id=db_listing.property_id,
        address=db_listing.address,
        city=db_listing.city,
        county=db_listing.county,
        state_code=db_listing.state_code,
        postal_code=db_listing.postal_code,
        price=db_listing.price,
        beds=db_listing.beds,
        baths=db_listing.baths,
        sqft=db_listing.sqft,
        list_date=db_listing.list_date,
        has_septic_system=db_listing.has_septic_system,
        has_private_well=db_listing.has_private_well,
        septic_mentions=db_listing.septic_mentions or [],
        well_mentions=db_listing.well_mentions or [],
        agent_url=db_listing.agent_url,
        agent_name=db_listing.agent_name,
        agent_phone=db_listing.agent_phone,
        brokerage_name=db_listing.brokerage_name,
        first_seen_at=db_listing.first_seen_at,
        last_seen_at=db_listing.last_seen_at,
        times_seen=db_listing.times_seen,
    )


def save_listing(listing: Listing) -> tuple[bool, Listing]:
    """Save listing to database

    Returns:
        Tuple of (is_new, updated_listing)
    """
    with get_db() as session:
        stmt = select(ListingModel).where(
            ListingModel.listing_url == listing.listing_url
        )
        existing = session.execute(stmt).scalar_one_or_none()

        if existing:
            # Update existing listing
            existing.times_seen = (existing.times_seen or 1) + 1
            existing.last_seen_at = datetime.utcnow()
            existing.price = listing.price
            existing.agent_name = listing.agent_name
            existing.agent_phone = listing.agent_phone
            existing.brokerage_name = listing.brokerage_name

            is_new = False
            logger.debug(
                f"Updated existing listing (seen {existing.times_seen}x): {listing.listing_url}"
            )
            session.flush()
            return is_new, _listing_model_to_pydantic(existing)
        else:
            # Insert new listing
            db_listing = ListingModel(
                listing_url=listing.listing_url,
                property_id=listing.property_id,
                address=listing.address,
                city=listing.city,
                county=listing.county,
                state_code=listing.state_code,
                postal_code=listing.postal_code,
                price=listing.price,
                beds=listing.beds,
                baths=listing.baths,
                sqft=listing.sqft,
                list_date=listing.list_date,
                has_septic_system=listing.has_septic_system,
                has_private_well=listing.has_private_well,
                septic_mentions=listing.septic_mentions,
                well_mentions=listing.well_mentions,
                agent_url=listing.agent_url,
                agent_name=listing.agent_name,
                agent_phone=listing.agent_phone,
                brokerage_name=listing.brokerage_name,
            )
            session.add(db_listing)
            is_new = True
            logger.debug(f"Inserted new listing: {listing.listing_url}")
            session.flush()
            return is_new, _listing_model_to_pydantic(db_listing)


def get_listings(
    since: Optional[datetime] = None,
    date_from: Optional[datetime] = None,
    date_to: Optional[datetime] = None,
    city: Optional[str] = None,
    search: Optional[str] = None,
    limit: int = 100,
) -> list[Listing]:
    """Get listings with optional filters"""
    with get_db() as session:
        stmt = select(ListingModel)

        if since:
            stmt = stmt.where(ListingModel.last_seen_at > since)

        if date_from:
            stmt = stmt.where(ListingModel.first_seen_at >= date_from)

        if date_to:
            stmt = stmt.where(ListingModel.first_seen_at <= date_to)

        if city:
            stmt = stmt.where(ListingModel.city == city)

        # Handle search with OR operator support
        if search:
            search_terms = [term.strip() for term in search.split("|") if term.strip()]
            if search_terms:
                search_conditions = []
                for term in search_terms:
                    like_pattern = f"%{term.lower()}%"
                    search_conditions.append(
                        or_(
                            func.lower(ListingModel.address).like(like_pattern),
                            func.lower(ListingModel.city).like(like_pattern),
                            func.lower(ListingModel.county).like(like_pattern),
                        )
                    )
                stmt = stmt.where(or_(*search_conditions))

        # Sort with septic/well at top
        stmt = stmt.order_by(
            (ListingModel.has_septic_system | ListingModel.has_private_well).desc(),
            ListingModel.first_seen_at.desc(),
        ).limit(limit)

        results = session.execute(stmt).scalars().all()
        return [_listing_model_to_pydantic(r) for r in results]


def get_new_septic_well_listings(hours: int = 24) -> list[Listing]:
    """Get listings with septic/well first seen in the last N hours"""
    with get_db() as session:
        cutoff = datetime.utcnow() - timedelta(hours=hours)

        stmt = (
            select(ListingModel)
            .where(ListingModel.first_seen_at > cutoff)
            .where(
                or_(
                    ListingModel.has_septic_system == True,
                    ListingModel.has_private_well == True,
                )
            )
            .order_by(ListingModel.first_seen_at.desc())
        )

        results = session.execute(stmt).scalars().all()
        return [_listing_model_to_pydantic(r) for r in results]


def get_all_cities() -> list[str]:
    """Get all unique cities"""
    with get_db() as session:
        stmt = (
            select(ListingModel.city)
            .where(ListingModel.city.isnot(None))
            .distinct()
            .order_by(ListingModel.city)
        )
        results = session.execute(stmt).scalars().all()
        return [r for r in results if r]


def get_stats() -> DbStats:
    """Get database statistics"""
    with get_db() as session:
        total = session.execute(select(func.count(ListingModel.id))).scalar() or 0

        septic = (
            session.execute(
                select(func.count(ListingModel.id)).where(
                    ListingModel.has_septic_system == True
                )
            ).scalar()
            or 0
        )

        well = (
            session.execute(
                select(func.count(ListingModel.id)).where(
                    ListingModel.has_private_well == True
                )
            ).scalar()
            or 0
        )

        cutoff = datetime.utcnow() - timedelta(hours=24)
        new_24h = (
            session.execute(
                select(func.count(ListingModel.id)).where(
                    ListingModel.first_seen_at > cutoff
                )
            ).scalar()
            or 0
        )

        return DbStats(
            total_listings=total,
            with_septic=septic,
            with_well=well,
            new_last_24h=new_24h,
        )


def get_septic_well_listings_in_window(
    from_time: datetime, to_time: datetime
) -> list[Listing]:
    """
    Get listings with septic/well that have list_date within the specified window.

    Args:
        from_time: Start of window (inclusive)
        to_time: End of window (exclusive)

    Returns:
        List of Listing objects with septic or well
    """
    with get_db() as session:
        # Convert datetime to date strings for comparison with list_date
        from_date = from_time.strftime("%Y-%m-%d")
        to_date = to_time.strftime("%Y-%m-%d")

        stmt = (
            select(ListingModel)
            .where(
                or_(
                    ListingModel.has_septic_system == True,
                    ListingModel.has_private_well == True,
                )
            )
            .where(ListingModel.list_date.isnot(None))
            .where(ListingModel.list_date >= from_date)
            .where(ListingModel.list_date <= to_date)
            .order_by(ListingModel.list_date.desc(), ListingModel.first_seen_at.desc())
        )

        results = session.execute(stmt).scalars().all()
        return [_listing_model_to_pydantic(r) for r in results]
