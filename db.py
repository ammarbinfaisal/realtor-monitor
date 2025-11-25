"""
Database layer for Realtor scraper - PostgreSQL
"""

from __future__ import annotations

import json
import os
import logging
from datetime import datetime
from typing import Optional
from contextlib import contextmanager

import psycopg2  # type: ignore
from psycopg2.extras import RealDictCursor, Json  # type: ignore

from models import Listing, Agent, DbStats
from migrations.runner import run_migrations

logger = logging.getLogger(__name__)


def get_database_url() -> str:
    """Get DATABASE_URL with validation"""
    url = os.environ.get("DATABASE_URL", "")

    if not url:
        raise RuntimeError(
            "DATABASE_URL environment variable is not set. "
            "Please set it to your PostgreSQL connection string."
        )

    # Railway uses postgres:// but psycopg2 needs postgresql://
    if url.startswith("postgres://"):
        url = url.replace("postgres://", "postgresql://", 1)

    return url


@contextmanager
def get_connection():
    """Get database connection with automatic cleanup"""
    url = get_database_url()
    conn = psycopg2.connect(url, cursor_factory=RealDictCursor)
    try:
        yield conn
    finally:
        conn.close()


def init_database():
    """Initialize PostgreSQL database - runs migrations"""
    logger.info("Initializing PostgreSQL database...")

    url = get_database_url()
    # Use a regular cursor for migrations (not RealDictCursor)
    conn = psycopg2.connect(url)
    try:
        run_migrations(conn)
        logger.info("Database initialized successfully")
    finally:
        conn.close()


def get_cached_agent(agent_url: str) -> Optional[Agent]:
    """Retrieve agent info from cache if exists"""
    with get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute(
            "SELECT agent_url, agent_name, agent_phone, fetched_at FROM agents WHERE agent_url = %s",
            (agent_url,),
        )
        row = cursor.fetchone()

        if row:
            logger.debug(f"Cache HIT for agent: {agent_url}")
            return Agent(
                agent_url=row["agent_url"],
                agent_name=row["agent_name"],
                agent_phone=row["agent_phone"],
                fetched_at=row["fetched_at"],
            )
        return None


def cache_agent(agent: Agent) -> None:
    """Cache agent information"""
    with get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute(
            """
            INSERT INTO agents (agent_url, agent_name, agent_phone)
            VALUES (%s, %s, %s)
            ON CONFLICT (agent_url) DO UPDATE SET
                agent_name = EXCLUDED.agent_name,
                agent_phone = EXCLUDED.agent_phone,
                fetched_at = NOW()
        """,
            (agent.agent_url, agent.agent_name or "", agent.agent_phone or ""),
        )
        conn.commit()
        logger.info(f"Cached agent: {agent.agent_name} ({agent.agent_phone})")


def save_listing(listing: Listing) -> tuple[bool, Listing]:
    """Save listing to database

    Returns:
        Tuple of (is_new, updated_listing)
    """
    with get_connection() as conn:
        cursor = conn.cursor()

        # Check if listing already exists
        cursor.execute(
            "SELECT times_seen FROM listings WHERE listing_url = %s",
            (listing.listing_url,),
        )
        existing = cursor.fetchone()

        if existing:
            # Update existing listing
            times_seen = (existing["times_seen"] or 1) + 1
            cursor.execute(
                """
                UPDATE listings SET
                    last_seen_at = NOW(),
                    times_seen = %s,
                    price = %s,
                    agent_name = %s,
                    agent_phone = %s,
                    brokerage_name = %s
                WHERE listing_url = %s
                RETURNING *
            """,
                (
                    times_seen,
                    listing.price,
                    listing.agent_name,
                    listing.agent_phone,
                    listing.brokerage_name,
                    listing.listing_url,
                ),
            )
            is_new = False
            logger.debug(
                f"Updated existing listing (seen {times_seen}x): {listing.listing_url}"
            )
        else:
            # Insert new listing
            cursor.execute(
                """
                INSERT INTO listings
                (listing_url, property_id, address, city, county, state_code, postal_code,
                 price, beds, baths, sqft, list_date,
                 has_septic_system, has_private_well, septic_mentions, well_mentions,
                 agent_url, agent_name, agent_phone, brokerage_name)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                RETURNING *
            """,
                (
                    listing.listing_url,
                    listing.property_id,
                    listing.address,
                    listing.city,
                    listing.county,
                    listing.state_code,
                    listing.postal_code,
                    listing.price,
                    listing.beds,
                    listing.baths,
                    listing.sqft,
                    listing.list_date,
                    listing.has_septic_system,
                    listing.has_private_well,
                    Json(listing.septic_mentions),
                    Json(listing.well_mentions),
                    listing.agent_url,
                    listing.agent_name,
                    listing.agent_phone,
                    listing.brokerage_name,
                ),
            )
            is_new = True
            logger.debug(f"Inserted new listing: {listing.listing_url}")

        row = cursor.fetchone()
        conn.commit()

        return is_new, Listing.from_db_row(dict(row)) if row else listing


def get_listings(
    since: Optional[datetime] = None,
    date_from: Optional[datetime] = None,
    date_to: Optional[datetime] = None,
    septic_only: bool = False,
    well_only: bool = False,
    city: Optional[str] = None,
    search: Optional[str] = None,
    limit: int = 100,
) -> list[Listing]:
    """Get listings with optional filters

    Args:
        since: Get listings updated after this time (last_seen_at)
        date_from: Get listings first seen after this date
        date_to: Get listings first seen before this date
        septic_only: Filter to septic system only
        well_only: Filter to private well only
        city: Filter by city
        search: Search query - supports | for OR matching (partial match on address, city, county)
        limit: Max results
    """
    with get_connection() as conn:
        cursor = conn.cursor()

        query = "SELECT * FROM listings WHERE 1=1"
        params: list = []

        if since:
            query += " AND last_seen_at > %s"
            params.append(since)

        if date_from:
            query += " AND first_seen_at >= %s"
            params.append(date_from)

        if date_to:
            query += " AND first_seen_at <= %s"
            params.append(date_to)

        if septic_only:
            query += " AND has_septic_system = true"

        if well_only:
            query += " AND has_private_well = true"

        if city:
            query += " AND city = %s"
            params.append(city)

        # Handle search with OR operator support
        if search:
            # Split by | for OR matching
            search_terms = [term.strip() for term in search.split("|") if term.strip()]
            if search_terms:
                search_conditions = []
                for term in search_terms:
                    # Each term does a partial match on address, city, or county
                    search_conditions.append(
                        "(LOWER(address) LIKE %s OR LOWER(city) LIKE %s OR LOWER(county) LIKE %s)"
                    )
                    like_pattern = f"%{term.lower()}%"
                    params.extend([like_pattern, like_pattern, like_pattern])

                # Combine with OR
                query += f" AND ({' OR '.join(search_conditions)})"

        query += " ORDER BY first_seen_at DESC LIMIT %s"
        params.append(limit)

        cursor.execute(query, params)
        return [Listing.from_db_row(dict(row)) for row in cursor.fetchall()]


def get_new_septic_well_listings(hours: int = 24) -> list[Listing]:
    """Get listings with septic/well first seen in the last N hours"""
    with get_connection() as conn:
        cursor = conn.cursor()

        cursor.execute(
            """
            SELECT * FROM listings 
            WHERE first_seen_at > NOW() - INTERVAL '%s hours'
            AND (has_septic_system = true OR has_private_well = true)
            ORDER BY first_seen_at DESC
        """,
            (hours,),
        )

        return [Listing.from_db_row(dict(row)) for row in cursor.fetchall()]


def get_all_cities() -> list[str]:
    """Get all unique cities"""
    with get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT DISTINCT city FROM listings 
            WHERE city IS NOT NULL 
            ORDER BY city
        """)
        return [row["city"] for row in cursor.fetchall()]


def get_stats() -> DbStats:
    """Get database statistics"""
    with get_connection() as conn:
        cursor = conn.cursor()

        cursor.execute("SELECT COUNT(*) as total FROM listings")
        row = cursor.fetchone()
        total = row["total"] if row else 0

        cursor.execute(
            "SELECT COUNT(*) as count FROM listings WHERE has_septic_system = true"
        )
        row = cursor.fetchone()
        septic = row["count"] if row else 0

        cursor.execute(
            "SELECT COUNT(*) as count FROM listings WHERE has_private_well = true"
        )
        row = cursor.fetchone()
        well = row["count"] if row else 0

        cursor.execute("""
            SELECT COUNT(*) as count FROM listings 
            WHERE first_seen_at > NOW() - INTERVAL '24 hours'
        """)
        row = cursor.fetchone()
        new_24h = row["count"] if row else 0

        return DbStats(
            total_listings=total,
            with_septic=septic,
            with_well=well,
            new_last_24h=new_24h,
        )
