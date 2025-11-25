"""
Initial schema migration

Creates the base tables: agents, listings
This migration captures the existing schema for new deployments.
"""


def upgrade(cursor) -> None:
    """Create initial database schema."""

    # Agents table
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS agents (
            agent_url TEXT PRIMARY KEY,
            agent_name TEXT,
            agent_phone TEXT,
            fetched_at TIMESTAMP DEFAULT NOW()
        )
    """)

    # Listings table
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS listings (
            listing_url TEXT PRIMARY KEY,
            property_id TEXT,
            address TEXT,
            city TEXT,
            county TEXT,
            state_code TEXT,
            postal_code TEXT,
            price INTEGER,
            beds INTEGER,
            baths REAL,
            sqft INTEGER,
            list_date TEXT,
            has_septic_system BOOLEAN DEFAULT FALSE,
            has_private_well BOOLEAN DEFAULT FALSE,
            septic_mentions JSONB DEFAULT '[]'::jsonb,
            well_mentions JSONB DEFAULT '[]'::jsonb,
            agent_url TEXT,
            agent_name TEXT,
            agent_phone TEXT,
            brokerage_name TEXT,
            first_seen_at TIMESTAMP DEFAULT NOW(),
            last_seen_at TIMESTAMP DEFAULT NOW(),
            times_seen INTEGER DEFAULT 1,
            scraped_at TIMESTAMP DEFAULT NOW()
        )
    """)

    # Indexes for common queries
    cursor.execute("""
        CREATE INDEX IF NOT EXISTS idx_listings_last_seen 
        ON listings(last_seen_at DESC)
    """)

    cursor.execute("""
        CREATE INDEX IF NOT EXISTS idx_listings_septic_well 
        ON listings(has_septic_system, has_private_well)
    """)

    cursor.execute("""
        CREATE INDEX IF NOT EXISTS idx_listings_city 
        ON listings(city)
    """)

    cursor.execute("""
        CREATE INDEX IF NOT EXISTS idx_listings_first_seen 
        ON listings(first_seen_at DESC)
    """)
