"""
Migration runner - handles automatic migrations on deploy

Migrations are tracked in a `schema_migrations` table.
Each migration runs once and is recorded with its version.
"""

from __future__ import annotations

import importlib.util
import logging
import os
from pathlib import Path
from typing import Callable

logger = logging.getLogger(__name__)

# Directory containing migration files
MIGRATIONS_DIR = Path(__file__).parent


def get_migration_files() -> list[tuple[int, str, Path]]:
    """
    Get all migration files sorted by version number.

    Migration files must be named: v{NNN}_{description}.py
    e.g., v001_initial_schema.py, v002_add_status_column.py
    """
    migrations = []

    for file in MIGRATIONS_DIR.glob("v*.py"):
        name = file.stem  # e.g., "v001_initial_schema"
        try:
            version = int(name[1:4])  # Extract version number
            migrations.append((version, name, file))
        except (ValueError, IndexError):
            logger.warning(f"Skipping invalid migration file: {file}")
            continue

    return sorted(migrations, key=lambda x: x[0])


def create_migrations_table(cursor) -> None:
    """Create the schema_migrations tracking table if it doesn't exist."""
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS schema_migrations (
            version INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            applied_at TIMESTAMP DEFAULT NOW()
        )
    """)


def get_applied_versions(cursor) -> set[int]:
    """Get set of already applied migration versions."""
    cursor.execute("SELECT version FROM schema_migrations")
    return {row[0] for row in cursor.fetchall()}


def record_migration(cursor, version: int, name: str) -> None:
    """Record that a migration has been applied."""
    cursor.execute(
        "INSERT INTO schema_migrations (version, name) VALUES (%s, %s)", (version, name)
    )


def run_migrations(conn) -> int:
    """
    Run all pending migrations.

    Args:
        conn: Database connection (psycopg2)

    Returns:
        Number of migrations applied
    """
    cursor = conn.cursor()

    # Ensure migrations table exists
    create_migrations_table(cursor)
    conn.commit()

    # Get applied versions
    applied = get_applied_versions(cursor)

    # Get all migration files
    migrations = get_migration_files()

    applied_count = 0

    for version, name, filepath in migrations:
        if version in applied:
            logger.debug(f"Migration {name} already applied, skipping")
            continue

        logger.info(f"Applying migration: {name}")

        try:
            # Import the migration module
            spec = importlib.util.spec_from_file_location(name, filepath)
            if spec is None or spec.loader is None:
                raise ImportError(f"Could not load migration: {filepath}")

            module = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(module)

            # Run the upgrade function
            if hasattr(module, "upgrade"):
                module.upgrade(cursor)
            else:
                raise AttributeError(f"Migration {name} missing upgrade() function")

            # Record successful migration
            record_migration(cursor, version, name)
            conn.commit()

            logger.info(f"Migration {name} applied successfully")
            applied_count += 1

        except Exception as e:
            conn.rollback()
            logger.error(f"Migration {name} failed: {e}")
            raise RuntimeError(f"Migration {name} failed: {e}") from e

    if applied_count == 0:
        logger.info("No pending migrations")
    else:
        logger.info(f"Applied {applied_count} migration(s)")

    return applied_count
