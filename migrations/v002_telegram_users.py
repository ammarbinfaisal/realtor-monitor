"""
Add telegram_users table for webhook-based notifications

Stores Telegram chat IDs that receive notifications.
Users can start/stop the bot via /start and /stop commands.
"""


def upgrade(cursor) -> None:
    """Create telegram_users table."""

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS telegram_users (
            chat_id BIGINT PRIMARY KEY,
            username TEXT,
            first_name TEXT,
            last_name TEXT,
            is_active BOOLEAN DEFAULT TRUE,
            created_at TIMESTAMP DEFAULT NOW(),
            updated_at TIMESTAMP DEFAULT NOW()
        )
    """)

    # Index for active users lookup
    cursor.execute("""
        CREATE INDEX IF NOT EXISTS idx_telegram_users_active 
        ON telegram_users(is_active) WHERE is_active = TRUE
    """)
