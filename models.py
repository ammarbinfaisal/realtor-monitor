"""
Data models for Realtor scraper
"""

from __future__ import annotations

from dataclasses import dataclass, field, asdict
from datetime import datetime
from typing import Optional


@dataclass
class Agent:
    """Agent information"""

    agent_url: str
    agent_name: Optional[str] = None
    agent_phone: Optional[str] = None
    fetched_at: Optional[datetime] = None

    def to_dict(self) -> dict:
        return {k: v for k, v in asdict(self).items() if v is not None}


@dataclass
class Listing:
    """Real estate listing"""

    listing_url: str
    property_id: Optional[str] = None
    address: Optional[str] = None
    city: Optional[str] = None
    county: Optional[str] = None
    state_code: Optional[str] = None
    postal_code: Optional[str] = None
    price: Optional[int] = None
    beds: Optional[int] = None
    baths: Optional[float] = None
    sqft: Optional[int] = None
    list_date: Optional[str] = None
    has_septic_system: bool = False
    has_private_well: bool = False
    septic_mentions: list[str] = field(default_factory=list)
    well_mentions: list[str] = field(default_factory=list)
    agent_url: Optional[str] = None
    agent_name: Optional[str] = None
    agent_phone: Optional[str] = None
    brokerage_name: Optional[str] = None
    first_seen_at: Optional[datetime] = None
    last_seen_at: Optional[datetime] = None
    times_seen: int = 1
    scraped_at: Optional[datetime] = None

    # Alias for compatibility
    @property
    def url(self) -> str:
        return self.listing_url

    @property
    def has_septic(self) -> bool:
        return self.has_septic_system

    @property
    def has_well(self) -> bool:
        return self.has_private_well

    def to_dict(self) -> dict:
        """Convert to dictionary, handling datetime serialization"""
        data = asdict(self)
        # Convert datetime to ISO string for JSON serialization
        for key, value in data.items():
            if isinstance(value, datetime):
                data[key] = value.isoformat()
        return data

    @classmethod
    def from_dict(cls, data: dict) -> Listing:
        """Create Listing from dictionary"""
        # Handle field name variations
        listing_url = data.get("listing_url") or data.get("url", "")

        return cls(
            listing_url=listing_url,
            property_id=data.get("property_id"),
            address=data.get("address"),
            city=data.get("city"),
            county=data.get("county"),
            state_code=data.get("state_code"),
            postal_code=data.get("postal_code"),
            price=data.get("price"),
            beds=data.get("beds"),
            baths=data.get("baths"),
            sqft=data.get("sqft"),
            list_date=data.get("list_date"),
            has_septic_system=data.get("has_septic_system")
            or data.get("has_septic", False),
            has_private_well=data.get("has_private_well")
            or data.get("has_well", False),
            septic_mentions=data.get("septic_mentions", []),
            well_mentions=data.get("well_mentions", []),
            agent_url=data.get("agent_url"),
            agent_name=data.get("agent_name"),
            agent_phone=data.get("agent_phone"),
            brokerage_name=data.get("brokerage_name"),
            first_seen_at=_parse_datetime(data.get("first_seen_at")),
            last_seen_at=_parse_datetime(data.get("last_seen_at")),
            times_seen=data.get("times_seen", 1),
            scraped_at=_parse_datetime(data.get("scraped_at")),
        )

    @classmethod
    def from_db_row(cls, row: dict) -> Listing:
        """Create Listing from database row"""
        return cls.from_dict(row)


@dataclass
class ScraperStats:
    """Statistics from a scraper run"""

    total_processed: int = 0
    new_listings: int = 0
    updated_listings: int = 0
    septic_well_count: int = 0
    skipped_count: int = 0
    errors: list[str] = field(default_factory=list)
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None

    @property
    def duration_seconds(self) -> Optional[float]:
        if self.started_at and self.completed_at:
            return (self.completed_at - self.started_at).total_seconds()
        return None

    def to_dict(self) -> dict:
        data = asdict(self)
        if self.started_at:
            data["started_at"] = self.started_at.isoformat()
        if self.completed_at:
            data["completed_at"] = self.completed_at.isoformat()
        data["duration_seconds"] = self.duration_seconds
        return data


@dataclass
class DbStats:
    """Database statistics"""

    total_listings: int = 0
    with_septic: int = 0
    with_well: int = 0
    new_last_24h: int = 0

    def to_dict(self) -> dict:
        return asdict(self)


def _parse_datetime(value) -> Optional[datetime]:
    """Parse datetime from various formats"""
    if value is None:
        return None
    if isinstance(value, datetime):
        return value
    if isinstance(value, str):
        try:
            # ISO format
            return datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError:
            pass
        try:
            # Common DB format
            return datetime.strptime(value, "%Y-%m-%d %H:%M:%S")
        except ValueError:
            pass
    return None
