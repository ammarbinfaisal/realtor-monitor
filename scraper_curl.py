#!/usr/bin/env python3
"""
Realtor.com Wisconsin Listings Scraper
Uses curl_cffi to bypass TLS fingerprinting
"""

import asyncio
import json
import logging
import re
import sqlite3
import time
from datetime import datetime, timedelta
from typing import Optional

from bs4 import BeautifulSoup
from curl_cffi import requests
from curl_cffi.requests import AsyncSession

# Configure logging
logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.FileHandler("scraper_curl.log"), logging.StreamHandler()],
)
logger = logging.getLogger(__name__)

# Database setup
DB_NAME = "realtor_cache.db"


def init_database():
    """Initialize SQLite database with required tables"""
    logger.info("Initializing SQLite database...")
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS agents (
            agent_url TEXT PRIMARY KEY,
            agent_name TEXT,
            agent_phone TEXT,
            fetched_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS listings (
            listing_url TEXT PRIMARY KEY,
            property_id TEXT,
            address TEXT,
            city TEXT,
            state_code TEXT,
            postal_code TEXT,
            price INTEGER,
            beds INTEGER,
            baths REAL,
            sqft INTEGER,
            list_date TEXT,
            has_septic_system BOOLEAN,
            has_private_well BOOLEAN,
            septic_mentions TEXT,
            well_mentions TEXT,
            agent_url TEXT,
            agent_name TEXT,
            agent_phone TEXT,
            brokerage_name TEXT,
            first_seen_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            last_seen_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            times_seen INTEGER DEFAULT 1,
            scraped_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (agent_url) REFERENCES agents(agent_url)
        )
    """)

    # Migration: Add new columns if they don't exist (for existing databases)
    new_columns = [
        ("property_id", "TEXT"),
        ("city", "TEXT"),
        ("state_code", "TEXT"),
        ("postal_code", "TEXT"),
        ("price", "INTEGER"),
        ("beds", "INTEGER"),
        ("baths", "REAL"),
        ("sqft", "INTEGER"),
        ("brokerage_name", "TEXT"),
        ("list_date", "TEXT"),
        ("first_seen_at", "TIMESTAMP"),
        ("last_seen_at", "TIMESTAMP"),
        ("times_seen", "INTEGER DEFAULT 1"),
    ]

    for col_name, col_type in new_columns:
        try:
            cursor.execute(f"ALTER TABLE listings ADD COLUMN {col_name} {col_type}")
            logger.info(f"Added column {col_name} to listings table")
        except sqlite3.OperationalError:
            # Column already exists
            pass

    conn.commit()
    conn.close()
    logger.info("Database initialized successfully")


def get_cached_agent(agent_url: str) -> Optional[dict]:
    """Retrieve agent info from cache if exists"""
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    cursor.execute(
        "SELECT agent_name, agent_phone FROM agents WHERE agent_url = ?", (agent_url,)
    )
    result = cursor.fetchone()
    conn.close()

    if result:
        logger.debug(f"Cache HIT for agent: {agent_url}")
        return {"name": result[0], "phone": result[1]}
    return None


def cache_agent(agent_url: str, name: Optional[str], phone: Optional[str]):
    """Cache agent information"""
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    cursor.execute(
        "INSERT OR REPLACE INTO agents (agent_url, agent_name, agent_phone) VALUES (?, ?, ?)",
        (agent_url, name or "", phone or ""),
    )
    conn.commit()
    conn.close()
    logger.info(f"Cached agent: {name} ({phone})")


def normalize_phone(phone: Optional[str]) -> str:
    """Normalize phone number to consistent format (digits only, last 10)

    Args:
        phone: Phone number in any format

    Returns:
        Normalized phone number (10 digits) or empty string
    """
    if not phone:
        return ""
    # Remove all non-digits
    digits = re.sub(r"\D", "", str(phone))
    # Return last 10 digits (removes country code if present)
    return digits[-10:] if len(digits) >= 10 else digits


def deduplicate_listings(listings: list[dict]) -> list[dict]:
    """Remove duplicate listings based on property_id

    Args:
        listings: List of listing dicts from API

    Returns:
        Deduplicated list of listings
    """
    seen_ids = set()
    unique_listings = []

    for listing in listings:
        pid = listing.get("property_id")
        if pid and pid not in seen_ids:
            seen_ids.add(pid)
            unique_listings.append(listing)
        elif not pid:
            # Keep listings without property_id (shouldn't happen, but be safe)
            unique_listings.append(listing)

    removed_count = len(listings) - len(unique_listings)
    if removed_count > 0:
        logger.info(f"Deduplicated: removed {removed_count} duplicate listings")

    return unique_listings


def save_listing(listing_data: dict) -> bool:
    """Save listing results to database

    Returns:
        True if this is a new listing, False if it was seen before
    """
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()

    listing_url = listing_data.get("url")

    # Check if listing already exists
    cursor.execute(
        "SELECT times_seen FROM listings WHERE listing_url = ?", (listing_url,)
    )
    existing = cursor.fetchone()

    if existing:
        # Update existing listing - increment times_seen and update last_seen_at
        times_seen = existing[0] + 1 if existing[0] else 2
        cursor.execute(
            """
            UPDATE listings SET
                last_seen_at = CURRENT_TIMESTAMP,
                times_seen = ?,
                price = ?,
                agent_name = ?,
                agent_phone = ?,
                brokerage_name = ?
            WHERE listing_url = ?
            """,
            (
                times_seen,
                listing_data.get("price"),
                listing_data.get("agent_name"),
                listing_data.get("agent_phone"),
                listing_data.get("brokerage_name"),
                listing_url,
            ),
        )
        is_new = False
        logger.debug(f"Updated existing listing (seen {times_seen}x): {listing_url}")
    else:
        # Insert new listing
        cursor.execute(
            """
            INSERT INTO listings
            (listing_url, property_id, address, city, state_code, postal_code, price, beds, baths, sqft,
             list_date, has_septic_system, has_private_well, septic_mentions, well_mentions,
             agent_url, agent_name, agent_phone, brokerage_name,
             first_seen_at, last_seen_at, times_seen)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, 1)
        """,
            (
                listing_url,
                listing_data.get("property_id"),
                listing_data.get("address"),
                listing_data.get("city"),
                listing_data.get("state_code"),
                listing_data.get("postal_code"),
                listing_data.get("price"),
                listing_data.get("beds"),
                listing_data.get("baths"),
                listing_data.get("sqft"),
                listing_data.get("list_date"),
                listing_data.get("has_septic"),
                listing_data.get("has_well"),
                json.dumps(listing_data.get("septic_mentions", [])),
                json.dumps(listing_data.get("well_mentions", [])),
                listing_data.get("agent_url"),
                listing_data.get("agent_name"),
                listing_data.get("agent_phone"),
                listing_data.get("brokerage_name"),
            ),
        )
        is_new = True
        logger.debug(f"Inserted new listing: {listing_url}")

    conn.commit()
    conn.close()
    return is_new


class RealtorScraperCurl:
    # Target Wisconsin counties for agent search
    TARGET_COUNTIES = ["Kenosha", "Milwaukee", "Racine", "Walworth", "Waukesha"]

    # Common headers for all requests
    COMMON_HEADERS = {
        "Accept": "*/*",
        "Accept-Language": "en-US,en;q=0.9",
        "Content-Type": "application/json",
        "Origin": "https://www.realtor.com",
        "Referer": "https://www.realtor.com/realestateandhomes-search/Wisconsin",
        "Sec-Ch-Ua": '"Chromium";v="136", "Google Chrome";v="136", "Not-A.Brand";v="99"',
        "Sec-Ch-Ua-Mobile": "?0",
        "Sec-Ch-Ua-Platform": '"macOS"',
        "Sec-Fetch-Dest": "empty",
        "Sec-Fetch-Mode": "cors",
        "Sec-Fetch-Site": "same-origin",
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/136.0.0.0 Safari/537.36",
        "rdc-client-name": "RDC_WEB_SRP_FS_PAGE",
        "rdc-client-version": "3.0.2449",
        "x-is-bot": "false",
    }

    def __init__(self):
        self.base_url = "https://www.realtor.com"
        self.graphql_url = "https://www.realtor.com/frontdoor/graphql"
        self.session = requests.Session(impersonate="chrome120")
        self.async_session: Optional[AsyncSession] = None

        # Set headers matching your working curl
        self.session.headers.update(self.COMMON_HEADERS)

    async def get_async_session(self) -> AsyncSession:
        """Get or create async curl_cffi session"""
        if self.async_session is None:
            self.async_session = AsyncSession(impersonate="chrome120")
            self.async_session.headers.update(self.COMMON_HEADERS)
        return self.async_session

    async def close_async_session(self):
        """Close async session"""
        if self.async_session:
            await self.async_session.close()
            self.async_session = None

    def search_agent_location(self, location: str) -> Optional[dict]:
        """Search for agent location using GraphQL API

        Args:
            location: Location string like "kenosha, wi"

        Returns:
            Location data with geo_id, city, state, etc.
        """
        query = {
            "operationName": "AgentLocationSearch",
            "variables": {
                "locationSearchInput": {
                    "input": location,
                    "client_id": "agent-branding-search",
                    "limit": 1,
                    "area_types": "city,postal_code",
                }
            },
            "query": """query AgentLocationSearch($locationSearchInput: AgentLocationSearchInput) {
  agents_location_search(location_search_input: $locationSearchInput) {
    auto_complete {
      id
      score
      area_type
      city
      city_slug_id
      counties {
        name
        fips
        state_code
        __typename
      }
      country
      county
      geo_id
      neighborhood
      park
      park_id
      postal_code
      school
      school_district
      school_district_id
      school_id
      slug_id
      state
      state_code
      university
      university_id
      centroid {
        lat
        lon
        __typename
      }
      line
      county_needed_for_uniq
      has_catchment
      __typename
    }
    __typename
  }
}""",
        }

        logger.info(f"Searching agent location for: {location}")

        try:
            # Update headers for agent branding profile
            original_headers = {
                "rdc-client-name": self.session.headers.get("rdc-client-name"),
                "rdc-client-version": self.session.headers.get("rdc-client-version"),
            }

            self.session.headers.update(
                {
                    "rdc-client-name": "agent-branding-profile",
                    "rdc-client-version": "0.0.695",
                }
            )

            response = self.session.post(self.graphql_url, json=query, timeout=30)

            # Restore original headers
            self.session.headers.update(original_headers)

            if response.status_code != 200:
                logger.error(
                    f"Agent location search API returned {response.status_code}"
                )
                return None

            data = response.json()

            if "errors" in data:
                logger.error(f"GraphQL errors: {data['errors']}")
                return None

            auto_complete = (
                data.get("data", {})
                .get("agents_location_search", {})
                .get("auto_complete", [])
            )

            if auto_complete:
                result = auto_complete[0]
                logger.info(
                    f"Found location: {result.get('city')}, {result.get('state_code')} (geo_id: {result.get('geo_id')})"
                )
                return result

            return None

        except Exception as e:
            logger.error(f"Agent location search failed: {e}")
            return None

    def get_target_county_locations(self) -> list[dict]:
        """Get location data for all target Wisconsin counties

        Returns:
            List of location data dicts for each target county
        """
        locations = []

        for county in self.TARGET_COUNTIES:
            location_str = f"{county}, WI"
            location_data = self.search_agent_location(location_str)

            if location_data:
                locations.append(location_data)
                logger.info(f"Added target location: {county}, WI")
            else:
                logger.warning(f"Could not find location data for: {county}, WI")

            # Rate limiting
            time.sleep(0.3)

        logger.info(f"Found {len(locations)} target county locations")
        return locations

    def is_in_target_county(
        self, city: Optional[str], state_code: Optional[str]
    ) -> bool:
        """Check if a location is in one of the target Wisconsin counties

        Args:
            city: City name
            state_code: State code (should be WI)

        Returns:
            True if the city is in a target county
        """
        if state_code != "WI":
            return False

        # Map of cities to their counties (common cities in target counties)
        # This is a simplified check - for production, use the location API
        target_cities = {
            # Kenosha County
            "Kenosha",
            "Pleasant Prairie",
            "Salem Lakes",
            "Paddock Lake",
            "Twin Lakes",
            "Bristol",
            "Paris",
            "Somers",
            "Brighton",
            "Wheatland",
            "Randall",
            "Silver Lake",
            # Milwaukee County
            "Milwaukee",
            "Wauwatosa",
            "West Allis",
            "Greenfield",
            "Oak Creek",
            "Franklin",
            "South Milwaukee",
            "Cudahy",
            "St. Francis",
            "Shorewood",
            "Whitefish Bay",
            "Fox Point",
            "Glendale",
            "Brown Deer",
            "River Hills",
            "Bayside",
            "Hales Corners",
            "Greendale",
            "West Milwaukee",
            # Racine County
            "Racine",
            "Mount Pleasant",
            "Caledonia",
            "Burlington",
            "Sturtevant",
            "Union Grove",
            "Waterford",
            "Wind Point",
            "North Bay",
            "Elmwood Park",
            "Rochester",
            "Norway",
            "Raymond",
            "Yorkville",
            "Dover",
            # Walworth County
            "Lake Geneva",
            "Whitewater",
            "Delavan",
            "Elkhorn",
            "East Troy",
            "Fontana",
            "Williams Bay",
            "Genoa City",
            "Walworth",
            "Sharon",
            "Darien",
            "Bloomfield",
            "La Grange",
            "Linn",
            "Lyons",
            "Spring Prairie",
            "Sugar Creek",
            "Troy",
            # Waukesha County
            "Waukesha",
            "Brookfield",
            "New Berlin",
            "Menomonee Falls",
            "Muskego",
            "Pewaukee",
            "Oconomowoc",
            "Hartland",
            "Sussex",
            "Mukwonago",
            "Delafield",
            "Wales",
            "Chenequa",
            "Nashotah",
            "Lac La Belle",
            "North Prairie",
            "Big Bend",
            "Eagle",
            "Dousman",
            "Genesee",
            "Ottawa",
            "Vernon",
            "Lisbon",
            "Merton",
            "Summit",
        }

        return city in target_cities if city else False

    def search_listings_api(
        self,
        state_code: str = "WI",
        limit: int = 1000,
        offset: int = 0,
        days_old: Optional[int] = None,
        location: Optional[str] = None,
        county: Optional[str] = None,
    ) -> list[dict]:
        """Use Realtor.com's GraphQL API to get listings

        Args:
            state_code: State to search (default: WI)
            limit: Number of listings to fetch
            offset: Pagination offset
            days_old: Only get listings from the past N days (None = no filter)
            location: Location string to filter by (e.g., "Kenosha, WI")
            county: County name to filter by (e.g., "Kenosha")
        """

        # Build query parameters
        query_params = {
            "primary": True,
            "status": ["for_sale", "ready_to_build"],
        }

        # Add county filter if specified (use "County Name County, ST" format)
        if county:
            county_location = f"{county} County, {state_code}"
            query_params["search_location"] = {"location": county_location}
            logger.info(f"Filtering by county: {county_location}")
        # Add location filter if specified (e.g., "Kenosha, WI")
        elif location:
            query_params["search_location"] = {"location": location}
            logger.info(f"Filtering by location: {location}")
        else:
            # Fall back to state-wide search
            query_params["state_code"] = state_code

        # Add date filter if specified
        if days_old is not None:
            min_date = (datetime.now() - timedelta(days=days_old)).strftime("%Y-%m-%d")
            query_params["list_date"] = {"min": min_date}
            logger.info(f"Filtering listings from {min_date} onwards ({days_old} days)")

        query = {
            "operationName": "ConsumerSearchQuery",
            "variables": {
                "query": query_params,
                "limit": limit,
                "offset": offset,
                "sort": [{"field": "list_date", "direction": "desc"}],
            },
            "query": """
                query ConsumerSearchQuery(
                    $query: HomeSearchCriteria!
                    $limit: Int
                    $offset: Int
                    $sort: [SearchAPISort]
                ) {
                    home_search(
                        query: $query
                        limit: $limit
                        offset: $offset
                        sort: $sort
                    ) {
                        total
                        results {
                            property_id
                            listing_id
                            permalink
                            list_price
                            list_date
                            location {
                                address {
                                    line
                                    city
                                    state_code
                                    postal_code
                                }
                            }
                            description {
                                text
                                sqft
                                beds
                                baths
                            }
                            advertisers {
                                name
                                href
                                phones {
                                    number
                                    type
                                    primary
                                }
                            }
                        }
                    }
                }
            """,
        }

        logger.info(
            f"Querying GraphQL API for {state_code} listings (offset={offset})..."
        )

        try:
            response = self.session.post(self.graphql_url, json=query, timeout=30)

            logger.debug(f"API response status: {response.status_code}")

            if response.status_code != 200:
                logger.error(f"API returned {response.status_code}")
                logger.debug(response.text[:1000])
                return []

            data = response.json()

            # Save response for debugging
            with open("api_response.json", "w") as f:
                json.dump(data, f, indent=2)

            # Check for errors
            if "errors" in data:
                logger.error(f"GraphQL errors: {data['errors']}")
                return []

            results = data.get("data", {}).get("home_search", {}).get("results", [])
            total = data.get("data", {}).get("home_search", {}).get("total", 0)

            logger.info(
                f"API returned {len(results)} listings (total available: {total})"
            )
            return results

        except Exception as e:
            logger.error(f"API request failed: {e}")
            logger.exception("Full traceback:")
            return []

    def get_property_details(self, property_id: str) -> Optional[dict]:
        """Fetch full property details using GraphQL API"""

        query = {
            "operationName": "FullPropertyDetails",
            "variables": {"propertyId": property_id},
            "query": """query FullPropertyDetails($propertyId: ID!, $listingId: ID) {
  home(property_id: $propertyId, listing_id: $listingId) {
    property_id
    listing_id
    permalink
    list_price
    status
    description {
      text
      sqft
      beds
      baths
      baths_full
      baths_half
      lot_sqft
      type
      sub_type
      year_built
    }
    details {
      category
      parent_category
      text
    }
    location {
      address {
        line
        city
        state_code
        postal_code
      }
      county {
        name
      }
    }
    advertisers {
      name
      href
      type
      email
      phones {
        ext
        number
        primary
        trackable
        type
      }
      broker {
        name
        fulfillment_id
      }
      office {
        name
        phones {
          ext
          number
          primary
          trackable
          type
        }
      }
    }
    source {
      agents {
        agent_id
        agent_name
        agent_email
        agent_phone
        office_id
        office_name
        office_phone
      }
    }
  }
}""",
        }

        logger.info(f"Fetching property details for ID: {property_id}")

        try:
            # Update headers for details page
            self.session.headers.update(
                {
                    "rdc-client-name": "RDC_WEB_DETAILS_PAGE",
                    "rdc-client-version": "2.161.0",
                }
            )

            response = self.session.post(self.graphql_url, json=query, timeout=30)

            if response.status_code != 200:
                logger.error(f"Property details API returned {response.status_code}")
                logger.debug(f"Response: {response.text[:1000]}")
                return None

            data = response.json()

            if "errors" in data:
                logger.error(f"GraphQL errors: {data['errors']}")
                return None

            home_data = data.get("data", {}).get("home")

            # Reset headers back to search page
            self.session.headers.update(
                {
                    "rdc-client-name": "RDC_WEB_SRP_FS_PAGE",
                    "rdc-client-version": "3.0.2449",
                }
            )

            return home_data

        except Exception as e:
            logger.error(f"Property details request failed: {e}")
            return None

    async def get_property_details_async(self, property_id: str) -> Optional[dict]:
        """Fetch full property details using GraphQL API (async version)"""

        query = {
            "operationName": "FullPropertyDetails",
            "variables": {"propertyId": property_id},
            "query": """query FullPropertyDetails($propertyId: ID!, $listingId: ID) {
  home(property_id: $propertyId, listing_id: $listingId) {
    property_id
    listing_id
    permalink
    list_price
    status
    description {
      text
      sqft
      beds
      baths
      baths_full
      baths_half
      lot_sqft
      type
      sub_type
      year_built
    }
    details {
      category
      parent_category
      text
    }
    location {
      address {
        line
        city
        state_code
        postal_code
      }
      county {
        name
      }
    }
    advertisers {
      name
      href
      type
      email
      phones {
        ext
        number
        primary
        trackable
        type
      }
      broker {
        name
        fulfillment_id
      }
      office {
        name
        phones {
          ext
          number
          primary
          trackable
          type
        }
      }
    }
    source {
      agents {
        agent_id
        agent_name
        agent_email
        agent_phone
        office_id
        office_name
        office_phone
      }
    }
  }
}""",
        }

        try:
            session = await self.get_async_session()

            # Update headers for details page
            headers = {
                "rdc-client-name": "RDC_WEB_DETAILS_PAGE",
                "rdc-client-version": "2.161.0",
            }

            response = await session.post(
                self.graphql_url, json=query, headers=headers, timeout=30
            )

            if response.status_code != 200:
                logger.error(f"Property details API returned {response.status_code}")
                return None

            data = response.json()

            if "errors" in data:
                logger.error(f"GraphQL errors: {data['errors']}")
                return None

            return data.get("data", {}).get("home")

        except Exception as e:
            logger.error(f"Async property details request failed: {e}")
            return None

    def check_property_for_septic_well(self, property_data: dict) -> dict:
        """Check property details for septic system and private well mentions"""
        result = {
            "has_septic": False,
            "has_well": False,
            "septic_mentions": [],
            "well_mentions": [],
        }

        if not property_data:
            return result

        # Check details array for utilities info
        details = property_data.get("details", [])
        for detail in details:
            if not isinstance(detail, dict):
                continue

            category = (detail.get("category") or "").lower()
            text_list = detail.get("text", [])

            if isinstance(text_list, str):
                text_list = [text_list]

            for text in text_list:
                text_lower = text.lower()

                # Check for septic (case-insensitive with word boundaries)
                septic_detail_patterns = [
                    r"\bseptic\b",
                    r"\bsewer:\s*septic\b",
                ]
                for pattern in septic_detail_patterns:
                    if re.search(pattern, text_lower):
                        result["has_septic"] = True
                        result["septic_mentions"].append(f"{category}: {text}")
                        break

                # Check for well (case-insensitive with word boundaries)
                # Use word boundaries to avoid matching "Howell", "Maxwell", etc.
                well_detail_patterns = [
                    r"\bprivate\s+well\b",
                    r"\bwater:\s*well\b",
                    r"\bwell\s+water\b",
                    r"\bdrilled\s+well\b",
                ]
                for pattern in well_detail_patterns:
                    if re.search(pattern, text_lower):
                        result["has_well"] = True
                        result["well_mentions"].append(f"{category}: {text}")
                        break

        # Also check description text
        description = property_data.get("description", {})
        desc_text = description.get("text", "") if isinstance(description, dict) else ""

        if desc_text:
            desc_lower = desc_text.lower()

            # Septic patterns (with word boundaries)
            septic_patterns = [
                r"\bseptic\s*system\b",
                r"\bseptic\s*tank\b",
                r"\bprivate\s+septic\b",
            ]
            for pattern in septic_patterns:
                if re.search(pattern, desc_lower):
                    result["has_septic"] = True
                    result["septic_mentions"].append(f"description: {pattern}")

            # Well patterns (with word boundaries to avoid "Howell", "Maxwell", etc.)
            well_patterns = [
                r"\bprivate\s+well\b",
                r"\bwell\s+water\b",
                r"\bwater\s+well\b",
                r"\bdrilled\s+well\b",
            ]
            for pattern in well_patterns:
                if re.search(pattern, desc_lower):
                    result["has_well"] = True
                    result["well_mentions"].append(f"description: {pattern}")

        return result

    def fetch_page(self, url: str) -> Optional[BeautifulSoup]:
        """Fetch a page and return parsed HTML"""
        logger.info(f"Fetching: {url}")

        try:
            response = self.session.get(url, timeout=30)

            # Check for block
            if "Your request could not be processed" in response.text:
                logger.error("Block page detected!")
                # Save for debugging
                with open("debug_curl_block.html", "w") as f:
                    f.write(response.text)
                return None

            if response.status_code != 200:
                logger.error(f"HTTP {response.status_code}")
                return None

            logger.debug(f"Got {len(response.text)} bytes")
            return BeautifulSoup(response.text, "html.parser")

        except Exception as e:
            logger.error(f"Request failed: {e}")
            return None

    def get_listing_urls(self, search_url: str) -> list[str]:
        """Extract all listing URLs from search results page"""
        soup = self.fetch_page(search_url)
        if not soup:
            return []

        listing_urls = []

        # Find listing links
        for link in soup.find_all("a", href=True):
            href = link["href"]
            if "/realestateandhomes-detail/" in href:
                full_url = href if href.startswith("http") else f"{self.base_url}{href}"
                if full_url not in listing_urls:
                    listing_urls.append(full_url)

        logger.info(f"Found {len(listing_urls)} listing URLs")
        return listing_urls

    def check_septic_and_well(self, listing_url: str) -> dict:
        """Check listing page for septic system and private well mentions"""
        result = {
            "url": listing_url,
            "address": None,
            "has_septic": False,
            "has_well": False,
            "septic_mentions": [],
            "well_mentions": [],
            "agent_url": None,
            "agent_name": None,
            "agent_phone": None,
        }

        soup = self.fetch_page(listing_url)
        if not soup:
            return result

        # Get page text
        page_text = soup.get_text()

        # Get address
        address_el = soup.find(attrs={"data-testid": "address"})
        if not address_el:
            # Try h1
            h1 = soup.find("h1")
            if h1:
                address_el = h1
        if address_el:
            result["address"] = address_el.get_text(strip=True)

        # Search for septic
        septic_patterns = [
            r"septic\s*system",
            r"septic\s*tank",
            r"private\s*septic",
            r"sewer[:\s]+septic",
        ]
        for pattern in septic_patterns:
            matches = re.findall(pattern, page_text, re.IGNORECASE)
            if matches:
                result["has_septic"] = True
                result["septic_mentions"].extend(matches)

        # Search for well
        well_patterns = [
            r"private\s*well",
            r"well\s*water",
            r"water[:\s]+well",
            r"water\s*source[:\s]+well",
        ]
        for pattern in well_patterns:
            matches = re.findall(pattern, page_text, re.IGNORECASE)
            if matches:
                result["has_well"] = True
                result["well_mentions"].extend(matches)

        # Find agent link
        for link in soup.find_all("a", href=True):
            href = link["href"]
            if "/realestateagents/" in href or "/realestateteam/" in href:
                result["agent_url"] = (
                    href if href.startswith("http") else f"{self.base_url}{href}"
                )
                break

        # Get agent info
        if result["agent_url"]:
            cached = get_cached_agent(result["agent_url"])
            if cached:
                result["agent_name"] = cached["name"]
                result["agent_phone"] = cached["phone"]
            else:
                agent_info = self.fetch_agent_info(result["agent_url"])
                result["agent_name"] = agent_info.get("name")
                result["agent_phone"] = agent_info.get("phone")
                if result["agent_name"] or result["agent_phone"]:
                    cache_agent(
                        result["agent_url"], result["agent_name"], result["agent_phone"]
                    )

        if result["has_septic"] or result["has_well"]:
            logger.info(
                f"FOUND - Septic: {result['has_septic']}, Well: {result['has_well']} - {result['address']}"
            )

        return result

    def fetch_agent_info(self, agent_url: str) -> dict:
        """Fetch agent name and phone from their profile page"""
        agent_info = {"name": None, "phone": None}

        soup = self.fetch_page(agent_url)
        if not soup:
            return agent_info

        # Get name
        name_el = soup.find(attrs={"data-testid": "agent-name"})
        if not name_el:
            name_el = soup.find("h1")
        if name_el:
            agent_info["name"] = name_el.get_text(strip=True)

        # Get phone
        phone_el = soup.find("a", href=lambda x: x and x.startswith("tel:"))
        if phone_el:
            agent_info["phone"] = phone_el["href"].replace("tel:", "")

        logger.info(f"Agent: {agent_info['name']} - {agent_info['phone']}")
        return agent_info

    def process_api_listing(self, listing: dict) -> dict:
        """Process a listing from API response - basic info only"""
        result = {
            "url": None,
            "property_id": None,
            "address": None,
            "city": None,
            "state_code": None,
            "postal_code": None,
            "price": None,
            "beds": None,
            "baths": None,
            "sqft": None,
            "list_date": None,
            "has_septic": False,
            "has_well": False,
            "septic_mentions": [],
            "well_mentions": [],
            "agent_url": None,
            "agent_name": None,
            "agent_phone": None,
            "brokerage_name": None,
        }

        # Get property ID for detailed lookup
        result["property_id"] = listing.get("property_id")

        # Build URL from permalink
        permalink = listing.get("permalink", "")
        if permalink:
            result["url"] = f"{self.base_url}/realestateandhomes-detail/{permalink}"

        # Get address info
        location = listing.get("location", {})
        address = location.get("address", {})
        if address:
            result["city"] = address.get("city")
            result["state_code"] = address.get("state_code")
            result["postal_code"] = address.get("postal_code")
            result["address"] = address.get("line", "")

        # Get price, list date, and property details
        result["price"] = listing.get("list_price")
        result["list_date"] = listing.get("list_date")
        description = listing.get("description", {})
        if isinstance(description, dict):
            result["beds"] = description.get("beds")
            result["baths"] = description.get("baths")
            result["sqft"] = description.get("sqft")

        # Get agent info from basic listing
        advertisers = listing.get("advertisers", [])
        if advertisers:
            agent = advertisers[0]
            result["agent_name"] = agent.get("name")
            result["agent_url"] = agent.get("href")

            # Extract phone from phones array
            phones = agent.get("phones", [])
            if phones:
                # Try to find primary phone first
                for phone in phones:
                    if phone.get("primary"):
                        result["agent_phone"] = phone.get("number")
                        break
                # If no primary, use first phone
                if not result["agent_phone"] and phones:
                    result["agent_phone"] = phones[0].get("number")

        # Normalize phone number
        result["agent_phone"] = normalize_phone(result.get("agent_phone"))

        return result

    def process_property_details(self, property_data: dict, basic_result: dict) -> dict:
        """Process full property details and update result with septic/well info"""
        if not property_data:
            return basic_result

        result = basic_result.copy()

        # Check for septic/well in details
        septic_well_info = self.check_property_for_septic_well(property_data)
        result["has_septic"] = septic_well_info["has_septic"]
        result["has_well"] = septic_well_info["has_well"]
        result["septic_mentions"] = septic_well_info["septic_mentions"]
        result["well_mentions"] = septic_well_info["well_mentions"]

        # Get agent and brokerage info from detailed data
        advertisers = property_data.get("advertisers", [])
        if advertisers:
            agent = advertisers[0]
            result["agent_name"] = agent.get("name") or result["agent_name"]

            # Get phone - try direct phone first, then office phones
            phones = agent.get("phones", [])
            if phones:
                for phone in phones:
                    if phone.get("primary") or phone.get("type") == "mobile":
                        result["agent_phone"] = phone.get("number")
                        break
                if not result["agent_phone"] and phones:
                    result["agent_phone"] = phones[0].get("number")

            if not result["agent_phone"]:
                result["agent_phone"] = agent.get("phone")

            result["agent_url"] = agent.get("href") or result["agent_url"]

            # Get broker/brokerage name
            broker = agent.get("broker", {})
            if broker:
                result["brokerage_name"] = broker.get("name")

            # Also check office for brokerage
            office = agent.get("office", {})
            if office and not result["brokerage_name"]:
                result["brokerage_name"] = office.get("name")

        # Also check source agents
        source = property_data.get("source", {})
        agents = source.get("agents", [])
        if agents and not result["agent_name"]:
            agent = agents[0]
            result["agent_name"] = agent.get("agent_name")
            result["agent_phone"] = agent.get("agent_phone") or result["agent_phone"]
            if not result["brokerage_name"]:
                result["brokerage_name"] = agent.get("office_name")

        # Normalize phone number
        result["agent_phone"] = normalize_phone(result.get("agent_phone"))

        return result

    async def scrape_async(
        self,
        state_code: str = "WI",
        limit: int = 200,
        fetch_details: bool = True,
        days_old: Optional[int] = None,
        filter_target_counties: bool = False,
        max_concurrent: int = 10,
    ):
        """Async scraping function using asyncio for concurrent processing

        Args:
            state_code: State to search (default: WI for Wisconsin)
            limit: Number of listings to fetch
            fetch_details: Whether to fetch full property details for septic/well check
            days_old: Only get listings from the past N days (None = no filter)
            filter_target_counties: If True, only process listings in target counties
            max_concurrent: Maximum concurrent requests (default: 10)
        """
        logger.info("=" * 60)
        logger.info(f"Starting Async Realtor Scraper for {state_code}")
        if days_old:
            logger.info(f"Filtering to listings from past {days_old} day(s)")
        if filter_target_counties:
            logger.info(
                f"Filtering to target counties: {', '.join(self.TARGET_COUNTIES)}"
            )
        logger.info(f"Max concurrent requests: {max_concurrent}")
        logger.info("=" * 60)

        init_database()

        results = []
        properties_with_septic_well = []
        new_listings_count = 0
        updated_listings_count = 0

        # Get listings via API (sync for now, as it's a single request per county)
        api_listings = []

        if filter_target_counties:
            for county_name in self.TARGET_COUNTIES:
                logger.info(f"Searching county: {county_name}, {state_code}")
                county_listings = self.search_listings_api(
                    state_code=state_code,
                    limit=limit,
                    days_old=days_old,
                    county=county_name,
                )
                api_listings.extend(county_listings)
                await asyncio.sleep(0.5)  # Rate limiting

            logger.info(f"Total listings from all target counties: {len(api_listings)}")

            # Deduplicate listings that may appear in multiple county searches
            api_listings = deduplicate_listings(api_listings)
            logger.info(f"After deduplication: {len(api_listings)} unique listings")
        else:
            api_listings = self.search_listings_api(
                state_code=state_code, limit=limit, days_old=days_old
            )

        if not api_listings:
            logger.error("No listings from API!")
            return results

        # Create asyncio queue for database writes
        db_queue: asyncio.Queue = asyncio.Queue()

        # Track progress
        processed_count = 0
        total_count = len(api_listings)

        async def db_writer():
            """Task that handles all database writes from the queue"""
            nonlocal new_listings_count, updated_listings_count
            while True:
                result = await db_queue.get()
                if result is None:  # Sentinel to stop
                    break
                is_new = save_listing(result)
                if is_new:
                    new_listings_count += 1
                else:
                    updated_listings_count += 1
                db_queue.task_done()

        async def process_listing(listing, semaphore):
            """Process a single listing with rate limiting"""
            nonlocal processed_count
            async with semaphore:
                # Get basic listing info
                result = self.process_api_listing(listing)

                # Fetch full property details to check for septic/well
                if fetch_details and result.get("property_id"):
                    property_data = await self.get_property_details_async(
                        result["property_id"]
                    )
                    if property_data:
                        result = self.process_property_details(property_data, result)

                processed_count += 1
                if processed_count % 10 == 0 or processed_count == total_count:
                    logger.info(f"Processed {processed_count}/{total_count}")

                results.append(result)

                if result["has_septic"] or result["has_well"]:
                    properties_with_septic_well.append(result)
                    logger.info(
                        f"FOUND - Septic: {result['has_septic']}, Well: {result['has_well']} - "
                        f"{result['address']}, {result['city']}"
                    )

                # Send to database writer queue
                await db_queue.put(result)

        # Run concurrent processing
        semaphore = asyncio.Semaphore(max_concurrent)

        # Start database writer task
        db_writer_task = asyncio.create_task(db_writer())

        # Process all listings concurrently
        tasks = [process_listing(listing, semaphore) for listing in api_listings]
        await asyncio.gather(*tasks)

        # Signal db_writer to stop and wait for it
        await db_queue.put(None)
        await db_writer_task

        # Close async session
        await self.close_async_session()

        # Summary
        logger.info("=" * 60)
        logger.info(f"Total processed: {len(results)}")
        logger.info(f"New listings: {new_listings_count}")
        logger.info(f"Updated (seen before): {updated_listings_count}")
        logger.info(f"With Septic/Well: {len(properties_with_septic_well)}")
        logger.info("=" * 60)

        # Print results with septic/well
        if properties_with_septic_well:
            print("\n" + "=" * 60)
            print("PROPERTIES WITH SEPTIC SYSTEM OR PRIVATE WELL")
            print("=" * 60)

            for prop in properties_with_septic_well:
                print(
                    f"\n{prop['address']}, {prop['city']}, {prop['state_code']} {prop['postal_code']}"
                )
                print(
                    f"  Price: ${prop['price']:,}" if prop["price"] else "  Price: N/A"
                )
                print(
                    f"  Beds: {prop['beds']} | Baths: {prop['baths']} | Sqft: {prop['sqft']}"
                )
                print(f"  URL: {prop['url']}")
                print(f"  Septic: {prop['has_septic']} - {prop['septic_mentions']}")
                print(f"  Well: {prop['has_well']} - {prop['well_mentions']}")
                print(f"  Agent: {prop['agent_name']} | Phone: {prop['agent_phone']}")
                print(f"  Brokerage: {prop['brokerage_name']}")
        else:
            print("\nNo properties found with septic system or private well.")

        return results

    def scrape(
        self,
        state_code: str = "WI",
        limit: int = 200,
        fetch_details: bool = True,
        days_old: Optional[int] = None,
        filter_target_counties: bool = False,
    ):
        """Main scraping function using API

        Args:
            state_code: State to search (default: WI for Wisconsin)
            limit: Number of listings to fetch
            fetch_details: Whether to fetch full property details for septic/well check
            days_old: Only get listings from the past N days (None = no filter, 1 = past 24 hours)
            filter_target_counties: If True, only process listings in target counties
                                   (Kenosha, Milwaukee, Racine, Walworth, Waukesha)
        """
        logger.info("=" * 60)
        logger.info(f"Starting Realtor Scraper for {state_code}")
        if days_old:
            logger.info(f"Filtering to listings from past {days_old} day(s)")
        if filter_target_counties:
            logger.info(
                f"Filtering to target counties: {', '.join(self.TARGET_COUNTIES)}"
            )
        logger.info("=" * 60)

        init_database()

        results = []
        properties_with_septic_well = []
        new_listings_count = 0
        updated_listings_count = 0
        skipped_count = 0

        # Get listings via API
        api_listings = []

        if filter_target_counties:
            # Search each target county separately
            for county_name in self.TARGET_COUNTIES:
                logger.info(f"Searching county: {county_name}, {state_code}")
                county_listings = self.search_listings_api(
                    state_code=state_code,
                    limit=limit,
                    days_old=days_old,
                    county=county_name,
                )
                api_listings.extend(county_listings)

                # Rate limiting between county searches
                time.sleep(0.5)

            logger.info(f"Total listings from all target counties: {len(api_listings)}")

            # Deduplicate listings that may appear in multiple county searches
            api_listings = deduplicate_listings(api_listings)
            logger.info(f"After deduplication: {len(api_listings)} unique listings")
        else:
            # Search entire state
            api_listings = self.search_listings_api(
                state_code=state_code, limit=limit, days_old=days_old
            )

        if not api_listings:
            logger.error("No listings from API!")
            return results

        # Process each listing
        for i, listing in enumerate(api_listings):
            logger.info(f"Processing {i + 1}/{len(api_listings)}")

            # Get basic listing info
            result = self.process_api_listing(listing)

            # Filter by target counties if enabled
            if filter_target_counties:
                if not self.is_in_target_county(
                    result.get("city"), result.get("state_code")
                ):
                    logger.debug(
                        f"Skipping {result.get('city')} - not in target counties"
                    )
                    skipped_count += 1
                    continue

            # Fetch full property details to check for septic/well
            if fetch_details and result.get("property_id"):
                time.sleep(0.5)  # Rate limiting

                property_data = self.get_property_details(result["property_id"])
                if property_data:
                    result = self.process_property_details(property_data, result)

            results.append(result)
            is_new = save_listing(result)

            if is_new:
                new_listings_count += 1
            else:
                updated_listings_count += 1

            if result["has_septic"] or result["has_well"]:
                properties_with_septic_well.append(result)
                new_marker = " [NEW]" if is_new else ""
                logger.info(
                    f"FOUND{new_marker} - Septic: {result['has_septic']}, Well: {result['has_well']} - "
                    f"{result['address']}, {result['city']}"
                )

        if skipped_count > 0:
            logger.info(f"Skipped {skipped_count} listings not in target counties")

        # Summary
        logger.info("=" * 60)
        logger.info(f"Total processed: {len(results)}")
        logger.info(f"New listings: {new_listings_count}")
        logger.info(f"Updated (seen before): {updated_listings_count}")
        logger.info(f"With Septic/Well: {len(properties_with_septic_well)}")
        logger.info("=" * 60)

        # Print results with septic/well
        if properties_with_septic_well:
            print("\n" + "=" * 60)
            print("PROPERTIES WITH SEPTIC SYSTEM OR PRIVATE WELL")
            print("=" * 60)

            for prop in properties_with_septic_well:
                print(
                    f"\n{prop['address']}, {prop['city']}, {prop['state_code']} {prop['postal_code']}"
                )
                print(
                    f"  Price: ${prop['price']:,}" if prop["price"] else "  Price: N/A"
                )
                print(
                    f"  Beds: {prop['beds']} | Baths: {prop['baths']} | Sqft: {prop['sqft']}"
                )
                print(f"  URL: {prop['url']}")
                print(f"  Septic: {prop['has_septic']} - {prop['septic_mentions']}")
                print(f"  Well: {prop['has_well']} - {prop['well_mentions']}")
                print(f"  Agent: {prop['agent_name']} | Phone: {prop['agent_phone']}")
                print(f"  Brokerage: {prop['brokerage_name']}")
        else:
            print("\nNo properties found with septic system or private well.")

        return results


async def main_async():
    """Async main function"""
    scraper = RealtorScraperCurl()
    # Default: Get listings from past 24 hours (1 day)
    # Set days_old=None to get all listings without date filter
    # Set filter_target_counties=True to only get listings from:
    # Kenosha, Milwaukee, Racine, Walworth, Waukesha counties
    # max_concurrent controls how many property details are fetched in parallel
    await scraper.scrape_async(
        days_old=1, filter_target_counties=True, max_concurrent=10
    )


def main():
    """Sync wrapper that runs the async main"""
    asyncio.run(main_async())


if __name__ == "__main__":
    main()
