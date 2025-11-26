#!/usr/bin/env python3
"""
Main entry point for Railway cron job.
Runs the scraper, saves to DB, sends email notification with septic/well listings.
"""

from __future__ import annotations

import asyncio
import sys
import logging
import traceback
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

# Import the existing scraper
from scraper_curl import RealtorScraperCurl

# Import new modules
import db
from models import Listing, ScraperStats
import email_notifier

# CST timezone
CST = ZoneInfo("America/Chicago")


def get_6am_cst_yesterday() -> datetime:
    """
    Get yesterday's 6am CST as a datetime object.
    This is the cutoff for filtering listings by list_date.

    Returns:
        datetime object for 6am CST yesterday
    """
    now = datetime.now(CST)
    # Yesterday at 6am CST
    yesterday_6am = (now - timedelta(days=1)).replace(
        hour=6, minute=0, second=0, microsecond=0
    )
    return yesterday_6am


def is_listing_after_cutoff(list_date_str: str | None, cutoff: datetime) -> bool:
    """
    Check if a listing's list_date is after the cutoff time.

    Args:
        list_date_str: List date string in format "YYYY-MM-DD"
        cutoff: Cutoff datetime

    Returns:
        True if listing is after cutoff, False otherwise
    """
    if not list_date_str:
        return False

    try:
        # Parse list_date (format: "2025-01-15")
        list_date = datetime.strptime(list_date_str, "%Y-%m-%d")
        # Assume listing is posted at midnight of that day in CST
        list_date = list_date.replace(tzinfo=CST)

        # Compare with cutoff (6am CST yesterday)
        return list_date >= cutoff.replace(hour=0, minute=0, second=0, microsecond=0)
    except ValueError:
        return False


# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
    ],
)
logger = logging.getLogger(__name__)


async def run_scraper(days_old: int = 1):
    """
    Main scraper function for Railway cron.

    1. Scrapes listings from Realtor.com
    2. Saves to PostgreSQL database
    3. Filters listings with list_date after 6am CST yesterday
    4. Sends email with Excel of septic/well listings

    Args:
        days_old: Number of days to look back for listings (default: 1)
    """
    stats = ScraperStats(started_at=datetime.utcnow())
    all_listings: list[Listing] = []
    septic_well_listings: list[Listing] = []

    # Get cutoff time: 6am CST yesterday
    cutoff_time = get_6am_cst_yesterday()
    logger.info(
        f"Filtering listings with list_date after: {cutoff_time.strftime('%Y-%m-%d %H:%M %Z')}"
    )

    try:
        logger.info("=" * 60)
        logger.info("Starting Realtor Scraper (Railway Cron)")
        logger.info(f"Looking back {days_old} day(s)")
        logger.info("=" * 60)

        # Initialize database
        db.init_database()

        # Create scraper instance
        scraper = RealtorScraperCurl()

        # Get listings from all target counties
        api_listings = []
        for county_name in scraper.TARGET_COUNTIES:
            logger.info(f"Searching county: {county_name}, WI")
            county_listings = scraper.search_listings_api(
                state_code="WI",
                limit=200,  # API max is 200
                days_old=days_old,
                county=county_name,
            )
            api_listings.extend(county_listings)
            await asyncio.sleep(0.5)  # Rate limiting

        logger.info(f"Total listings from all counties: {len(api_listings)}")

        # Deduplicate
        from scraper_curl import deduplicate_listings

        api_listings = deduplicate_listings(api_listings)
        logger.info(f"After deduplication: {len(api_listings)} unique listings")

        stats.total_processed = len(api_listings)

        # Process each listing
        for i, api_listing in enumerate(api_listings):
            try:
                # Get basic listing info
                result_dict = scraper.process_api_listing(api_listing)

                # Fetch property details to check for septic/well
                if result_dict.get("property_id"):
                    await asyncio.sleep(0.3)  # Rate limiting
                    property_data = await scraper.get_property_details_async(
                        result_dict["property_id"]
                    )
                    if property_data:
                        result_dict = scraper.process_property_details(
                            property_data, result_dict
                        )

                # Convert to Listing dataclass
                listing = Listing(
                    listing_url=result_dict.get("url", ""),
                    property_id=result_dict.get("property_id"),
                    address=result_dict.get("address"),
                    city=result_dict.get("city"),
                    county=result_dict.get("county"),
                    state_code=result_dict.get("state_code"),
                    postal_code=result_dict.get("postal_code"),
                    price=result_dict.get("price"),
                    beds=result_dict.get("beds"),
                    baths=result_dict.get("baths"),
                    sqft=result_dict.get("sqft"),
                    list_date=result_dict.get("list_date"),
                    has_septic_system=result_dict.get("has_septic", False),
                    has_private_well=result_dict.get("has_well", False),
                    septic_mentions=result_dict.get("septic_mentions", []),
                    well_mentions=result_dict.get("well_mentions", []),
                    agent_url=result_dict.get("agent_url"),
                    agent_name=result_dict.get("agent_name"),
                    agent_phone=result_dict.get("agent_phone"),
                    brokerage_name=result_dict.get("brokerage_name"),
                )

                # Save to database
                is_new, saved_listing = db.save_listing(listing)
                all_listings.append(saved_listing)

                if is_new:
                    stats.new_listings += 1
                else:
                    stats.updated_listings += 1

                # Check if listing has septic/well AND is after cutoff time
                list_date_str = result_dict.get("list_date")
                is_after_cutoff = is_listing_after_cutoff(list_date_str, cutoff_time)

                if is_after_cutoff and (
                    listing.has_septic_system or listing.has_private_well
                ):
                    septic_well_listings.append(saved_listing)
                    stats.septic_well_count += 1
                    logger.info(
                        f"SEPTIC/WELL MATCH: {listing.address}, {listing.city} "
                        f"(Septic: {listing.has_septic_system}, Well: {listing.has_private_well}) "
                        f"[list_date: {list_date_str}]"
                    )

                # Progress logging
                if (i + 1) % 50 == 0 or (i + 1) == len(api_listings):
                    logger.info(f"Processed {i + 1}/{len(api_listings)}")

            except Exception as e:
                logger.error(f"Error processing listing: {e}")
                stats.errors.append(str(e))

        # Close async session
        await scraper.close_async_session()

        stats.completed_at = datetime.utcnow()

        # Log summary
        logger.info("=" * 60)
        logger.info("SCRAPE COMPLETE")
        logger.info(f"Total processed: {stats.total_processed}")
        logger.info(f"New listings: {stats.new_listings}")
        logger.info(f"Updated: {stats.updated_listings}")
        logger.info(f"Septic/Well matches (after cutoff): {stats.septic_well_count}")
        logger.info(f"Duration: {stats.duration_seconds:.1f}s")
        logger.info("=" * 60)

        # Send email notification with Excel of septic/well listings
        await email_notifier.send_scrape_report(
            stats=stats,
            septic_well_listings=septic_well_listings,
        )

        logger.info("Email notification sent")

    except Exception as e:
        logger.error(f"Scraper failed: {e}")
        logger.error(traceback.format_exc())
        stats.errors.append(str(e))
        stats.completed_at = datetime.utcnow()

        # Send error notification
        await email_notifier.send_error_alert(
            f"Scraper failed:\n{str(e)}\n\n{traceback.format_exc()}"
        )

        sys.exit(1)

    logger.info("Scraper finished successfully")
    return stats


def main():
    """Sync entry point"""
    asyncio.run(run_scraper())


if __name__ == "__main__":
    main()
