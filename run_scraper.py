#!/usr/bin/env python3
"""
Main entry point for Railway cron job.
Runs the scraper, saves to DB, sends Telegram notification.
"""

from __future__ import annotations

import asyncio
import sys
import logging
import traceback
from datetime import datetime, timedelta, timezone

# Import the existing scraper
from scraper_curl import RealtorScraperCurl

# Import new modules
import db
from models import Listing, ScraperStats
import notifier


def get_24h_window() -> tuple[datetime, datetime]:
    """
    Get the 24-hour window for marking listings as "new".
    Window is from 2am yesterday to 2am today (in UTC).

    Returns:
        Tuple of (window_start, window_end) in UTC
    """
    now = datetime.now(timezone.utc)
    # Today at 2am UTC
    today_2am = now.replace(hour=2, minute=0, second=0, microsecond=0)

    # If current time is before 2am, the window is from yesterday's 2am to today's 2am
    # If current time is after 2am, the window is from today's 2am to tomorrow's 2am
    if now < today_2am:
        window_end = today_2am
        window_start = today_2am - timedelta(days=1)
    else:
        window_start = today_2am
        window_end = today_2am + timedelta(days=1)

    return window_start, window_end


# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
    ],
)
logger = logging.getLogger(__name__)


async def run_scraper(days_old: int = 1, mark_new_within_24h: bool = False):
    """
    Main scraper function for Railway cron.

    1. Scrapes listings from Realtor.com
    2. Saves to PostgreSQL database
    3. Sends Telegram notification with new septic/well listings
    4. Sends XLSX with all listings

    Args:
        days_old: Number of days to look back for listings (default: 1)
        mark_new_within_24h: If True, only mark listings as "new" if they fall
                            within the 2am-to-2am 24h window. Used for 7-day
                            history scrapes to avoid marking old listings as new.
    """
    stats = ScraperStats(started_at=datetime.utcnow())
    all_listings: list[Listing] = []
    new_septic_well_listings: list[Listing] = []

    # Get the 24h window if we need to filter what's marked as "new"
    window_start, window_end = None, None
    if mark_new_within_24h:
        window_start, window_end = get_24h_window()
        logger.info(f"24h window for 'new' listings: {window_start} to {window_end}")

    try:
        logger.info("=" * 60)
        logger.info("Starting Realtor Scraper (Railway Cron)")
        logger.info(f"Looking back {days_old} day(s)")
        if mark_new_within_24h:
            logger.info("Only marking listings within 24h window as new")
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

                    # Determine if this listing should be considered "new" for notifications
                    # When mark_new_within_24h is True, only count listings whose list_date
                    # falls within the 24h window (2am yesterday to 2am today)
                    should_notify = True
                    if mark_new_within_24h and window_start and window_end:
                        list_date_str = result_dict.get("list_date")
                        if list_date_str:
                            try:
                                # Parse list_date (format: "2025-01-15")
                                list_date = datetime.strptime(list_date_str, "%Y-%m-%d")
                                list_date = list_date.replace(tzinfo=timezone.utc)
                                # Only notify if listing date is within the 24h window
                                should_notify = window_start <= list_date < window_end
                                if not should_notify:
                                    logger.debug(
                                        f"Listing {listing.address} list_date {list_date_str} "
                                        f"outside 24h window, not marking as new for notification"
                                    )
                            except ValueError:
                                # If we can't parse the date, include it to be safe
                                should_notify = True

                    # Track new septic/well listings for notification
                    if should_notify and (
                        listing.has_septic_system or listing.has_private_well
                    ):
                        new_septic_well_listings.append(saved_listing)
                        stats.septic_well_count += 1
                        logger.info(
                            f"NEW SEPTIC/WELL: {listing.address}, {listing.city} "
                            f"(Septic: {listing.has_septic_system}, Well: {listing.has_private_well})"
                        )
                else:
                    stats.updated_listings += 1

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
        logger.info(f"New Septic/Well: {stats.septic_well_count}")
        logger.info(f"Duration: {stats.duration_seconds:.1f}s")
        logger.info("=" * 60)

        # Send Telegram notification
        await notifier.send_scrape_report(
            stats=stats,
            all_listings=all_listings,
            new_septic_well_listings=new_septic_well_listings,
        )

        logger.info("Telegram notification sent")

    except Exception as e:
        logger.error(f"Scraper failed: {e}")
        logger.error(traceback.format_exc())
        stats.errors.append(str(e))
        stats.completed_at = datetime.utcnow()

        # Send error notification
        await notifier.send_error_alert(
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
