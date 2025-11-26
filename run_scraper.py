#!/usr/bin/env python3
"""
Main entry point for Railway cron job.
Runs the scraper, saves to DB, sends email notification with septic/well listings.

Usage:
    python run_scraper.py          # Normal cron run
    python run_scraper.py --debug  # Debug run (loads .env, uses DEBUG_EMAIL_TO)
"""

from __future__ import annotations

import argparse
import asyncio
import os
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


def get_6am_window() -> tuple[datetime, datetime]:
    """
    Get the 6am CST window: yesterday 6am to today 6am.

    Returns:
        Tuple of (from_time, to_time) - yesterday 6am CST to today 6am CST
    """
    now = datetime.now(CST)
    today_6am = now.replace(hour=6, minute=0, second=0, microsecond=0)
    yesterday_6am = today_6am - timedelta(days=1)
    return yesterday_6am, today_6am


def is_listing_in_window(
    list_date_str: str | None, from_time: datetime, to_time: datetime
) -> bool:
    """
    Check if a listing's list_date falls within the 6am window.

    Args:
        list_date_str: List date string in format "YYYY-MM-DD"
        from_time: Start of window (yesterday 6am CST)
        to_time: End of window (today 6am CST)

    Returns:
        True if listing is within window, False otherwise
    """
    if not list_date_str:
        return False

    try:
        # Parse list_date (format: "2025-01-15")
        list_date = datetime.strptime(list_date_str, "%Y-%m-%d")
        # Assume listing is posted at midnight of that day in CST
        list_date = list_date.replace(tzinfo=CST)

        # Get date boundaries from the window
        from_date = from_time.replace(hour=0, minute=0, second=0, microsecond=0)
        to_date = to_time.replace(hour=0, minute=0, second=0, microsecond=0)

        # Include listings from yesterday's date and today's date
        return from_date <= list_date <= to_date
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


async def run_scraper(days_old: int = 1, debug_mode: bool = False):
    """
    Main scraper function for Railway cron.

    1. Scrapes listings from Realtor.com
    2. Saves to PostgreSQL database
    3. Filters listings with list_date in 6am-6am window (yesterday to today)
    4. Sends email with Excel of septic/well listings

    Args:
        days_old: Number of days to look back for listings (default: 1)
        debug_mode: If True, send to DEBUG_EMAIL_TO instead of EMAIL_TO
    """
    stats = ScraperStats(started_at=datetime.utcnow())
    all_listings: list[Listing] = []
    septic_well_listings: list[Listing] = []

    # Get 6am window: yesterday 6am CST to today 6am CST
    from_time, to_time = get_6am_window()
    logger.info(
        f"Filtering listings in window: {from_time.strftime('%Y-%m-%d %H:%M %Z')} to {to_time.strftime('%Y-%m-%d %H:%M %Z')}"
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

                # Check if listing has septic/well AND is in the 6am window
                list_date_str = result_dict.get("list_date")
                is_in_window = is_listing_in_window(list_date_str, from_time, to_time)

                if is_in_window and (
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
        logger.info(f"Septic/Well matches (in window): {stats.septic_well_count}")
        logger.info(f"Duration: {stats.duration_seconds:.1f}s")
        logger.info("=" * 60)

        # Send email notification with Excel of septic/well listings
        if debug_mode:
            # Debug mode: use send_debug_email which respects DEBUG_EMAIL_TO
            logger.info("Debug mode: sending to DEBUG_EMAIL_TO")
            email_notifier.send_debug_email(septic_well_listings, from_time, to_time)
        else:
            # Normal mode: send regular report
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
    """Sync entry point with CLI argument parsing"""
    # Always try to load .env file (for local development)
    # In production (Railway), env vars are set directly so this is a no-op
    from dotenv import load_dotenv

    if load_dotenv():
        logger.info("Loaded .env file")
        # Reload email_notifier config after .env is loaded
        email_notifier.RESEND_API_KEY = os.environ.get("RESEND_API_KEY", "")
        email_to_raw = os.environ.get("EMAIL_TO", "binfaisal.ammar@gmail.com")
        email_notifier.EMAIL_TO = [
            e.strip() for e in email_to_raw.split(",") if e.strip()
        ]
        email_notifier.DEBUG_EMAIL_TO = os.environ.get("DEBUG_EMAIL_TO", "")

    parser = argparse.ArgumentParser(
        description="Realtor scraper - scrapes listings and sends email reports"
    )
    parser.add_argument(
        "--debug",
        action="store_true",
        help="Debug mode: sends email to DEBUG_EMAIL_TO instead of EMAIL_TO",
    )
    parser.add_argument(
        "--days",
        type=int,
        default=1,
        help="Number of days to look back for listings (default: 1)",
    )
    args = parser.parse_args()

    if args.debug:
        debug_email = email_notifier.DEBUG_EMAIL_TO
        if debug_email:
            logger.info(f"Debug mode: will send to DEBUG_EMAIL_TO: {debug_email}")
        else:
            logger.warning("DEBUG_EMAIL_TO not set, will use EMAIL_TO")

    asyncio.run(run_scraper(days_old=args.days, debug_mode=args.debug))


if __name__ == "__main__":
    main()
