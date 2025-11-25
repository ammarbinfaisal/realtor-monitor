"""
Telegram notification service for Realtor scraper

- XLSX: Contains ALL listings scraped
- Telegram messages: Only NEW septic/well listings from past 24h
"""

from __future__ import annotations

import io
import os
import logging
from datetime import datetime
from typing import TYPE_CHECKING

import pandas as pd  # type: ignore
from telegram import Bot  # type: ignore
from telegram.constants import ParseMode  # type: ignore

if TYPE_CHECKING:
    from models import Listing, ScraperStats

logger = logging.getLogger(__name__)

TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "")


def is_configured() -> bool:
    """Check if Telegram notifications are configured"""
    return bool(TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID)


async def send_scrape_report(
    stats: ScraperStats,
    all_listings: list[Listing],
    new_septic_well_listings: list[Listing],
) -> None:
    """
    Send complete scrape report:
    - Telegram message with NEW septic/well listings only
    - XLSX attachment with ALL listings

    Args:
        stats: Scraper run statistics
        all_listings: ALL listings scraped (for XLSX)
        new_septic_well_listings: Only NEW listings with septic OR well (for message)
    """
    if not is_configured():
        logger.warning("Telegram not configured, skipping notification")
        return

    bot = Bot(token=TELEGRAM_BOT_TOKEN)

    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M UTC")
    duration = f"{stats.duration_seconds:.1f}s" if stats.duration_seconds else "N/A"

    # Build summary message - focus on septic/well findings
    message = f"*Realtor Scrape Report*\n{timestamp}\n\n"

    message += f"*Run Stats:*\n"
    message += f"- Processed: {stats.total_processed}\n"
    message += f"- New: {stats.new_listings} | Updated: {stats.updated_listings}\n"
    message += f"- Duration: {duration}\n\n"

    # Highlight septic/well listings (the main interest!)
    if new_septic_well_listings:
        message += f"*NEW Septic/Well Listings ({len(new_septic_well_listings)}):*\n\n"

        for listing in new_septic_well_listings[:10]:  # Show up to 10
            price_str = f"${listing.price:,}" if listing.price else "N/A"

            # Show what was found
            features = []
            if listing.has_septic_system:
                features.append("SEPTIC")
            if listing.has_private_well:
                features.append("WELL")
            feat_str = " | ".join(features)

            message += f"*{listing.address}*\n"
            message += f"{listing.city}, {listing.state_code} {listing.postal_code}\n"
            message += f"{price_str} | {listing.beds or '-'}bd/{listing.baths or '-'}ba | {listing.sqft or '-'}sqft\n"
            message += f"[{feat_str}]\n"

            if listing.agent_name:
                phone = listing.agent_phone or "N/A"
                message += f"Agent: {listing.agent_name} ({phone})\n"

            message += f"[View]({listing.listing_url})\n\n"

        if len(new_septic_well_listings) > 10:
            message += (
                f"_...and {len(new_septic_well_listings) - 10} more in spreadsheet_\n"
            )
    else:
        message += "*No NEW septic/well listings found this run.*\n"

    if stats.errors:
        message += f"\n_Errors: {len(stats.errors)}_"

    try:
        # Send message first
        await bot.send_message(
            chat_id=TELEGRAM_CHAT_ID,
            text=message,
            parse_mode=ParseMode.MARKDOWN,
            disable_web_page_preview=True,
        )
        logger.info("Sent Telegram summary notification")

        # Then send XLSX with ALL listings
        if all_listings:
            await _send_xlsx(bot, all_listings, "All Listings")

    except Exception as e:
        logger.error(f"Failed to send Telegram report: {e}")


async def _send_xlsx(bot: Bot, listings: list[Listing], title: str) -> None:
    """Generate and send XLSX file"""
    if not listings:
        return

    # Convert to dicts for DataFrame
    data = [listing.to_dict() for listing in listings]
    df = pd.DataFrame(data)

    # Separate septic/well listings to top
    df["_has_sw"] = df["has_septic_system"] | df["has_private_well"]
    df = df.sort_values(["_has_sw", "first_seen_at"], ascending=[False, False])
    df = df.drop("_has_sw", axis=1)

    # Select and order columns for export
    export_columns = [
        "address",
        "city",
        "state_code",
        "postal_code",
        "price",
        "beds",
        "baths",
        "sqft",
        "has_septic_system",
        "has_private_well",
        "septic_mentions",
        "well_mentions",
        "agent_name",
        "agent_phone",
        "brokerage_name",
        "listing_url",
        "list_date",
        "first_seen_at",
        "times_seen",
    ]

    available_columns = [col for col in export_columns if col in df.columns]
    df_export = df[available_columns].copy()

    # Rename columns for readability
    column_renames = {
        "address": "Address",
        "city": "City",
        "state_code": "State",
        "postal_code": "Zip",
        "price": "Price",
        "beds": "Beds",
        "baths": "Baths",
        "sqft": "Sqft",
        "has_septic_system": "Septic",
        "has_private_well": "Well",
        "septic_mentions": "Septic Details",
        "well_mentions": "Well Details",
        "agent_name": "Agent",
        "agent_phone": "Phone",
        "brokerage_name": "Brokerage",
        "listing_url": "URL",
        "list_date": "List Date",
        "first_seen_at": "First Seen",
        "times_seen": "Times Seen",
    }
    df_export = df_export.rename(columns=column_renames)

    # Generate XLSX in memory
    buffer = io.BytesIO()
    with pd.ExcelWriter(buffer, engine="openpyxl") as writer:  # type: ignore
        df_export.to_excel(writer, index=False, sheet_name="Listings")

        # Auto-adjust column widths
        worksheet = writer.sheets["Listings"]
        for idx, col in enumerate(df_export.columns):
            max_length = (
                max(df_export[col].astype(str).map(len).max(), len(str(col))) + 2
            )
            # Handle columns beyond Z
            if idx < 26:
                col_letter = chr(65 + idx)
            else:
                col_letter = f"A{chr(65 + idx - 26)}"
            worksheet.column_dimensions[col_letter].width = min(max_length, 50)

    buffer.seek(0)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M")
    filename = f"realtor_listings_{timestamp}.xlsx"

    # Count septic/well for caption
    septic_count = sum(1 for l in listings if l.has_septic_system)
    well_count = sum(1 for l in listings if l.has_private_well)

    caption = f"*{title}*\n"
    caption += f"Total: {len(listings)} | Septic: {septic_count} | Well: {well_count}"

    await bot.send_document(
        chat_id=TELEGRAM_CHAT_ID,
        document=buffer,
        filename=filename,
        caption=caption,
        parse_mode=ParseMode.MARKDOWN,
    )
    logger.info(f"Sent XLSX: {filename} ({len(listings)} listings)")


async def send_septic_well_alert(listing: Listing) -> None:
    """
    Send immediate alert for a single new septic/well listing.
    Use this for real-time alerts during scraping if desired.
    """
    if not is_configured():
        return

    if not (listing.has_septic_system or listing.has_private_well):
        return

    bot = Bot(token=TELEGRAM_BOT_TOKEN)

    price_str = f"${listing.price:,}" if listing.price else "N/A"

    features = []
    if listing.has_septic_system:
        features.append("SEPTIC")
    if listing.has_private_well:
        features.append("WELL")

    message = f"""
*NEW LISTING - {" & ".join(features)}*

*{listing.address or "N/A"}*
{listing.city or ""}, {listing.state_code or ""} {listing.postal_code or ""}

*Price:* {price_str}
*Details:* {listing.beds or "-"} bed / {listing.baths or "-"} bath / {listing.sqft or "-"} sqft

*Agent:* {listing.agent_name or "N/A"}
*Phone:* {listing.agent_phone or "N/A"}
*Brokerage:* {listing.brokerage_name or "N/A"}

[View Listing]({listing.listing_url})
"""

    try:
        await bot.send_message(
            chat_id=TELEGRAM_CHAT_ID,
            text=message,
            parse_mode=ParseMode.MARKDOWN,
            disable_web_page_preview=True,
        )
        logger.info(f"Sent septic/well alert: {listing.address}")
    except Exception as e:
        logger.error(f"Failed to send alert: {e}")


async def send_error_alert(error_message: str) -> None:
    """Send error notification"""
    if not is_configured():
        return

    bot = Bot(token=TELEGRAM_BOT_TOKEN)

    # Truncate long errors
    error_text = error_message[:800] if len(error_message) > 800 else error_message

    message = f"""
*Scraper Error*

```
{error_text}
```

Time: {datetime.now().strftime("%Y-%m-%d %H:%M UTC")}
"""

    try:
        await bot.send_message(
            chat_id=TELEGRAM_CHAT_ID, text=message, parse_mode=ParseMode.MARKDOWN
        )
    except Exception as e:
        logger.error(f"Failed to send error alert: {e}")
