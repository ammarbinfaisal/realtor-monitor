"""
Email notification service for Realtor scraper using Resend.

Sends Excel file with septic/well matched listings to configured email address.
"""

from __future__ import annotations

import base64
import io
import os
import logging
from datetime import datetime
from typing import TYPE_CHECKING

import pandas as pd
import resend

if TYPE_CHECKING:
    from models import Listing, ScraperStats

logger = logging.getLogger(__name__)

# Resend configuration from environment variables
RESEND_API_KEY = os.environ.get("RESEND_API_KEY", "")
EMAIL_TO = os.environ.get("EMAIL_TO", "binfaisal.ammar@gmail.com")
EMAIL_FROM = os.environ.get("EMAIL_FROM", "realtor@fullstacktics.com")


def is_configured() -> bool:
    """Check if email notifications are configured"""
    return bool(RESEND_API_KEY and EMAIL_TO)


def generate_septic_well_xlsx(listings: list[Listing]) -> tuple[io.BytesIO, str]:
    """
    Generate Excel file with septic/well listings.

    Returns:
        Tuple of (buffer with xlsx data, filename)
    """
    if not listings:
        return io.BytesIO(), ""

    # Convert to list of dicts with specific columns for export
    data = []
    for listing in listings:
        # Combine septic and well mentions into matched phrases
        matched_phrases = []
        if listing.septic_mentions:
            matched_phrases.extend([f"SEPTIC: {m}" for m in listing.septic_mentions])
        if listing.well_mentions:
            matched_phrases.extend([f"WELL: {m}" for m in listing.well_mentions])

        data.append(
            {
                "Address": listing.address,
                "City": listing.city,
                "County": listing.county,
                "State": listing.state_code,
                "Zip": listing.postal_code,
                "Price": listing.price,
                "Beds": listing.beds,
                "Baths": listing.baths,
                "Sqft": listing.sqft,
                "List Date": listing.list_date,
                "Has Septic": "Yes" if listing.has_septic_system else "No",
                "Has Well": "Yes" if listing.has_private_well else "No",
                "Matched Phrases": "; ".join(matched_phrases),
                "Septic Details": ", ".join(listing.septic_mentions)
                if listing.septic_mentions
                else "",
                "Well Details": ", ".join(listing.well_mentions)
                if listing.well_mentions
                else "",
                "Agent": listing.agent_name,
                "Phone": listing.agent_phone,
                "Brokerage": listing.brokerage_name,
                "URL": listing.listing_url,
            }
        )

    df = pd.DataFrame(data)

    # Generate XLSX in memory
    buffer = io.BytesIO()
    with pd.ExcelWriter(buffer, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name="Septic-Well Listings")

        # Auto-adjust column widths
        worksheet = writer.sheets["Septic-Well Listings"]
        for idx, col in enumerate(df.columns):
            max_length = max(df[col].astype(str).map(len).max(), len(str(col))) + 2
            # Handle columns beyond Z
            if idx < 26:
                col_letter = chr(65 + idx)
            else:
                col_letter = f"A{chr(65 + idx - 26)}"
            worksheet.column_dimensions[col_letter].width = min(max_length, 50)

    buffer.seek(0)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M")
    filename = f"septic_well_listings_{timestamp}.xlsx"

    return buffer, filename


def send_email_with_attachment(
    subject: str,
    body: str,
    attachment_buffer: io.BytesIO | None = None,
    attachment_filename: str | None = None,
) -> bool:
    """
    Send email with optional attachment using Resend.

    Returns:
        True if email sent successfully, False otherwise
    """
    if not is_configured():
        logger.warning("Resend not configured, skipping notification")
        return False

    try:
        # Set API key
        resend.api_key = RESEND_API_KEY

        # Build email params
        email_params: dict = {
            "from": EMAIL_FROM,
            "to": [EMAIL_TO],
            "subject": subject,
            "text": body,
        }

        # Add attachment if provided
        if attachment_buffer and attachment_filename:
            attachment_content = base64.b64encode(attachment_buffer.read()).decode(
                "utf-8"
            )
            email_params["attachments"] = [
                {
                    "filename": attachment_filename,
                    "content": attachment_content,
                }
            ]

        # Send email
        response = resend.Emails.send(email_params)

        logger.info(
            f"Email sent successfully to {EMAIL_TO}, id: {response.get('id', 'unknown')}"
        )
        return True

    except Exception as e:
        logger.error(f"Failed to send email: {e}")
        return False


async def send_scrape_report(
    stats: ScraperStats,
    septic_well_listings: list[Listing],
) -> None:
    """
    Send scrape report via email with Excel attachment of septic/well listings.

    Args:
        stats: Scraper run statistics
        septic_well_listings: Listings with septic OR well (already filtered by list_date)
    """
    if not is_configured():
        logger.warning("Resend not configured, skipping notification")
        return

    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M CST")
    duration = f"{stats.duration_seconds:.1f}s" if stats.duration_seconds else "N/A"

    # Build email subject
    subject = f"Realtor Scrape Report - {len(septic_well_listings)} Septic/Well Listings Found"

    # Build email body
    body = f"""Realtor Scrape Report
{timestamp}

=== Run Statistics ===
Total Processed: {stats.total_processed}
New Listings: {stats.new_listings}
Updated: {stats.updated_listings}
Duration: {duration}

=== Septic/Well Listings ===
Found: {len(septic_well_listings)} listings with septic system or private well

"""

    if septic_well_listings:
        body += "Top listings:\n"
        for listing in septic_well_listings[:5]:
            price_str = f"${listing.price:,}" if listing.price else "N/A"
            features = []
            if listing.has_septic_system:
                features.append("SEPTIC")
            if listing.has_private_well:
                features.append("WELL")

            body += f"\n- {listing.address}, {listing.city}\n"
            body += (
                f"  {price_str} | {listing.beds or '-'}bd/{listing.baths or '-'}ba\n"
            )
            body += f"  [{' | '.join(features)}]\n"
            body += f"  {listing.listing_url}\n"

        if len(septic_well_listings) > 5:
            body += f"\n... and {len(septic_well_listings) - 5} more in the attached Excel file.\n"

        body += "\nSee attached Excel file for complete list with all details.\n"
    else:
        body += "No new septic/well listings found in this run.\n"

    if stats.errors:
        body += f"\nErrors: {len(stats.errors)}\n"
        for error in stats.errors[:3]:
            body += f"  - {error[:100]}...\n"

    # Generate Excel attachment
    attachment_buffer = None
    attachment_filename = None

    if septic_well_listings:
        attachment_buffer, attachment_filename = generate_septic_well_xlsx(
            septic_well_listings
        )

    # Send email
    send_email_with_attachment(
        subject=subject,
        body=body,
        attachment_buffer=attachment_buffer,
        attachment_filename=attachment_filename,
    )


async def send_error_alert(error_message: str) -> None:
    """Send error notification via email"""
    if not is_configured():
        return

    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M CST")

    # Truncate long errors
    error_text = error_message[:2000] if len(error_message) > 2000 else error_message

    subject = "Realtor Scraper Error Alert"
    body = f"""Scraper Error Alert
{timestamp}

Error:
{error_text}
"""

    send_email_with_attachment(subject=subject, body=body)
