#!/usr/bin/env python3
"""
Debug script to analyze GraphQL query responses for property details.
Tests with a large number of properties to identify redundant data processing.

Usage:
    python debug_graphql_query.py [--days 7] [--limit 50]
"""

import argparse
import asyncio
import json
import sys
from datetime import datetime
from collections import defaultdict, Counter
from typing import Optional, Any

from scraper_curl import RealtorScraperCurl, deduplicate_listings


class GraphQLDebugger:
    def __init__(self, scraper: RealtorScraperCurl):
        self.scraper = scraper
        self.stats = {
            "total_listings": 0,
            "total_detail_calls": 0,
            "septic_well_matches": 0,
            "septic_only": 0,
            "well_only": 0,
            "neither": 0,
            "api_data_analysis": defaultdict(int),
            "detail_data_analysis": defaultdict(int),
            "redundant_fields": [],
            "field_sizes": defaultdict(list),
        }
        self.sample_responses: dict[str, Any] = {
            "api_listing": None,
            "property_details": None,
        }

    def analyze_api_listing(self, listing: dict) -> dict:
        """Analyze what data is available in the initial API listing response"""
        analysis = {
            "has_property_id": bool(listing.get("property_id")),
            "has_address": bool(listing.get("location", {}).get("address")),
            "has_price": bool(listing.get("list_price")),
            "has_description": bool(listing.get("description")),
            "has_advertisers": bool(listing.get("advertisers")),
            "advertiser_count": len(listing.get("advertisers", [])),
            "total_size_bytes": len(json.dumps(listing)),
        }

        # Track field sizes
        for key, value in listing.items():
            if value:
                size = len(json.dumps(value))
                self.stats["field_sizes"][f"api.{key}"].append(size)

        return analysis

    def analyze_property_details(self, details: dict) -> dict:
        """Analyze what data is fetched in the property details call"""
        if not details:
            return {}

        analysis = {
            "has_details": bool(details.get("details")),
            "details_count": len(details.get("details", [])),
            "has_description": bool(details.get("description")),
            "has_advertisers": bool(details.get("advertisers")),
            "advertiser_count": len(details.get("advertisers", [])),
            "has_photos": bool(details.get("photos")),
            "photo_count": len(details.get("photos", [])),
            "has_mortgage": bool(details.get("mortgage")),
            "has_community": bool(details.get("community")),
            "has_augmented_gallery": bool(details.get("augmented_gallery")),
            "has_virtual_tours": bool(details.get("virtual_tours")),
            "has_matterport": bool(details.get("matterport")),
            "has_buyers": bool(details.get("buyers")),
            "total_size_bytes": len(json.dumps(details)),
        }

        # Track field sizes
        for key, value in details.items():
            if value:
                size = len(json.dumps(value))
                self.stats["field_sizes"][f"detail.{key}"].append(size)

        return analysis

    def identify_redundant_data(self, api_listing: dict, property_details: dict):
        """Compare API listing vs property details to find redundant fields"""
        redundant = []

        # Fields that appear in both responses
        common_fields = ["description", "list_price", "list_date", "property_id"]

        for field in common_fields:
            api_value = api_listing.get(field)
            detail_value = property_details.get(field)

            if api_value and detail_value:
                if api_value == detail_value:
                    redundant.append(
                        {
                            "field": field,
                            "reason": "Identical in both API and details",
                            "size_bytes": len(json.dumps(detail_value)),
                        }
                    )

        # Check advertisers
        api_advertisers = api_listing.get("advertisers", [])
        detail_advertisers = property_details.get("advertisers", [])

        if api_advertisers and detail_advertisers:
            redundant.append(
                {
                    "field": "advertisers",
                    "reason": f"Fetched in both (API: {len(api_advertisers)}, Details: {len(detail_advertisers)})",
                    "size_bytes": len(json.dumps(detail_advertisers)),
                }
            )

        return redundant

    def print_field_size_analysis(self):
        """Print analysis of field sizes to identify large/unnecessary data"""
        print("\n" + "=" * 80)
        print("FIELD SIZE ANALYSIS (Top 10 largest fields by average size)")
        print("=" * 80)

        # Calculate average sizes
        avg_sizes = {}
        for field, sizes in self.stats["field_sizes"].items():
            if sizes:
                avg_sizes[field] = sum(sizes) / len(sizes)

        # Sort by average size
        sorted_fields = sorted(avg_sizes.items(), key=lambda x: x[1], reverse=True)[:10]

        for field, avg_size in sorted_fields:
            sizes = self.stats["field_sizes"][field]
            print(
                f"  {field:40s} | Avg: {avg_size:8.0f} bytes | "
                f"Max: {max(sizes):8.0f} | Min: {min(sizes):8.0f} | Count: {len(sizes)}"
            )

    async def debug_query_batch(
        self, days_old: int = 7, max_listings: int = 50
    ) -> dict:
        """
        Run GraphQL queries for a batch of listings and analyze the data.

        Args:
            days_old: Number of days to look back for listings
            max_listings: Maximum number of listings to process

        Returns:
            Dictionary with debug statistics
        """
        print("=" * 80)
        print("GraphQL Query Debug Analysis")
        print("=" * 80)
        print(f"Configuration:")
        print(f"  - Looking back: {days_old} days")
        print(f"  - Max listings to analyze: {max_listings}")
        print(f"  - Target counties: {self.scraper.TARGET_COUNTIES}")
        print("=" * 80)

        # Step 1: Get listings from API
        print("\n[1/3] Fetching listings from API...")
        api_listings = []

        for county_name in self.scraper.TARGET_COUNTIES:
            print(f"  Searching county: {county_name}, WI")
            county_listings = self.scraper.search_listings_api(
                state_code="WI",
                limit=200,
                days_old=days_old,
                county=county_name,
            )
            api_listings.extend(county_listings)
            await asyncio.sleep(0.5)  # Rate limiting

        # Deduplicate
        api_listings = deduplicate_listings(api_listings)
        print(f"  Total unique listings: {len(api_listings)}")

        # Limit to max_listings
        if len(api_listings) > max_listings:
            print(f"  Limiting to first {max_listings} listings for analysis")
            api_listings = api_listings[:max_listings]

        self.stats["total_listings"] = len(api_listings)

        # Save first sample
        if api_listings and not self.sample_responses.get("api_listing"):
            self.sample_responses["api_listing"] = api_listings[0]  # type: ignore

        # Step 2: Analyze API listing data
        print("\n[2/3] Analyzing API listing data...")
        for i, listing in enumerate(api_listings):
            analysis = self.analyze_api_listing(listing)

            for key, value in analysis.items():
                if isinstance(value, bool):
                    if value:
                        self.stats["api_data_analysis"][key] += 1
                elif isinstance(value, int):
                    self.stats["api_data_analysis"][f"{key}_sum"] += value

            if (i + 1) % 10 == 0:
                print(f"  Analyzed {i + 1}/{len(api_listings)} API listings...")

        # Step 3: Fetch property details and analyze
        print("\n[3/3] Fetching property details and analyzing...")
        septic_well_properties = []

        for i, listing in enumerate(api_listings):
            property_id = listing.get("property_id")
            if not property_id:
                continue

            # Fetch details
            await asyncio.sleep(0.3)  # Rate limiting
            details = await self.scraper.get_property_details_async(property_id)

            if details:
                self.stats["total_detail_calls"] += 1

                # Save first sample
                if not self.sample_responses["property_details"]:
                    self.sample_responses["property_details"] = details

                # Analyze details
                analysis = self.analyze_property_details(details)
                for key, value in analysis.items():
                    if isinstance(value, bool):
                        if value:
                            self.stats["detail_data_analysis"][key] += 1
                    elif isinstance(value, int):
                        self.stats["detail_data_analysis"][f"{key}_sum"] += value

                # Check for septic/well
                septic_well_check = self.scraper.check_property_for_septic_well(details)
                has_septic = septic_well_check["has_septic"]
                has_well = septic_well_check["has_well"]

                if has_septic or has_well:
                    septic_well_properties.append(
                        {
                            "property_id": property_id,
                            "address": details.get("location", {})
                            .get("address", {})
                            .get("line"),
                            "has_septic": has_septic,
                            "has_well": has_well,
                            "match_score": septic_well_check["match_score"],
                        }
                    )

                if has_septic and has_well:
                    self.stats["septic_well_matches"] += 1
                elif has_septic:
                    self.stats["septic_only"] += 1
                elif has_well:
                    self.stats["well_only"] += 1
                else:
                    self.stats["neither"] += 1

                # Identify redundant data
                redundant = self.identify_redundant_data(listing, details)
                self.stats["redundant_fields"].extend(redundant)

            if (i + 1) % 10 == 0 or (i + 1) == len(api_listings):
                print(
                    f"  Processed {i + 1}/{len(api_listings)} property details... "
                    f"(Septic/Well: {self.stats['septic_well_matches']})"
                )

        await self.scraper.close_async_session()

        # Print results
        self.print_results(septic_well_properties)

        return self.stats

    def print_results(self, septic_well_properties: list):
        """Print debug analysis results"""
        print("\n" + "=" * 80)
        print("ANALYSIS RESULTS")
        print("=" * 80)

        # Summary stats
        print(f"\nSUMMARY:")
        print(f"  Total listings analyzed: {self.stats['total_listings']}")
        print(f"  Property detail calls made: {self.stats['total_detail_calls']}")
        print(
            f"  Properties with BOTH septic & well: {self.stats['septic_well_matches']}"
        )
        print(f"  Properties with septic only: {self.stats['septic_only']}")
        print(f"  Properties with well only: {self.stats['well_only']}")
        print(f"  Properties with neither: {self.stats['neither']}")

        # API data analysis
        print(f"\nAPI LISTING DATA:")
        print(
            f"  Listings with property_id: {self.stats['api_data_analysis']['has_property_id']}"
        )
        print(
            f"  Listings with address: {self.stats['api_data_analysis']['has_address']}"
        )
        print(f"  Listings with price: {self.stats['api_data_analysis']['has_price']}")
        print(
            f"  Listings with description: {self.stats['api_data_analysis']['has_description']}"
        )
        print(
            f"  Listings with advertisers: {self.stats['api_data_analysis']['has_advertisers']}"
        )

        if self.stats["total_listings"] > 0:
            avg_api_size = (
                self.stats["api_data_analysis"]["total_size_bytes_sum"]
                / self.stats["total_listings"]
            )
            print(f"  Average API listing size: {avg_api_size:.0f} bytes")

        # Property details data analysis
        print(f"\nPROPERTY DETAILS DATA:")
        print(
            f"  Properties with details array: {self.stats['detail_data_analysis']['has_details']}"
        )
        if self.stats["detail_data_analysis"]["details_count_sum"] > 0:
            avg_details = (
                self.stats["detail_data_analysis"]["details_count_sum"]
                / self.stats["total_detail_calls"]
            )
            print(f"  Average details count per property: {avg_details:.1f}")

        print(
            f"  Properties with photos: {self.stats['detail_data_analysis']['has_photos']}"
        )
        if self.stats["detail_data_analysis"]["photo_count_sum"] > 0:
            avg_photos = (
                self.stats["detail_data_analysis"]["photo_count_sum"]
                / self.stats["total_detail_calls"]
            )
            print(f"  Average photo count per property: {avg_photos:.1f}")

        print(
            f"  Properties with mortgage data: {self.stats['detail_data_analysis']['has_mortgage']}"
        )
        print(
            f"  Properties with community data: {self.stats['detail_data_analysis']['has_community']}"
        )
        print(
            f"  Properties with augmented_gallery: {self.stats['detail_data_analysis']['has_augmented_gallery']}"
        )
        print(
            f"  Properties with virtual_tours: {self.stats['detail_data_analysis']['has_virtual_tours']}"
        )
        print(
            f"  Properties with matterport: {self.stats['detail_data_analysis']['has_matterport']}"
        )

        if self.stats["total_detail_calls"] > 0:
            avg_detail_size = (
                self.stats["detail_data_analysis"]["total_size_bytes_sum"]
                / self.stats["total_detail_calls"]
            )
            print(f"  Average property details size: {avg_detail_size:.0f} bytes")

        # Redundant data analysis
        print(f"\nREDUNDANT DATA ANALYSIS:")
        if self.stats["redundant_fields"]:
            # Aggregate by field
            redundant_by_field = defaultdict(list)
            for r in self.stats["redundant_fields"]:
                redundant_by_field[r["field"]].append(r)

            for field, occurrences in redundant_by_field.items():
                count = len(occurrences)
                avg_size = sum(r["size_bytes"] for r in occurrences) / count
                total_size = sum(r["size_bytes"] for r in occurrences)
                print(f"  {field}:")
                print(f"    - Occurrences: {count}")
                print(f"    - Avg size: {avg_size:.0f} bytes")
                print(f"    - Total redundant data: {total_size:.0f} bytes")
                if occurrences:
                    print(f"    - Reason: {occurrences[0]['reason']}")
        else:
            print("  No redundant fields detected")

        # Field size analysis
        self.print_field_size_analysis()

        # Septic/Well matches
        if septic_well_properties:
            print(f"\nSEPTIC/WELL PROPERTIES FOUND:")
            for prop in septic_well_properties[:10]:  # Show first 10
                print(
                    f"  - {prop['address']} (Septic: {prop['has_septic']}, "
                    f"Well: {prop['has_well']}, Score: {prop['match_score']})"
                )
            if len(septic_well_properties) > 10:
                print(f"  ... and {len(septic_well_properties) - 10} more")

        # Save sample responses
        print(f"\nSAMPLE DATA SAVED TO:")
        with open("debug_sample_api_listing.json", "w") as f:
            json.dump(self.sample_responses["api_listing"], f, indent=2)
        print(f"  - debug_sample_api_listing.json")

        with open("debug_sample_property_details.json", "w") as f:
            json.dump(self.sample_responses["property_details"], f, indent=2)
        print(f"  - debug_sample_property_details.json")

        # Recommendations
        print(f"\nRECOMMENDATIONS:")
        self.print_recommendations()

    def print_recommendations(self):
        """Print recommendations based on analysis"""
        recommendations = []

        # Check for large unnecessary fields
        large_fields = []
        for field, sizes in self.stats["field_sizes"].items():
            if sizes:
                avg_size = sum(sizes) / len(sizes)
                if "detail." in field and avg_size > 5000:
                    large_fields.append((field, avg_size))

        if large_fields:
            recommendations.append(
                "REDUCE GRAPHQL QUERY SIZE:\n"
                f"    The following fields are large and may not be needed:\n"
                + "\n".join(
                    f"    - {field} (avg: {size:.0f} bytes)"
                    for field, size in large_fields[:5]
                )
            )

        # Check for redundant fields
        if self.stats["redundant_fields"]:
            recommendations.append(
                "AVOID REDUNDANT DATA FETCHING:\n"
                "    Some fields are fetched in both API listing and property details.\n"
                "    Consider using only the API listing data for these fields."
            )

        # Check for unnecessary detail calls
        neither_ratio = (
            self.stats["neither"] / self.stats["total_detail_calls"]
            if self.stats["total_detail_calls"] > 0
            else 0
        )
        if neither_ratio > 0.7:
            recommendations.append(
                f"OPTIMIZE DETAIL CALLS:\n"
                f"    {neither_ratio * 100:.1f}% of properties have neither septic nor well.\n"
                f"    Consider pre-filtering before fetching full details (if possible)."
            )

        # Check photo/media data
        if self.stats["detail_data_analysis"]["photo_count_sum"] > 0:
            avg_photos = (
                self.stats["detail_data_analysis"]["photo_count_sum"]
                / self.stats["total_detail_calls"]
            )
            if avg_photos > 10:
                recommendations.append(
                    f"REDUCE PHOTO DATA:\n"
                    f"    Average {avg_photos:.1f} photos per property.\n"
                    f"    If photos aren't needed, remove 'photos' from GraphQL query."
                )

        if recommendations:
            for i, rec in enumerate(recommendations, 1):
                print(f"  {i}. {rec}")
        else:
            print("  No specific optimizations detected. Current approach looks good!")


async def main():
    parser = argparse.ArgumentParser(
        description="Debug GraphQL queries to identify redundant data processing"
    )
    parser.add_argument(
        "--days",
        type=int,
        default=7,
        help="Number of days to look back for listings (default: 7)",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=50,
        help="Maximum number of listings to analyze (default: 50)",
    )
    args = parser.parse_args()

    scraper = RealtorScraperCurl()
    debugger = GraphQLDebugger(scraper)

    try:
        await debugger.debug_query_batch(days_old=args.days, max_listings=args.limit)
    except KeyboardInterrupt:
        print("\n\nDebug interrupted by user")
        await scraper.close_async_session()
        sys.exit(0)
    except Exception as e:
        print(f"\n\nError: {e}")
        import traceback

        traceback.print_exc()
        await scraper.close_async_session()
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main())
