# GraphQL Query Analysis - Redundant Data Report

## Summary
Analyzed 20 properties from the last 3 days across 5 Wisconsin counties to identify redundant data processing.

## Key Findings

### 1. **Massive Data Overhead per Property**
- **API Listing Size**: ~1,151 bytes per listing
- **Property Details Size**: ~27,087 bytes per listing
- **23.5x size increase** when fetching property details!

### 2. **Largest Redundant Fields**

#### Photos (7,832 bytes avg per property)
- Average of **25 photos per property**
- Fetches full photo URLs with tags
- **NOT NEEDED** for septic/well detection
- **Recommendation**: Remove `photos` field from GraphQL query

#### Augmented Gallery (6,336 bytes avg)
- Duplicate photo organization data
- **NOT NEEDED** for septic/well detection
- **Recommendation**: Remove `augmented_gallery` field

#### Advertisers (1,413 bytes avg)
- Fetched in BOTH API listing AND property details
- Identical data in both responses (28,260 bytes total redundant)
- **Recommendation**: Use only API listing advertisers, skip in details query

### 3. **Low Match Rate**
- Only **1 out of 20** properties (5%) had septic/well
- **95% of properties** required full details fetch but had no match
- This means we're downloading ~27KB per property when most don't match

### 4. **Unnecessary Fields in Property Details**

These fields consume bandwidth but aren't used for septic/well detection:

| Field | Avg Size | Needed? |
|-------|----------|---------|
| photos | 7,832 bytes | ❌ No |
| augmented_gallery | 6,336 bytes | ❌ No |
| mortgage | 1,774 bytes | ❌ No |
| consumer_advertisers | 838 bytes | ❌ No |
| virtual_tours | varies | ❌ No |
| matterport | varies | ❌ No |
| videos | varies | ❌ No |
| open_houses | varies | ❌ No |

### 5. **Essential Fields** (Keep These)
- `details` array - **CRITICAL** (contains septic/well info)
- `description.text` - **CRITICAL** (backup septic/well detection)
- `location` - Needed for county/address
- `list_date`, `list_price` - Basic listing info
- `property_id` - Identifier

## Recommendations

### Immediate Optimizations

1. **Create Minimal GraphQL Query**
   - Remove: `photos`, `augmented_gallery`, `mortgage`, `virtual_tours`, `matterport`, `videos`, `open_houses`, `consumer_advertisers`, `buyers`, `community`
   - Keep: `details`, `description`, `location`, `list_date`, `list_price`, `property_id`, `advertisers`, `source`
   - **Expected savings**: ~16KB per property (60% reduction)

2. **Use API Listing Advertisers**
   - Skip `advertisers` in property details query
   - Use advertiser data from initial API listing
   - **Savings**: 1,413 bytes per property

3. **Consider Two-Phase Approach** (Future Enhancement)
   - Phase 1: Check if `description.text` from API listing mentions septic/well
   - Phase 2: Only fetch full details for potential matches
   - **Potential savings**: Skip 95% of detail calls

### Example Optimized Query

See `scraper_curl.py:1300-1850` for the current query.

Suggested minimal query should only fetch:
```graphql
query FullPropertyDetails($propertyId: ID!) {
  home(property_id: $propertyId) {
    property_id
    list_date
    list_price
    description {
      text
      beds
      baths
      sqft
      type
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
        state_code
      }
    }
    advertisers {
      name
      href
      phones {
        number
        type
        primary
      }
      broker {
        name
      }
    }
    source {
      agents {
        agent_name
        agent_phone
      }
    }
  }
}
```

## Impact Analysis

### Current Approach
- 20 properties × 27,087 bytes = **541,740 bytes** (529 KB)
- Processing time: ~20 API calls × 0.3s = 6 seconds

### Optimized Approach
- 20 properties × ~11,000 bytes = **220,000 bytes** (215 KB)
- Processing time: ~20 API calls × 0.3s = 6 seconds (same)
- **Bandwidth savings**: 60% reduction
- **Faster JSON parsing**: Smaller payloads

### For 200 Properties (Daily Run)
- Current: ~5.2 MB
- Optimized: ~2.1 MB
- **Savings**: 3.1 MB per day

## Next Steps

1. ✅ Run debug script to analyze current queries
2. ⬜ Create optimized GraphQL query in `scraper_curl.py`
3. ⬜ Test optimized query with sample properties
4. ⬜ Deploy to production
5. ⬜ Monitor for any missing data issues

## Files Generated
- `debug_sample_api_listing.json` - Example API listing response
- `debug_sample_property_details.json` - Example property details response (full query)
