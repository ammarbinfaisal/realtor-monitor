# Deployment Guide

## Architecture (Cost-Optimized)

```
┌─────────────────────────────────────────────────────────────────┐
│                         RAILWAY PROJECT                         │
│                                                                 │
│  ┌──────────────────────────────────────────────────────────┐  │
│  │  Scraper Service (Cron)                                  │  │
│  │  - Runs every 6 hours                                    │  │
│  │  - Scrapes Realtor.com                                   │  │
│  │  - Sends Telegram notification + XLSX                    │  │
│  └──────────────────────────────┬───────────────────────────┘  │
│                                 │                              │
│                                 ▼                              │
│  ┌──────────────────────────────────────────────────────────┐  │
│  │  PostgreSQL                                              │  │
│  │  - Listings, Agents tables                               │  │
│  └──────────────────────────────────────────────────────────┘  │
│                                 ▲                              │
│                                 │                              │
│  ┌──────────────────────────────┴───────────────────────────┐  │
│  │  API Service (Serverless/Scale-to-Zero)                  │  │
│  │  - REST API + Static frontend                            │  │
│  │  - Sleeps after 10 mins of inactivity                    │  │
│  │  - Wakes on first request (~5-10s cold start)            │  │
│  └──────────────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────────┘
```

## Prerequisites

1. **Railway Account**: https://railway.app
2. **Telegram Bot** (for notifications):
   - Message [@BotFather](https://t.me/botfather) on Telegram
   - Send `/newbot` and follow instructions
   - Save the bot token
   - Get your chat ID: message your bot, then visit `https://api.telegram.org/bot<TOKEN>/getUpdates`

## Deployment Steps

### 1. Create Railway Project

```bash
# Install Railway CLI
npm install -g @railway/cli

# Login
railway login

# Create new project
railway init
```

### 2. Add PostgreSQL Database

```bash
# Via CLI
railway add --database postgres

# Or via Dashboard:
# 1. Go to your project on railway.app
# 2. Click "+ New"
# 3. Select "Database" → "PostgreSQL"
```

### 3. Deploy API Service (First)

```bash
# Copy the API config
cp railway-api.toml railway.toml

# Deploy
railway up

# Get the deployment URL
railway domain
```

### 4. Deploy Scraper Service

In Railway dashboard:
1. Go to your project
2. Click "+ New" → "Empty Service"
3. Name it "scraper"
4. Connect your GitHub repo (or upload code)
5. In service settings:
   - **Start Command**: `python run_scraper.py`
   - **Cron Schedule**: `0 */6 * * *` (every 6 hours)

### 5. Set Environment Variables

In Railway dashboard, set these variables for BOTH services:

| Variable | Description |
|----------|-------------|
| `DATABASE_URL` | Auto-set by Railway when you link PostgreSQL |
| `TELEGRAM_BOT_TOKEN` | Your Telegram bot token |
| `TELEGRAM_CHAT_ID` | Your Telegram chat/group ID |

To link PostgreSQL to both services:
1. Click on PostgreSQL service
2. Go to "Variables"
3. Click "Add Reference" on each service

### 6. Enable Serverless (Scale-to-Zero)

To reduce costs, enable serverless mode for the API:

1. Go to Railway dashboard
2. Click on your API service
3. Go to **Settings** → **Deploy**
4. Enable **"Serverless"** toggle (may be called "App Sleeping")
5. The service will now:
   - Sleep after 10 minutes of no outbound traffic
   - Wake automatically on first request (~5-10s cold start)

**Note:** The frontend handles cold starts gracefully by showing "Starting API..." after 3 seconds of waiting.

### 7. Verify Deployment

```bash
# Check API health
curl https://your-api.up.railway.app/api/health

# Check stats
curl https://your-api.up.railway.app/api/stats

# Trigger manual scrape (for testing)
railway run python run_scraper.py
```

**Test the full flow:**
1. Visit your Railway app URL
2. First load may show "Starting API..." (cold start)
3. After ~5-10s, data should load
4. Subsequent requests are fast while API is warm

## Environment Variables

### Required

| Variable | Example | Description |
|----------|---------|-------------|
| `DATABASE_URL` | `postgresql://...` | PostgreSQL connection string |
| `TELEGRAM_BOT_TOKEN` | `123456:ABC-DEF...` | Telegram bot token |
| `TELEGRAM_CHAT_ID` | `123456789` | Your Telegram chat ID |

### Optional

| Variable | Default | Description |
|----------|---------|-------------|
| `PORT` | `8000` | API server port (Railway sets this) |

## Cron Schedule

The scraper runs on UTC time. Examples:

| Schedule | Description |
|----------|-------------|
| `0 */6 * * *` | Every 6 hours (default) |
| `0 */4 * * *` | Every 4 hours |
| `0 8,20 * * *` | 8 AM and 8 PM UTC |
| `0 14 * * *` | 2 PM UTC daily (9 AM CST) |

## Monitoring

### Logs

```bash
# View API logs
railway logs -s api

# View scraper logs
railway logs -s scraper
```

### Telegram Notifications

You'll receive:
- **Summary message**: After each scrape with new septic/well listings
- **XLSX attachment**: All listings from the scrape
- **Error alerts**: If scraper fails

## Cost Estimate (Serverless Setup)

| Component | Est. Usage | Cost |
|-----------|------------|------|
| Scraper (4x/day, ~5min each) | ~10 hrs/mo | ~$0.50 |
| API (serverless, on-demand) | ~5-20 hrs/mo | ~$0.25-1.00 |
| PostgreSQL (1GB) | Storage | ~$1 |
| **Total** | | **~$2-3/mo** |

### Cost Savings vs Always-On

| Setup | Monthly Cost |
|-------|--------------|
| Always-on API | ~$4-5/mo |
| Serverless API | ~$2-3/mo |
| **Savings** | **~50%** |

## Troubleshooting

### Scraper not running

1. Check cron schedule is set correctly
2. Check logs: `railway logs -s scraper`
3. Verify DATABASE_URL is set

### No Telegram notifications

1. Verify `TELEGRAM_BOT_TOKEN` and `TELEGRAM_CHAT_ID` are set
2. Make sure you've messaged the bot first
3. Check logs for "Telegram not configured" warnings

### Database connection issues

1. Ensure DATABASE_URL is referenced (not copied)
2. Check PostgreSQL service is running
3. Try restarting the service

## Local Development

```bash
# Install dependencies
pip install -r requirements.txt

# Set environment variables
export DATABASE_URL="postgresql://..."
export TELEGRAM_BOT_TOKEN="..."
export TELEGRAM_CHAT_ID="..."

# Run API locally
python api.py

# Run scraper locally
python run_scraper.py
```

## Files Overview

| File | Purpose |
|------|---------|
| `models.py` | Dataclasses for Listing, Agent, Stats |
| `db.py` | PostgreSQL database operations |
| `scraper_curl.py` | Realtor.com scraper (existing) |
| `run_scraper.py` | Cron job entry point |
| `notifier.py` | Telegram notifications |
| `api.py` | FastAPI REST + WebSocket server |
| `static/index.html` | Web frontend |
| `Dockerfile` | Container build |
| `railway-api.toml` | Railway config for API |
| `railway-scraper.toml` | Railway config for scraper |
