"""
FastAPI server for Realtor listings
- REST API for querying listings
- WebSocket for real-time updates
- Serves frontend
"""

from __future__ import annotations

import asyncio
import json
import os
import logging
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional, Set

from fastapi import (
    FastAPI,
    WebSocket,
    WebSocketDisconnect,
    Query,
    HTTPException,
    BackgroundTasks,
)
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse, FileResponse
from fastapi.staticfiles import StaticFiles

import db
from models import Listing, DbStats

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(title="Realtor Listings API", version="1.0.0")

# CORS for frontend
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# WebSocket connection manager
class ConnectionManager:
    def __init__(self):
        self.active_connections: Set[WebSocket] = set()

    async def connect(self, websocket: WebSocket):
        await websocket.accept()
        self.active_connections.add(websocket)
        logger.info(f"WebSocket connected. Total: {len(self.active_connections)}")

    def disconnect(self, websocket: WebSocket):
        self.active_connections.discard(websocket)
        logger.info(f"WebSocket disconnected. Total: {len(self.active_connections)}")

    async def broadcast(self, message: dict):
        """Broadcast message to all connected clients"""
        disconnected = []
        for connection in self.active_connections:
            try:
                await connection.send_json(message)
            except Exception:
                disconnected.append(connection)

        for conn in disconnected:
            self.active_connections.discard(conn)


manager = ConnectionManager()


# === REST API Endpoints ===


@app.get("/api/health")
async def health_check():
    """Health check endpoint"""
    return {"status": "ok", "timestamp": datetime.utcnow().isoformat()}


@app.get("/api/stats")
async def get_stats() -> dict:
    """Get database statistics"""
    stats = db.get_stats()
    return stats.to_dict()


@app.get("/api/listings")
async def get_listings(
    since: Optional[str] = Query(
        None, description="ISO timestamp - get listings updated after this time"
    ),
    septic: bool = Query(False, description="Filter to septic system only"),
    well: bool = Query(False, description="Filter to private well only"),
    city: Optional[str] = Query(None, description="Filter by city"),
    limit: int = Query(100, le=1000, description="Max results"),
) -> dict:
    """Get listings with optional filters"""
    since_dt = None
    if since:
        try:
            since_dt = datetime.fromisoformat(since.replace("Z", "+00:00"))
        except ValueError:
            raise HTTPException(400, "Invalid 'since' timestamp format")

    listings = db.get_listings(
        since=since_dt, septic_only=septic, well_only=well, city=city, limit=limit
    )

    return {
        "listings": [l.to_dict() for l in listings],
        "count": len(listings),
        "timestamp": datetime.utcnow().isoformat(),
    }


@app.get("/api/listings/septic-well")
async def get_septic_well_listings(
    hours: int = Query(24, le=168, description="Get listings from past N hours"),
    limit: int = Query(100, le=500),
) -> dict:
    """Get listings with septic system or private well"""
    listings = db.get_new_septic_well_listings(hours=hours)

    if limit:
        listings = listings[:limit]

    return {
        "listings": [l.to_dict() for l in listings],
        "count": len(listings),
        "hours": hours,
        "timestamp": datetime.utcnow().isoformat(),
    }


@app.get("/api/cities")
async def get_cities() -> dict:
    """Get all unique cities"""
    cities = db.get_all_cities()
    return {"cities": cities, "count": len(cities)}


# === WebSocket Endpoint ===


@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    """WebSocket for real-time listing updates"""
    await manager.connect(websocket)
    try:
        # Send initial stats
        stats = db.get_stats()
        await websocket.send_json(
            {
                "type": "connected",
                "stats": stats.to_dict(),
                "timestamp": datetime.utcnow().isoformat(),
            }
        )

        while True:
            # Keep connection alive, handle any client messages
            try:
                data = await asyncio.wait_for(websocket.receive_text(), timeout=30)
                # Could handle subscription filters here
                if data == "ping":
                    await websocket.send_json({"type": "pong"})
            except asyncio.TimeoutError:
                # Send periodic ping to keep connection alive
                await websocket.send_json({"type": "ping"})

    except WebSocketDisconnect:
        manager.disconnect(websocket)
    except Exception as e:
        logger.error(f"WebSocket error: {e}")
        manager.disconnect(websocket)


# === Internal endpoint for scraper to notify new listings ===


@app.post("/internal/notify")
async def notify_new_listings(data: dict):
    """
    Called by scraper to notify about new listings.
    Broadcasts to all WebSocket clients.
    """
    listings_data = data.get("listings", [])
    stats = data.get("stats", {})

    if manager.active_connections:
        await manager.broadcast(
            {
                "type": "new_listings",
                "count": len(listings_data),
                "listings": listings_data,
                "stats": stats,
                "timestamp": datetime.utcnow().isoformat(),
            }
        )
        logger.info(
            f"Broadcast {len(listings_data)} listings to {len(manager.active_connections)} clients"
        )

    return {"notified": len(manager.active_connections)}


# === Manual scraper trigger ===

# Track if scraper is running to prevent concurrent runs
_scraper_running = False


@app.post("/api/scraper/trigger")
async def trigger_scraper():
    """
    Manually trigger the scraper.
    Runs in background and returns immediately.
    """
    global _scraper_running

    if _scraper_running:
        raise HTTPException(409, "Scraper is already running")

    async def run_scraper_task():
        global _scraper_running
        _scraper_running = True
        try:
            # Import here to avoid circular imports
            from run_scraper import run_scraper

            await run_scraper()
        except Exception as e:
            logger.error(f"Scraper failed: {e}")
        finally:
            _scraper_running = False

    # Run in background
    asyncio.create_task(run_scraper_task())

    return {
        "status": "started",
        "message": "Scraper triggered, running in background",
        "timestamp": datetime.utcnow().isoformat(),
    }


@app.get("/api/scraper/status")
async def get_scraper_status():
    """Check if scraper is currently running"""
    return {"running": _scraper_running, "timestamp": datetime.utcnow().isoformat()}


# === Background polling for updates ===


async def poll_for_updates():
    """Background task to check for new listings and broadcast"""
    last_check = datetime.utcnow()

    while True:
        await asyncio.sleep(60)  # Check every minute

        if not manager.active_connections:
            continue

        try:
            # Check for listings updated since last check
            listings = db.get_listings(since=last_check, limit=50)

            if listings:
                # Filter to septic/well only for broadcasts
                sw_listings = [
                    l for l in listings if l.has_septic_system or l.has_private_well
                ]

                if sw_listings:
                    await manager.broadcast(
                        {
                            "type": "update",
                            "count": len(sw_listings),
                            "listings": [l.to_dict() for l in sw_listings],
                            "timestamp": datetime.utcnow().isoformat(),
                        }
                    )

            last_check = datetime.utcnow()

        except Exception as e:
            logger.error(f"Polling error: {e}")


@app.on_event("startup")
async def startup_event():
    """Initialize on startup"""
    logger.info("Starting API server...")
    try:
        db.init_database()
        asyncio.create_task(poll_for_updates())
        logger.info("Database initialized, polling started")
    except Exception as e:
        logger.error(f"Failed to initialize database: {e}")
        logger.error("Make sure DATABASE_URL environment variable is set correctly")
        raise


# === Serve Frontend ===

FRONTEND_PATH = Path(__file__).parent / "static"


@app.get("/", response_class=HTMLResponse)
async def serve_frontend():
    """Serve the frontend HTML"""
    index_path = FRONTEND_PATH / "index.html"
    if index_path.exists():
        return FileResponse(index_path)

    # Fallback inline HTML if static file doesn't exist
    return HTMLResponse("""
    <!DOCTYPE html>
    <html>
    <head><title>Realtor Listings</title></head>
    <body>
        <h1>Realtor Listings API</h1>
        <p>API is running. Frontend not found at /static/index.html</p>
        <p>API docs: <a href="/docs">/docs</a></p>
    </body>
    </html>
    """)


# Mount static files if directory exists
if FRONTEND_PATH.exists():
    app.mount("/static", StaticFiles(directory=str(FRONTEND_PATH)), name="static")


if __name__ == "__main__":
    import uvicorn

    port = int(os.environ.get("PORT", 8000))
    uvicorn.run(app, host="0.0.0.0", port=port)
