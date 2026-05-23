import asyncio
import json
import logging
import os
from contextlib import asynccontextmanager
from pathlib import Path

import asyncpg
import websockets
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException
from fastapi.responses import HTMLResponse
from pydantic import BaseModel

load_dotenv()

logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)

DATABASE_URL = os.getenv("DATABASE_URL", "")
AISSTREAM_API_KEY = os.getenv("AISSTREAM_API_KEY", "")
TRACKED_MMSIS = ["338234916"]

db_pool: asyncpg.Pool | None = None


async def get_pool() -> asyncpg.Pool:
    global db_pool
    if db_pool is None:
        raise RuntimeError("Database pool not initialized")
    return db_pool


async def init_db(pool: asyncpg.Pool) -> None:
    schema = Path("schema.sql").read_text()
    async with pool.acquire() as conn:
        await conn.execute(schema)
    log.info("Database schema initialized")


async def ais_stream_task(pool: asyncpg.Pool) -> None:
    url = "wss://stream.aisstream.io/v0/stream"
    subscribe_msg = json.dumps({
        "APIKey": AISSTREAM_API_KEY,
        "BoundingBoxes": [[[-90, -180], [90, 180]]],
        "FiltersShipMMSI": TRACKED_MMSIS,
        "FilterMessageTypes": ["PositionReport"],
    })

    while True:
        try:
            log.info("Connecting to AIS stream...")
            async with websockets.connect(url) as ws:
                await ws.send(subscribe_msg)
                log.info("Subscribed to AIS stream for MMSIs: %s", TRACKED_MMSIS)
                async for raw in ws:
                    try:
                        msg = json.loads(raw)
                        msg_type = msg.get("MessageType")
                        if msg_type != "PositionReport":
                            continue
                        meta = msg.get("MetaData", {})
                        report = msg.get("Message", {}).get("PositionReport", {})
                        mmsi = str(meta.get("MMSI", ""))
                        lat = meta.get("latitude")
                        lon = meta.get("longitude")
                        if not mmsi or lat is None or lon is None:
                            continue
                        speed = report.get("Sog")
                        heading = report.get("TrueHeading")
                        async with pool.acquire() as conn:
                            await conn.execute(
                                """
                                INSERT INTO positions (mmsi, lat, lon, speed, heading)
                                VALUES ($1, $2, $3, $4, $5)
                                """,
                                mmsi, float(lat), float(lon),
                                float(speed) if speed is not None else None,
                                float(heading) if heading is not None else None,
                            )
                        log.info("Saved position: MMSI=%s lat=%.4f lon=%.4f", mmsi, lat, lon)
                    except Exception:
                        log.exception("Error processing AIS message")
        except Exception:
            log.exception("AIS stream connection lost, reconnecting in 10s")
            await asyncio.sleep(10)


@asynccontextmanager
async def lifespan(app: FastAPI):
    global db_pool
    db_pool = await asyncpg.create_pool(DATABASE_URL, min_size=2, max_size=10)
    await init_db(db_pool)

    # Seed the tracked boat if not already present
    async with db_pool.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO boats (name, mmsi, color) VALUES ($1, $2, $3)
            ON CONFLICT (mmsi) DO NOTHING
            """,
            "San Diego Boat", "338234916", "#e74c3c",
        )

    stream_task = asyncio.create_task(ais_stream_task(db_pool))
    log.info("AIS stream task started")

    yield

    stream_task.cancel()
    try:
        await stream_task
    except asyncio.CancelledError:
        pass
    await db_pool.close()


app = FastAPI(title="AIS Tracker", lifespan=lifespan)


class BoatCreate(BaseModel):
    name: str
    mmsi: str
    color: str = "#3388ff"


@app.get("/", response_class=HTMLResponse)
async def serve_index():
    return HTMLResponse(Path("index.html").read_text())


@app.get("/api/boats")
async def list_boats():
    pool = await get_pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT id, name, mmsi, color, active, created_at FROM boats ORDER BY created_at"
        )
    return [dict(r) for r in rows]


@app.post("/api/boats", status_code=201)
async def add_boat(boat: BoatCreate):
    pool = await get_pool()
    async with pool.acquire() as conn:
        try:
            row = await conn.fetchrow(
                """
                INSERT INTO boats (name, mmsi, color)
                VALUES ($1, $2, $3)
                RETURNING id, name, mmsi, color, active, created_at
                """,
                boat.name, boat.mmsi, boat.color,
            )
        except asyncpg.UniqueViolationError:
            raise HTTPException(status_code=409, detail="MMSI already registered")
    return dict(row)


@app.get("/api/positions/{mmsi}")
async def get_positions(mmsi: str, limit: int = 500):
    pool = await get_pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT lat, lon, speed, heading, timestamp
            FROM positions
            WHERE mmsi = $1
            ORDER BY timestamp DESC
            LIMIT $2
            """,
            mmsi, limit,
        )
    return [dict(r) for r in rows]
