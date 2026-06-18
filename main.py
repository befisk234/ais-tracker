import asyncio
import json
import logging
import math
import os
import urllib.request
from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import asyncpg
import websockets
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException, Request, Response
from fastapi.responses import HTMLResponse, RedirectResponse
from itsdangerous import BadSignature, SignatureExpired, URLSafeTimedSerializer
from pydantic import BaseModel
from starlette.middleware.base import BaseHTTPMiddleware

load_dotenv()

logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)

DATABASE_URL = os.getenv("DATABASE_URL", "")
AISSTREAM_API_KEY = os.getenv("AISSTREAM_API_KEY", "")
ADMIN_USER = os.getenv("ADMIN_USER", "admin")
ADMIN_PASS = os.getenv("ADMIN_PASS", "changeme")
SECRET_KEY = os.getenv("SECRET_KEY", "dev-secret-change-in-production")
SESSION_COOKIE = "ais_session"

RECONNECT_INTERVAL = 14 * 60
GLOBAL_BBOX = [[[-90, -180], [90, 180]]]

_serializer = URLSafeTimedSerializer(SECRET_KEY)
db_pool: asyncpg.Pool | None = None
tracked_mmsis: dict[str, str] = {}
stream_task: asyncio.Task | None = None

HOME_LAT, HOME_LON = 32.6644, -117.2417
HOME_RADIUS_NM = 2.0

FISHING_BANKS = [
    {"name": "Point Loma Kelp",         "lat": 32.7083, "lon": -117.2717},
    {"name": "Bird Rock La Jolla",       "lat": 32.8150, "lon": -117.2762},
    {"name": "La Jolla Kelp",            "lat": 32.8333, "lon": -117.2917},
    {"name": "Del Mar Kelp",             "lat": 32.9750, "lon": -117.2833},
    {"name": "Leucadia Kelp",            "lat": 33.0667, "lon": -117.3108},
    {"name": "Scripps Canyon",           "lat": 32.8700, "lon": -117.2700},
    {"name": "Quast Rock",               "lat": 32.8500, "lon": -117.2783},
    {"name": "Horseshoe Reef",           "lat": 32.8567, "lon": -117.2733},
    {"name": "Imperial Beach Pipe",      "lat": 32.5800, "lon": -117.1800},
    {"name": "IB Kelp",                  "lat": 32.5667, "lon": -117.1833},
    {"name": "Coronado Islands North",   "lat": 32.4167, "lon": -117.2500},
    {"name": "Coronado Islands Middle",  "lat": 32.4000, "lon": -117.2667},
    {"name": "Coronado Islands South",   "lat": 32.3833, "lon": -117.2833},
    {"name": "Rockpile",                 "lat": 32.2833, "lon": -117.1583},
    {"name": "9 Mile Bank",              "lat": 32.6167, "lon": -117.4833},
    {"name": "13 Fathom Spot",           "lat": 32.6833, "lon": -117.4500},
    {"name": "43 Fathom Spot",           "lat": 32.6560, "lon": -117.9688},
    {"name": "178 Spot",                 "lat": 32.6000, "lon": -117.5667},
    {"name": "181 Fathom Spot",          "lat": 32.9067, "lon": -117.8833},
    {"name": "182 Fathom Spot",          "lat": 32.6933, "lon": -117.7067},
    {"name": "The Bump",                 "lat": 32.8533, "lon": -117.3050},
    {"name": "The Half",                 "lat": 32.7967, "lon": -117.2917},
    {"name": "3/4 Spot",                 "lat": 32.7833, "lon": -117.3167},
    {"name": "The 10",                   "lat": 32.8483, "lon": -117.3033},
    {"name": "NW Corner",                "lat": 32.4250, "lon": -117.2700},
    {"name": "The 32",                   "lat": 32.7950, "lon": -117.2933},
    {"name": "The 270",                  "lat": 32.4500, "lon": -117.2900},
    {"name": "Widow Rock",               "lat": 32.8200, "lon": -117.2933},
    {"name": "Jetty Kelp",               "lat": 32.6800, "lon": -117.2400},
    {"name": "SW Corner",                "lat": 32.3833, "lon": -117.2917},
    {"name": "Round House",              "lat": 32.7000, "lon": -117.2500},
    {"name": "Pine Tree",                "lat": 32.7350, "lon": -117.2533},
    {"name": "Hill Street",              "lat": 32.7600, "lon": -117.2567},
    {"name": "USS Yukon Wreck",          "lat": 32.6800, "lon": -117.2867},
    {"name": "5 Tanks",                  "lat": 32.6633, "lon": -117.2517},
    {"name": "Sub Wreck",                "lat": 32.6800, "lon": -117.2800},
    {"name": "Zuniga Jetty",             "lat": 32.6783, "lon": -117.2350},
    {"name": "USS Hogan Wreck",          "lat": 32.6483, "lon": -117.3033},
    {"name": "International Reef",       "lat": 32.5333, "lon": -117.1833},
    {"name": "Las Playas Kelp",          "lat": 32.5167, "lon": -117.1283},
    {"name": "Waterfall",                "lat": 32.4583, "lon": -117.1933},
    {"name": "Pukey Point",              "lat": 32.4083, "lon": -117.2667},
    {"name": "30 Mile Bank",             "lat": 32.7917, "lon": -117.7767},
    {"name": "60 Mile Bank",             "lat": 32.4500, "lon": -117.7833},
    {"name": "86 Bank",                  "lat": 32.4000, "lon": -118.0500},
    {"name": "209 Fathom Spot",          "lat": 33.0937, "lon": -117.8713},
    {"name": "213 Fathom Bank",          "lat": 31.7833, "lon": -117.8833},
    {"name": "226 Spot",                 "lat": 32.5167, "lon": -117.7333},
    {"name": "277 Fathom Spot",          "lat": 33.1983, "lon": -118.0787},
    {"name": "295 Fathom Spot",          "lat": 31.5833, "lon": -117.4000},
    {"name": "302 Fathom / Kidney",      "lat": 32.4417, "lon": -117.5833},
    {"name": "312 Spot",                 "lat": 32.4333, "lon": -117.8333},
    {"name": "371 Bank",                 "lat": 32.7167, "lon": -117.9500},
    {"name": "390 Bank",                 "lat": 32.0833, "lon": -118.2833},
    {"name": "425 Fathom Spot",          "lat": 32.2500, "lon": -117.3833},
    {"name": "499 Spot",                 "lat": 31.9167, "lon": -118.0833},
    {"name": "Airplane Bank",            "lat": 31.5833, "lon": -117.6500},
    {"name": "Mushroom",                 "lat": 32.1500, "lon": -117.7000},
    {"name": "Finger Bank",              "lat": 32.2167, "lon": -117.7833},
    {"name": "East Pinnacle",            "lat": 32.3000, "lon": -118.4833},
    {"name": "Hidden Bank",              "lat": 32.4500, "lon": -118.4500},
    {"name": "Catalina West End",        "lat": 33.4000, "lon": -118.6167},
    {"name": "San Clemente Island East", "lat": 33.0000, "lon": -118.5167},
    {"name": "Pyramid Head",             "lat": 32.8153, "lon": -118.3542},
    {"name": "China Point",              "lat": 32.8000, "lon": -118.4250},
    {"name": "14 Mile Bank",             "lat": 33.5500, "lon": -118.1500},
    {"name": "Farnsworth Bank",          "lat": 33.3417, "lon": -118.5167},
    {"name": "Ship Rock Catalina",       "lat": 33.4750, "lon": -118.5167},
    {"name": "Cortez Bank",              "lat": 32.4443, "lon": -119.1108},
    {"name": "Tanner Bank",              "lat": 32.7058, "lon": -119.1335},
    {"name": "Butterfly Bank",           "lat": 32.3667, "lon": -118.2500},
    {"name": "1010 Trench",              "lat": 31.5000, "lon": -118.5000},
]


def _bearing_to(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    phi1 = math.radians(lat1)
    phi2 = math.radians(lat2)
    dlambda = math.radians(lon2 - lon1)
    y = math.sin(dlambda) * math.cos(phi2)
    x = math.cos(phi1) * math.sin(phi2) - math.sin(phi1) * math.cos(phi2) * math.cos(dlambda)
    return (math.atan2(y, x) * 180 / math.pi + 360) % 360


def _compass_dir(bearing: float) -> str:
    dirs = ["N", "NE", "E", "SE", "S", "SW", "W", "NW"]
    return dirs[round(bearing / 45) % 8]


def _fmt_time_ago(ts: datetime) -> str:
    diff_s = (datetime.now(timezone.utc) - ts).total_seconds()
    diff_min = int(diff_s / 60)
    if diff_min < 1:
        return "just now"
    if diff_min < 60:
        return f"{diff_min} min ago"
    diff_h = diff_min // 60
    diff_m = diff_min % 60
    if diff_h < 24:
        return f"{diff_h}h {diff_m}m ago" if diff_m > 0 else f"{diff_h}h ago"
    diff_d = diff_h // 24
    return f"{diff_d} day{'s' if diff_d != 1 else ''} ago"


def _fmt_duration(minutes: float) -> str:
    total = int(minutes)
    h, m = total // 60, total % 60
    if h == 0:
        return f"{m} min"
    return f"{h}h {m}m" if m > 0 else f"{h}h"


def _fmt_eta(hours: float) -> str:
    total_min = round(hours * 60)
    if total_min < 60:
        return f"{total_min} min away"
    h, m = total_min // 60, total_min % 60
    return f"{h}h {m}m away" if m > 0 else f"{h}h away"


def _nearest_bank(lat: float, lon: float, max_nm: float = 2.0) -> str | None:
    best: str | None = None
    best_d = max_nm
    for bank in FISHING_BANKS:
        d = _nm_dist(lat, lon, bank["lat"], bank["lon"])
        if d < best_d:
            best_d = d
            best = bank["name"]
    return best

COLOR_PALETTE = [
    "#E6194B",  # Red
    "#3CB44B",  # Green
    "#4363D8",  # Blue
    "#F58231",  # Orange
    "#911EB4",  # Purple
    "#42D4F4",  # Cyan
    "#F032E6",  # Magenta
    "#BFEF45",  # Lime
    "#FABED4",  # Pink
    "#469990",  # Teal
    "#9A6324",  # Brown
    "#800000",  # Maroon
    "#AAFFC3",  # Mint
    "#808000",  # Olive
    "#FFD8B1",  # Apricot
    "#000075",  # Navy
    "#A9A9A9",  # Gray
    "#FFE119",  # Yellow
    "#DCBEFF",  # Lavender
    "#000000",  # Black
]


LOGIN_HTML = """\
<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1.0" />
  <title>AIS Tracker — Sign in</title>
  <style>
    * {{ margin: 0; padding: 0; box-sizing: border-box; }}
    body {{
      font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
      background: #0f1117; color: #e0e0e0;
      height: 100vh; display: flex; align-items: center; justify-content: center;
    }}
    .card {{
      background: #1a1d27; border: 1px solid #2a2d3a;
      border-radius: 12px; padding: 36px 40px; width: 340px;
    }}
    h1 {{ font-size: 20px; font-weight: 600; margin-bottom: 24px; }}
    h1 span {{ color: #4a9eff; }}
    label {{ font-size: 13px; color: #888; display: block; margin-bottom: 6px; }}
    input {{
      width: 100%; background: #0f1117; border: 1px solid #2a2d3a;
      border-radius: 7px; color: #e0e0e0; padding: 9px 12px;
      font-size: 14px; margin-bottom: 16px; outline: none;
    }}
    input:focus {{ border-color: #4a9eff; }}
    button {{
      width: 100%; background: #4a9eff; color: #fff; border: none;
      border-radius: 7px; padding: 10px; font-size: 14px; font-weight: 600; cursor: pointer;
    }}
    button:hover {{ background: #3a8eef; }}
    .error {{ color: #e74c3c; font-size: 13px; margin-bottom: 14px; }}
  </style>
</head>
<body>
  <div class="card">
    <h1>AIS <span>Tracker</span></h1>
    {error}
    <form method="post" action="/login">
      <label for="username">Username</label>
      <input type="text" id="username" name="username" autocomplete="username" required />
      <label for="password">Password</label>
      <input type="password" id="password" name="password" autocomplete="current-password" required />
      <button type="submit">Sign in</button>
    </form>
  </div>
</body>
</html>"""


def _verify_session(token: str) -> str | None:
    try:
        return _serializer.loads(token, max_age=86400 * 7)
    except (BadSignature, SignatureExpired):
        return None


class AuthMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next: object):
        if request.url.path in ("/login",):
            return await call_next(request)
        token = request.cookies.get(SESSION_COOKIE)
        if not token or not _verify_session(token):
            return RedirectResponse("/login", status_code=302)
        return await call_next(request)


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


def _nm_dist(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    R = 3440.065
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlambda = math.radians(lon2 - lon1)
    a = math.sin(dphi / 2) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(dlambda / 2) ** 2
    return 2 * R * math.asin(math.sqrt(a))


async def cleanup_old_positions(pool: asyncpg.Pool) -> None:
    while True:
        await asyncio.sleep(3600)
        try:
            cutoff = datetime.now(timezone.utc) - timedelta(days=10)
            result = await pool.execute("DELETE FROM positions WHERE timestamp < $1", cutoff)
            n = int(result.split()[-1])
            log.info("Deleted %d positions older than 10 days", n)
        except Exception:
            log.exception("Error during position cleanup")


async def ais_stream_task(pool: asyncpg.Pool) -> None:
    uri = "wss://stream.aisstream.io/v0/stream"
    boat_home_state: dict[str, bool] = {}

    while True:
        try:
            log.info("Connecting to AISstream.io...")
            async with websockets.connect(uri, ping_interval=20, ping_timeout=20) as ws:
                mmsi_list = [int(m) for m in tracked_mmsis]
                await ws.send(json.dumps({
                    "APIKey": AISSTREAM_API_KEY,
                    "BoundingBoxes": GLOBAL_BBOX,
                    "FilterMessageTypes": ["PositionReport", "StandardClassBPositionReport"],
                    "MMSI": mmsi_list,
                }))
                log.info("Subscribed to AISstream.io for %d MMSI(s)", len(mmsi_list))

                deadline = asyncio.get_event_loop().time() + RECONNECT_INTERVAL

                while True:
                    remaining = deadline - asyncio.get_event_loop().time()
                    if remaining <= 0:
                        log.info("14-minute reconnect interval reached, reconnecting")
                        break

                    try:
                        raw = await asyncio.wait_for(ws.recv(), timeout=min(remaining, 30))
                    except asyncio.TimeoutError:
                        continue

                    try:
                        msg = json.loads(raw)
                    except json.JSONDecodeError:
                        continue

                    meta = msg.get("MetaData", {})
                    mmsi = str(meta.get("MMSI", ""))
                    ship_name = tracked_mmsis.get(mmsi)
                    if not ship_name:
                        continue

                    lat = meta.get("latitude")
                    lon = meta.get("longitude")
                    if lat is None or lon is None:
                        continue

                    message_type = msg.get("MessageType", "")
                    position = msg.get("Message", {}).get(message_type, {})
                    speed = position.get("Sog")
                    heading = position.get("TrueHeading")
                    if heading == 511:
                        heading = None

                    try:
                        async with pool.acquire() as conn:
                            await conn.execute(
                                "INSERT INTO positions (mmsi, lat, lon, speed, heading) VALUES ($1, $2, $3, $4, $5)",
                                mmsi, float(lat), float(lon),
                                float(speed) if speed is not None else None,
                                float(heading) if heading is not None else None,
                            )

                        # Detect departure / arrival for notifications
                        dist = _nm_dist(float(lat), float(lon), HOME_LAT, HOME_LON)
                        was_home = boat_home_state.get(mmsi, True)
                        is_home = dist < HOME_RADIUS_NM

                        if was_home and not is_home:
                            await pool.execute(
                                "INSERT INTO notifications (type, message, mmsi) VALUES ($1, $2, $3)",
                                "departure", f"{ship_name} departed — {dist:.1f} nm from Point Loma", mmsi,
                            )
                        elif not was_home and is_home:
                            await pool.execute(
                                "INSERT INTO notifications (type, message, mmsi) VALUES ($1, $2, $3)",
                                "arrival", f"{ship_name} returned to dock", mmsi,
                            )

                        boat_home_state[mmsi] = is_home
                        log.info("Saved position for MMSI %s (%s)", mmsi, ship_name)
                    except Exception:
                        log.exception("Error saving position for MMSI=%s", mmsi)

        except asyncio.CancelledError:
            raise
        except Exception:
            log.exception("AISstream WebSocket error, reconnecting in 5s")
            await asyncio.sleep(5)


@asynccontextmanager
async def lifespan(app: FastAPI):
    global db_pool, tracked_mmsis, stream_task
    db_pool = await asyncpg.create_pool(DATABASE_URL, min_size=2, max_size=10)
    await init_db(db_pool)
    cleanup_task: asyncio.Task | None = None

    async with db_pool.acquire() as conn:
        rows = await conn.fetch("SELECT mmsi, name FROM boats WHERE active = true")
        tracked_mmsis = {row["mmsi"]: row["name"] for row in rows}

        # Self-healing color migration: runs every startup, ensures each boat has
        # its palette-assigned color so duplicates can never persist.
        all_boats = await conn.fetch("SELECT mmsi, name FROM boats ORDER BY created_at")
        for i, row in enumerate(all_boats):
            color = COLOR_PALETTE[i % len(COLOR_PALETTE)]
            await conn.execute(
                "UPDATE boats SET color = $1 WHERE mmsi = $2",
                color, row["mmsi"],
            )
            log.info("Assigned %s to boat %s", color, row["name"])

        # Run position cleanup once on startup
        cutoff = datetime.now(timezone.utc) - timedelta(days=10)
        result = await conn.execute("DELETE FROM positions WHERE timestamp < $1", cutoff)
        n = int(result.split()[-1])
        log.info("Deleted %d positions older than 10 days", n)

    stream_task = asyncio.create_task(ais_stream_task(db_pool))
    cleanup_task = asyncio.create_task(cleanup_old_positions(db_pool))
    log.info("AIS stream task started, tracking %d boat(s)", len(tracked_mmsis))

    yield

    for task in (stream_task, cleanup_task):
        if task:
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
    await db_pool.close()


app = FastAPI(title="AIS Tracker", lifespan=lifespan)
app.add_middleware(AuthMiddleware)


# ---------------------------------------------------------------------------
# Auth routes
# ---------------------------------------------------------------------------

@app.get("/login", response_class=HTMLResponse)
async def login_page():
    return HTMLResponse(LOGIN_HTML.format(error=""))


@app.post("/login")
async def login_submit(request: Request):
    form = await request.form()
    username = str(form.get("username", ""))
    password = str(form.get("password", ""))
    if username == ADMIN_USER and password == ADMIN_PASS:
        token = _serializer.dumps(username)
        response = RedirectResponse("/", status_code=303)
        response.set_cookie(SESSION_COOKIE, token, httponly=True, samesite="lax", max_age=86400 * 7)
        return response
    error = '<p class="error">Invalid credentials</p>'
    return HTMLResponse(LOGIN_HTML.format(error=error), status_code=401)


@app.post("/logout")
async def logout():
    response = RedirectResponse("/login", status_code=303)
    response.delete_cookie(SESSION_COOKIE)
    return response


# ---------------------------------------------------------------------------
# App routes
# ---------------------------------------------------------------------------

@app.get("/", response_class=HTMLResponse)
async def serve_index():
    return HTMLResponse(Path("index.html").read_text())


# ---------------------------------------------------------------------------
# Boat API
# ---------------------------------------------------------------------------

class BoatCreate(BaseModel):
    name: str
    mmsi: str
    color: str | None = None
    track_style: str = "solid"


class BoatUpdate(BaseModel):
    name: str | None = None
    color: str | None = None
    active: bool | None = None
    track_style: str | None = None


@app.get("/api/boats")
async def list_boats():
    pool = await get_pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT id, name, mmsi, color, track_style, active, created_at FROM boats ORDER BY created_at"
        )
    return [dict(r) for r in rows]


@app.post("/api/boats", status_code=201)
async def add_boat(boat: BoatCreate):
    pool = await get_pool()
    async with pool.acquire() as conn:
        if boat.color is None:
            used = {row["color"].lower() for row in await conn.fetch("SELECT color FROM boats")}
            color = next(
                (c for c in COLOR_PALETTE if c.lower() not in used),
                COLOR_PALETTE[len(used) % len(COLOR_PALETTE)],
            )
        else:
            color = boat.color
        try:
            row = await conn.fetchrow(
                "INSERT INTO boats (name, mmsi, color, track_style) VALUES ($1, $2, $3, $4) RETURNING id, name, mmsi, color, track_style, active, created_at",
                boat.name, boat.mmsi, color, boat.track_style,
            )
        except asyncpg.UniqueViolationError:
            raise HTTPException(status_code=409, detail="MMSI already registered")
    tracked_mmsis[boat.mmsi] = boat.name
    return dict(row)


@app.put("/api/boats/{mmsi}")
async def update_boat(mmsi: str, update: BoatUpdate):
    fields: list[str] = []
    values: list[object] = []

    if update.name is not None:
        fields.append(f"name = ${len(values) + 1}")
        values.append(update.name)
    if update.color is not None:
        fields.append(f"color = ${len(values) + 1}")
        values.append(update.color)
    if update.active is not None:
        fields.append(f"active = ${len(values) + 1}")
        values.append(update.active)
    if update.track_style is not None:
        fields.append(f"track_style = ${len(values) + 1}")
        values.append(update.track_style)

    if not fields:
        raise HTTPException(status_code=400, detail="No fields to update")

    values.append(mmsi)
    pool = await get_pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            f"UPDATE boats SET {', '.join(fields)} WHERE mmsi = ${len(values)} RETURNING id, name, mmsi, color, track_style, active, created_at",
            *values,
        )
    if not row:
        raise HTTPException(status_code=404, detail="Boat not found")

    if update.active is not None:
        if update.active:
            tracked_mmsis[mmsi] = row["name"]
        else:
            tracked_mmsis.pop(mmsi, None)

    return dict(row)


@app.delete("/api/boats/{mmsi}", status_code=204)
async def delete_boat(mmsi: str):
    pool = await get_pool()
    async with pool.acquire() as conn:
        async with conn.transaction():
            await conn.execute("DELETE FROM positions WHERE mmsi = $1", mmsi)
            result = await conn.execute("DELETE FROM boats WHERE mmsi = $1", mmsi)
    if result == "DELETE 0":
        raise HTTPException(status_code=404, detail="Boat not found")
    tracked_mmsis.pop(mmsi, None)
    return Response(status_code=204)


@app.post("/api/boats/{mmsi}/backfill")
async def backfill_positions(mmsi: str):
    # AISstream free tier doesn't support historical data retrieval
    return {"inserted": 0, "message": "History API not available on free tier"}


# ---------------------------------------------------------------------------
# Position API
# ---------------------------------------------------------------------------

def _parse_dt(s: str) -> datetime | None:
    for fmt in ("%Y-%m-%dT%H:%M:%S", "%Y-%m-%dT%H:%M", "%Y-%m-%d"):
        try:
            return datetime.strptime(s.strip(), fmt).replace(tzinfo=timezone.utc)
        except ValueError:
            continue
    return None


@app.get("/api/positions/{mmsi}")
async def get_positions(
    mmsi: str,
    limit: int = 2000,
    start: str | None = None,
    end: str | None = None,
):
    pool = await get_pool()
    clauses = ["mmsi = $1"]
    params: list = [mmsi]

    if not start and not end:
        # Default: last 7 days
        params.append(datetime.now(timezone.utc) - timedelta(days=7))
        clauses.append(f"timestamp >= ${len(params)}")
    else:
        if start:
            dt = _parse_dt(start)
            if dt:
                params.append(dt)
                clauses.append(f"timestamp >= ${len(params)}")
        if end:
            dt = _parse_dt(end)
            if dt:
                params.append(dt)
                clauses.append(f"timestamp <= ${len(params)}")

    params.append(limit)
    where = " AND ".join(clauses)
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            f"SELECT lat, lon, speed, heading, timestamp FROM positions WHERE {where} ORDER BY timestamp DESC LIMIT ${len(params)}",
            *params,
        )
    return [dict(r) for r in rows]


@app.get("/api/heatmap-positions")
async def get_fleet_positions(hours: int = 24):
    pool = await get_pool()
    since = datetime.now(timezone.utc) - timedelta(hours=hours)
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT mmsi, lat, lon, speed, heading, timestamp FROM positions "
            "WHERE timestamp >= $1 ORDER BY timestamp DESC LIMIT 20000",
            since,
        )
    return [dict(r) for r in rows]


# ---------------------------------------------------------------------------
# Trips API
# ---------------------------------------------------------------------------

@app.get("/api/boats/{mmsi}/trips")
async def get_trips(mmsi: str):
    pool = await get_pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT lat, lon, speed, heading, timestamp FROM positions "
            "WHERE mmsi = $1 ORDER BY timestamp ASC",
            mmsi,
        )

    if not rows:
        return []

    MIN_TRIP_HOURS = 1.5

    trips: list[dict] = []
    trip_start: datetime | None = None
    trip_positions: list[dict] = []

    for row in rows:
        dist = _nm_dist(row["lat"], row["lon"], HOME_LAT, HOME_LON)
        at_home = dist < HOME_RADIUS_NM

        if not at_home:
            if trip_start is None:
                trip_start = row["timestamp"]
                trip_positions = []
            trip_positions.append(dict(row))
        else:
            if trip_start is not None and trip_positions:
                trip_end = trip_positions[-1]["timestamp"]
                duration_h = (trip_end - trip_start).total_seconds() / 3600
                if duration_h >= MIN_TRIP_HOURS:
                    max_range = max(
                        _nm_dist(p["lat"], p["lon"], HOME_LAT, HOME_LON)
                        for p in trip_positions
                    )
                    speeds = [p["speed"] for p in trip_positions if p["speed"] is not None]
                    avg_speed = sum(speeds) / len(speeds) if speeds else 0
                    total_dist = sum(
                        _nm_dist(trip_positions[i]["lat"], trip_positions[i]["lon"],
                                 trip_positions[i-1]["lat"], trip_positions[i-1]["lon"])
                        for i in range(1, len(trip_positions))
                    )
                    trips.append({
                        "index": len(trips),
                        "start": trip_start.isoformat(),
                        "end": trip_end.isoformat(),
                        "duration_h": round(duration_h, 1),
                        "max_range_nm": round(max_range, 1),
                        "total_dist_nm": round(total_dist, 1),
                        "avg_speed_kt": round(avg_speed, 1),
                        "position_count": len(trip_positions),
                        "ongoing": False,
                    })
                trip_start = None
                trip_positions = []

    # Handle ongoing trip
    if trip_start and trip_positions:
        trip_end = trip_positions[-1]["timestamp"]
        duration_h = (trip_end - trip_start).total_seconds() / 3600
        if duration_h >= 0.25:
            max_range = max(
                _nm_dist(p["lat"], p["lon"], HOME_LAT, HOME_LON)
                for p in trip_positions
            )
            speeds = [p["speed"] for p in trip_positions if p["speed"] is not None]
            avg_speed = sum(speeds) / len(speeds) if speeds else 0
            total_dist = sum(
                _nm_dist(trip_positions[i]["lat"], trip_positions[i]["lon"],
                         trip_positions[i-1]["lat"], trip_positions[i-1]["lon"])
                for i in range(1, len(trip_positions))
            )
            trips.append({
                "index": len(trips),
                "start": trip_start.isoformat(),
                "end": None,
                "duration_h": round(duration_h, 1),
                "max_range_nm": round(max_range, 1),
                "total_dist_nm": round(total_dist, 1),
                "avg_speed_kt": round(avg_speed, 1),
                "position_count": len(trip_positions),
                "ongoing": True,
            })

    return list(reversed(trips))


async def _compute_trips_v2(mmsi: str, pool: asyncpg.Pool) -> list[dict]:
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT lat, lon, speed, heading, timestamp FROM positions "
            "WHERE mmsi = $1 ORDER BY timestamp ASC",
            mmsi,
        )

    if not rows:
        return []

    MIN_TRIP_HOURS = 2.0
    trips: list[dict] = []
    trip_start: datetime | None = None
    trip_positions: list[dict] = []

    for row in rows:
        dist = _nm_dist(row["lat"], row["lon"], HOME_LAT, HOME_LON)
        at_home = dist < HOME_RADIUS_NM

        if not at_home:
            if trip_start is None:
                trip_start = row["timestamp"]
                trip_positions = []
            trip_positions.append(dict(row))
        else:
            if trip_start is not None and trip_positions:
                trip_end = trip_positions[-1]["timestamp"]
                duration_h = (trip_end - trip_start).total_seconds() / 3600
                if duration_h >= MIN_TRIP_HOURS:
                    max_dist = max(_nm_dist(p["lat"], p["lon"], HOME_LAT, HOME_LON) for p in trip_positions)
                    total_dist = sum(
                        _nm_dist(trip_positions[i]["lat"], trip_positions[i]["lon"],
                                 trip_positions[i-1]["lat"], trip_positions[i-1]["lon"])
                        for i in range(1, len(trip_positions))
                    )
                    trips.append({
                        "id": len(trips),
                        "start_time": trip_start.isoformat(),
                        "end_time": trip_end.isoformat(),
                        "duration_hours": round(duration_h, 2),
                        "max_distance_nm": round(max_dist, 1),
                        "total_track_nm": round(total_dist, 1),
                    })
                trip_start = None
                trip_positions = []

    if trip_start and trip_positions:
        trip_end = trip_positions[-1]["timestamp"]
        duration_h = (trip_end - trip_start).total_seconds() / 3600
        if duration_h >= MIN_TRIP_HOURS:
            max_dist = max(_nm_dist(p["lat"], p["lon"], HOME_LAT, HOME_LON) for p in trip_positions)
            total_dist = sum(
                _nm_dist(trip_positions[i]["lat"], trip_positions[i]["lon"],
                         trip_positions[i-1]["lat"], trip_positions[i-1]["lon"])
                for i in range(1, len(trip_positions))
            )
            trips.append({
                "id": len(trips),
                "start_time": trip_start.isoformat(),
                "end_time": None,
                "duration_hours": round(duration_h, 2),
                "max_distance_nm": round(max_dist, 1),
                "total_track_nm": round(total_dist, 1),
            })

    return trips


@app.get("/api/trips/{mmsi}")
async def get_trips_by_mmsi(mmsi: str):
    pool = await get_pool()
    trips = await _compute_trips_v2(mmsi, pool)
    return list(reversed(trips))


@app.get("/api/trips/{mmsi}/{trip_id}/track")
async def get_trip_track_by_id(mmsi: str, trip_id: int):
    pool = await get_pool()
    trips = await _compute_trips_v2(mmsi, pool)
    trip = next((t for t in trips if t["id"] == trip_id), None)
    if not trip:
        raise HTTPException(404, "Trip not found")

    start_dt = datetime.fromisoformat(trip["start_time"])
    end_dt = datetime.fromisoformat(trip["end_time"]) if trip["end_time"] else datetime.now(timezone.utc)

    async with pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT lat, lon, speed, heading, timestamp FROM positions "
            "WHERE mmsi = $1 AND timestamp BETWEEN $2 AND $3 ORDER BY timestamp ASC",
            mmsi, start_dt, end_dt,
        )
    return [dict(r) for r in rows]


@app.get("/api/boats/{mmsi}/trips/{trip_index}/gpx")
async def export_trip_gpx(mmsi: str, trip_index: int, start: str, end: str):
    pool = await get_pool()
    start_dt = _parse_dt(start)
    end_dt = _parse_dt(end)
    if not start_dt or not end_dt:
        raise HTTPException(400, "Invalid date range")

    async with pool.acquire() as conn:
        boat = await conn.fetchrow("SELECT name FROM boats WHERE mmsi = $1", mmsi)
        rows = await conn.fetch(
            "SELECT lat, lon, speed, heading, timestamp FROM positions "
            "WHERE mmsi = $1 AND timestamp BETWEEN $2 AND $3 ORDER BY timestamp ASC",
            mmsi, start_dt, end_dt,
        )

    if not rows or not boat:
        raise HTTPException(404, "Trip not found")

    boat_name = boat["name"]
    gpx_lines = [
        '<?xml version="1.0" encoding="UTF-8"?>',
        '<gpx version="1.1" creator="SD Fish Tracker" xmlns="http://www.topografix.com/GPX/1/1">',
        "  <trk>",
        f"    <name>{boat_name} Trip {trip_index + 1}</name>",
        "    <trkseg>",
    ]
    for row in rows:
        ts = row["timestamp"].strftime("%Y-%m-%dT%H:%M:%SZ")
        gpx_lines.append(f'      <trkpt lat="{row["lat"]}" lon="{row["lon"]}"><time>{ts}</time></trkpt>')
    gpx_lines += ["    </trkseg>", "  </trk>", "</gpx>"]

    filename = f"{boat_name.replace(' ', '_')}_trip_{trip_index + 1}.gpx"
    return Response(
        content="\n".join(gpx_lines),
        media_type="application/gpx+xml",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )


# ---------------------------------------------------------------------------
# Hot spots
# ---------------------------------------------------------------------------

@app.get("/api/spots/hot")
async def get_hot_spots(days: int = 7):
    pool = await get_pool()
    since = datetime.now(timezone.utc) - timedelta(days=days)
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT
                ROUND(lat::numeric, 2) AS grid_lat,
                ROUND(lon::numeric, 2) AS grid_lon,
                COUNT(DISTINCT mmsi) AS boat_count,
                COUNT(*) AS hit_count,
                AVG(lat) AS center_lat,
                AVG(lon) AS center_lon,
                MAX(timestamp) AS last_seen
            FROM positions
            WHERE timestamp >= $1 AND speed IS NOT NULL AND speed < 2.5
            GROUP BY ROUND(lat::numeric, 2), ROUND(lon::numeric, 2)
            HAVING COUNT(DISTINCT mmsi) >= 1
            ORDER BY boat_count DESC, hit_count DESC
            LIMIT 20
            """,
            since,
        )
    return [
        {
            "lat": float(r["center_lat"]),
            "lon": float(r["center_lon"]),
            "boat_count": r["boat_count"],
            "hit_count": r["hit_count"],
            "last_seen": r["last_seen"].isoformat() if r["last_seen"] else None,
        }
        for r in rows
    ]


# ---------------------------------------------------------------------------
# Boat status endpoint
# ---------------------------------------------------------------------------

@app.get("/api/boats/{mmsi}/status")
async def get_boat_status(mmsi: str):
    pool = await get_pool()
    now = datetime.now(timezone.utc)

    async with pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT lat, lon, speed, heading, timestamp FROM positions "
            "WHERE mmsi = $1 AND timestamp >= $2 ORDER BY timestamp DESC LIMIT 500",
            mmsi, now - timedelta(hours=2),
        )
        latest_row = rows[0] if rows else await conn.fetchrow(
            "SELECT lat, lon, speed, heading, timestamp FROM positions "
            "WHERE mmsi = $1 ORDER BY timestamp DESC LIMIT 1",
            mmsi,
        )

    if not latest_row:
        return {"status": "No signal", "is_at_dock": False, "last_seen": None, "distance_nm": None}

    lat = latest_row["lat"]
    lon = latest_row["lon"]
    speed = latest_row["speed"] if latest_row["speed"] is not None else 0.0
    heading = latest_row["heading"]
    ts = latest_row["timestamp"]

    dist_to_pl = _nm_dist(lat, lon, HOME_LAT, HOME_LON)
    age_h = (now - ts).total_seconds() / 3600
    time_ago = _fmt_time_ago(ts)

    def _result(status: str, is_at_dock: bool = False) -> dict:
        return {
            "status": status,
            "is_at_dock": is_at_dock,
            "last_seen": ts.isoformat(),
            "distance_nm": round(dist_to_pl, 1),
        }

    if age_h >= 6:
        return _result(f"No signal {time_ago}")

    if dist_to_pl < 2.0 and speed < 0.5:
        return _result("At dock", is_at_dock=True)

    if speed > 12:
        brg_to_pl = _bearing_to(lat, lon, HOME_LAT, HOME_LON)
        if heading is not None:
            angle_diff = abs(((heading - brg_to_pl) + 180) % 360 - 180)
            if angle_diff <= 90:
                eta = _fmt_eta(dist_to_pl / speed)
                return _result(f"Returning to harbor, {eta}")
        compass = _compass_dir(heading if heading is not None else 0.0)
        return _result(f"Running {compass} at {speed:.0f} kt")

    if speed >= 6:
        compass = _compass_dir(heading if heading is not None else 0.0)
        return _result(f"Cruising {compass} at {speed:.1f} kt")

    if speed >= 2 and heading is not None:
        compass = _compass_dir(heading)
        return _result(f"Trolling {compass}")

    # speed < 2 kt: find how long the slow period has lasted
    slow_start = ts
    for row in rows:
        spd = row["speed"] if row["speed"] is not None else 0.0
        if spd < 2.0:
            slow_start = row["timestamp"]
        else:
            break

    slow_min = (now - slow_start).total_seconds() / 60

    if slow_min >= 20:
        dur = _fmt_duration(slow_min)
        bank = _nearest_bank(lat, lon, max_nm=1.0)
        status_str = f"Working a spot for {dur}"
        if bank:
            status_str += f" near {bank}"
        return _result(status_str)

    bank = _nearest_bank(lat, lon, max_nm=2.0)
    if bank:
        return _result(f"Slowing down, near {bank}")

    return _result("Idle")


# ---------------------------------------------------------------------------
# Hot spots endpoint
# ---------------------------------------------------------------------------

@app.get("/api/hotspots")
async def get_hotspots():
    pool = await get_pool()
    now = datetime.now(timezone.utc)
    since = now - timedelta(hours=72)

    async with pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT p.mmsi, p.lat, p.lon, p.speed, p.timestamp,
                   b.name AS boat_name, b.color AS boat_color
            FROM positions p
            JOIN boats b ON b.mmsi = p.mmsi
            WHERE p.timestamp >= $1 AND p.speed IS NOT NULL AND p.speed < 2.0
            ORDER BY p.mmsi, p.timestamp ASC
            """,
            since,
        )

    by_mmsi: dict[str, list] = {}
    for row in rows:
        by_mmsi.setdefault(row["mmsi"], []).append(dict(row))

    hotspots = []
    for mmsi, positions in by_mmsi.items():
        boat_name = positions[0]["boat_name"]
        boat_color = positions[0]["boat_color"]

        clusters: list[list] = []
        current: list = [positions[0]]

        for pos in positions[1:]:
            prev = current[-1]
            dist = _nm_dist(prev["lat"], prev["lon"], pos["lat"], pos["lon"])
            gap_min = (pos["timestamp"] - prev["timestamp"]).total_seconds() / 60
            if dist <= 0.5 and gap_min <= 120:
                current.append(pos)
            else:
                clusters.append(current)
                current = [pos]
        clusters.append(current)

        for cluster in clusters:
            if len(cluster) < 2:
                continue
            start_t = cluster[0]["timestamp"]
            end_t = cluster[-1]["timestamp"]
            duration_min = (end_t - start_t).total_seconds() / 60
            if duration_min < 60:
                continue

            center_lat = sum(p["lat"] for p in cluster) / len(cluster)
            center_lon = sum(p["lon"] for p in cluster) / len(cluster)
            dist_pl = _nm_dist(center_lat, center_lon, HOME_LAT, HOME_LON)
            nearest_bank = _nearest_bank(center_lat, center_lon, max_nm=2.0)

            hotspots.append({
                "lat": round(center_lat, 5),
                "lon": round(center_lon, 5),
                "boat_name": boat_name,
                "boat_color": boat_color,
                "start_time": start_t.isoformat(),
                "end_time": end_t.isoformat(),
                "duration_minutes": round(duration_min),
                "nearest_bank_name": nearest_bank,
                "distance_from_point_loma_nm": round(dist_pl, 1),
            })

    hotspots.sort(key=lambda h: h["end_time"], reverse=True)
    return hotspots


# ---------------------------------------------------------------------------
# Notifications API
# ---------------------------------------------------------------------------

class NotificationCreate(BaseModel):
    type: str = "info"
    message: str
    mmsi: str | None = None
    data: dict[str, Any] = {}


@app.get("/api/notifications")
async def list_notifications(limit: int = 50):
    pool = await get_pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT id, type, message, mmsi, data, read, created_at FROM notifications "
            "ORDER BY created_at DESC LIMIT $1",
            limit,
        )
    return [dict(r) for r in rows]


@app.post("/api/notifications", status_code=201)
async def create_notification(notif: NotificationCreate):
    pool = await get_pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            "INSERT INTO notifications (type, message, mmsi, data) VALUES ($1, $2, $3, $4) "
            "RETURNING id, type, message, mmsi, data, read, created_at",
            notif.type, notif.message, notif.mmsi, json.dumps(notif.data),
        )
    return dict(row)


@app.patch("/api/notifications/{notif_id}/read")
async def mark_notification_read(notif_id: int):
    pool = await get_pool()
    async with pool.acquire() as conn:
        await conn.execute("UPDATE notifications SET read = TRUE WHERE id = $1", notif_id)
    return {"ok": True}


@app.delete("/api/notifications/read", status_code=204)
async def clear_read_notifications():
    pool = await get_pool()
    async with pool.acquire() as conn:
        await conn.execute("DELETE FROM notifications WHERE read = TRUE")
    return Response(status_code=204)


# ---------------------------------------------------------------------------
# Saved points API
# ---------------------------------------------------------------------------

class PointCreate(BaseModel):
    lat: float
    lon: float
    name: str = ""
    notes: str = ""


@app.post("/api/points", status_code=201)
async def create_point(point: PointCreate):
    pool = await get_pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            "INSERT INTO saved_points (lat, lon, name, notes) VALUES ($1, $2, $3, $4)"
            " RETURNING id, lat, lon, name, notes, created_at",
            point.lat, point.lon, point.name, point.notes,
        )
    return dict(row)


@app.get("/api/points")
async def list_points():
    pool = await get_pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT id, lat, lon, name, notes, created_at FROM saved_points ORDER BY created_at DESC"
        )
    return [dict(r) for r in rows]


# ---------------------------------------------------------------------------
# Preferences API
# ---------------------------------------------------------------------------

class PrefUpdate(BaseModel):
    key: str
    value: Any


@app.get("/api/prefs")
async def get_prefs():
    pool = await get_pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch("SELECT key, value FROM user_preferences")
    return {r["key"]: r["value"] for r in rows}


@app.put("/api/prefs")
async def set_pref(update: PrefUpdate):
    pool = await get_pool()
    async with pool.acquire() as conn:
        await conn.execute(
            "INSERT INTO user_preferences (key, value, updated_at) VALUES ($1, $2, NOW()) "
            "ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value, updated_at = NOW()",
            update.key, json.dumps(update.value),
        )
    return {"ok": True}


# ---------------------------------------------------------------------------
# Proxy: NDBC buoy data (avoids CORS issues in some browsers)
# ---------------------------------------------------------------------------

@app.get("/api/proxy/ndbc/{station}")
async def proxy_ndbc(station: str):
    if not station.isalnum():
        raise HTTPException(400, "Invalid station ID")
    url = f"https://www.ndbc.noaa.gov/data/realtime2/{station}.txt"
    try:
        loop = asyncio.get_event_loop()
        text = await loop.run_in_executor(
            None,
            lambda: urllib.request.urlopen(url, timeout=10).read().decode("utf-8", errors="replace"),
        )
        return Response(content=text, media_type="text/plain")
    except Exception as exc:
        raise HTTPException(502, f"Failed to fetch buoy data: {exc}")
