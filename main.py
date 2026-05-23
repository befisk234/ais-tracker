import asyncio
import logging
import os
from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone
from pathlib import Path

import asyncpg
import httpx
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
AISHUB_USERNAME = os.getenv("AISHUB_USERNAME", "")
ADMIN_USER = os.getenv("ADMIN_USER", "admin")
ADMIN_PASS = os.getenv("ADMIN_PASS", "changeme")
SECRET_KEY = os.getenv("SECRET_KEY", "dev-secret-change-in-production")
SESSION_COOKIE = "ais_session"

_serializer = URLSafeTimedSerializer(SECRET_KEY)
db_pool: asyncpg.Pool | None = None
tracked_mmsis: set[str] = set()
stream_task: asyncio.Task | None = None


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


def schedule_stream_restart() -> None:
    async def _restart() -> None:
        global stream_task, db_pool
        old = stream_task
        if db_pool is not None:
            stream_task = asyncio.create_task(ais_poll_task(db_pool))
        if old and not old.done():
            old.cancel()
    asyncio.create_task(_restart())


async def ais_poll_task(pool: asyncpg.Pool) -> None:
    """Poll AISHub HTTP API every 60 seconds (their rate limit is once per minute)."""
    url = "https://data.aishub.net/ws.php"
    while True:
        mmsis = list(tracked_mmsis)
        if not mmsis:
            await asyncio.sleep(60)
            continue

        if not AISHUB_USERNAME:
            log.error("AISHUB_USERNAME not set — AIS polling disabled")
            await asyncio.sleep(60)
            continue

        try:
            log.info("Polling AISHub for MMSIs: %s", mmsis)
            async with httpx.AsyncClient(timeout=30.0) as client:
                resp = await client.get(url, params={
                    "username": AISHUB_USERNAME,
                    "format": "1",
                    "output": "json",
                    "compress": "0",
                    "mmsi": ",".join(mmsis),
                })
                resp.raise_for_status()
                data = resp.json()

            # Response is [metadata_header, vessel1, vessel2, ...]
            if not isinstance(data, list) or len(data) < 2:
                log.info("AISHub: no vessel data in response")
            else:
                async with pool.acquire() as conn:
                    for vessel in data[1:]:
                        try:
                            mmsi = str(vessel.get("MMSI", ""))
                            lat = vessel.get("LATITUDE")
                            lon = vessel.get("LONGITUDE")
                            if not mmsi or lat is None or lon is None:
                                continue
                            speed = vessel.get("SOG")
                            heading = vessel.get("HEADING")
                            # 511 is the AIS "not available" sentinel for heading
                            if heading == 511:
                                heading = None
                            await conn.execute(
                                "INSERT INTO positions (mmsi, lat, lon, speed, heading) VALUES ($1, $2, $3, $4, $5)",
                                mmsi, float(lat), float(lon),
                                float(speed) if speed is not None else None,
                                float(heading) if heading is not None else None,
                            )
                            log.info("Position saved: MMSI=%s lat=%.4f lon=%.4f speed=%s", mmsi, lat, lon, speed)
                        except Exception:
                            log.exception("Error processing vessel record")
        except asyncio.CancelledError:
            raise
        except Exception:
            log.exception("AISHub poll error, retrying in 60s")

        await asyncio.sleep(60)


@asynccontextmanager
async def lifespan(app: FastAPI):
    global db_pool, tracked_mmsis, stream_task
    db_pool = await asyncpg.create_pool(DATABASE_URL, min_size=2, max_size=10)
    await init_db(db_pool)

    async with db_pool.acquire() as conn:
        rows = await conn.fetch("SELECT mmsi FROM boats WHERE active = true")
        tracked_mmsis = {row["mmsi"] for row in rows}

    stream_task = asyncio.create_task(ais_poll_task(db_pool))
    log.info("AIS poll task started for MMSIs: %s", tracked_mmsis)

    yield

    if stream_task:
        stream_task.cancel()
        try:
            await stream_task
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
    color: str = "#3388ff"


class BoatUpdate(BaseModel):
    name: str | None = None
    color: str | None = None
    active: bool | None = None


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
                "INSERT INTO boats (name, mmsi, color) VALUES ($1, $2, $3) RETURNING id, name, mmsi, color, active, created_at",
                boat.name, boat.mmsi, boat.color,
            )
        except asyncpg.UniqueViolationError:
            raise HTTPException(status_code=409, detail="MMSI already registered")
    tracked_mmsis.add(boat.mmsi)
    schedule_stream_restart()
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

    if not fields:
        raise HTTPException(status_code=400, detail="No fields to update")

    values.append(mmsi)
    pool = await get_pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            f"UPDATE boats SET {', '.join(fields)} WHERE mmsi = ${len(values)} RETURNING id, name, mmsi, color, active, created_at",
            *values,
        )
    if not row:
        raise HTTPException(status_code=404, detail="Boat not found")

    if update.active is not None:
        if update.active:
            tracked_mmsis.add(mmsi)
        else:
            tracked_mmsis.discard(mmsi)
        schedule_stream_restart()

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
    tracked_mmsis.discard(mmsi)
    schedule_stream_restart()
    return Response(status_code=204)


@app.post("/api/boats/{mmsi}/backfill")
async def backfill_positions(mmsi: str):
    pool = await get_pool()
    async with pool.acquire() as conn:
        boat = await conn.fetchrow("SELECT id FROM boats WHERE mmsi = $1", mmsi)
    if not boat:
        raise HTTPException(status_code=404, detail="Boat not found")

    since = (datetime.now(timezone.utc) - timedelta(hours=24)).strftime("%Y-%m-%dT%H:%M:%SZ")
    url = f"https://api.aisstream.io/v0/vessel/{mmsi}/track"

    try:
        async with httpx.AsyncClient(timeout=30.0) as client:
            resp = await client.get(
                url,
                headers={"Authorization": f"Bearer {AISSTREAM_API_KEY}"},
                params={"start": since},
            )
            if resp.status_code in (401, 402, 403):
                return {"inserted": 0, "message": "Track history not available on current plan"}
            if resp.status_code == 404:
                return {"inserted": 0, "message": "No track data found"}
            resp.raise_for_status()
            data = resp.json()
    except httpx.HTTPStatusError as e:
        raise HTTPException(status_code=502, detail=f"AISStream error: {e.response.status_code}")
    except httpx.RequestError as e:
        raise HTTPException(status_code=502, detail=f"AISStream connection error: {str(e)}")

    inserted = 0
    points = data.get("positions") or data.get("points") or []
    async with pool.acquire() as conn:
        for point in points:
            try:
                ts = point.get("timestamp") or point.get("time")
                await conn.execute(
                    "INSERT INTO positions (mmsi, lat, lon, speed, heading, timestamp) VALUES ($1, $2, $3, $4, $5, $6)",
                    mmsi,
                    float(point["lat"]),
                    float(point["lon"]),
                    float(point["speed"]) if point.get("speed") is not None else None,
                    float(point["heading"]) if point.get("heading") is not None else None,
                    ts,
                )
                inserted += 1
            except Exception:
                log.exception("Error inserting backfill point")

    return {"inserted": inserted}


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
    limit: int = 500,
    start: str | None = None,
    end: str | None = None,
):
    pool = await get_pool()
    clauses = ["mmsi = $1"]
    params: list = [mmsi]

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
