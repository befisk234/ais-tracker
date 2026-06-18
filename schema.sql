CREATE TABLE IF NOT EXISTS boats (
    id SERIAL PRIMARY KEY,
    name TEXT NOT NULL,
    mmsi TEXT NOT NULL UNIQUE,
    color TEXT NOT NULL DEFAULT '#3388ff',
    track_style TEXT NOT NULL DEFAULT 'solid',
    active BOOLEAN NOT NULL DEFAULT TRUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

ALTER TABLE boats ADD COLUMN IF NOT EXISTS track_style TEXT NOT NULL DEFAULT 'solid';

CREATE TABLE IF NOT EXISTS positions (
    id SERIAL PRIMARY KEY,
    mmsi TEXT NOT NULL,
    lat DOUBLE PRECISION NOT NULL,
    lon DOUBLE PRECISION NOT NULL,
    speed DOUBLE PRECISION,
    heading DOUBLE PRECISION,
    timestamp TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS positions_mmsi_timestamp_idx ON positions (mmsi, timestamp DESC);
CREATE INDEX IF NOT EXISTS positions_timestamp_idx ON positions (timestamp);

CREATE TABLE IF NOT EXISTS saved_points (
    id SERIAL PRIMARY KEY,
    lat DOUBLE PRECISION NOT NULL,
    lon DOUBLE PRECISION NOT NULL,
    name TEXT NOT NULL DEFAULT '',
    notes TEXT NOT NULL DEFAULT '',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Notifications (Part K)
CREATE TABLE IF NOT EXISTS notifications (
    id SERIAL PRIMARY KEY,
    type TEXT NOT NULL DEFAULT 'info',
    message TEXT NOT NULL,
    mmsi TEXT,
    data JSONB DEFAULT '{}',
    read BOOLEAN NOT NULL DEFAULT FALSE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS notifications_created_at_idx ON notifications (created_at DESC);

-- Detected fishing spots (Part B)
CREATE TABLE IF NOT EXISTS detected_spots (
    id SERIAL PRIMARY KEY,
    mmsi TEXT NOT NULL,
    lat DOUBLE PRECISION NOT NULL,
    lon DOUBLE PRECISION NOT NULL,
    duration_min INTEGER NOT NULL,
    started_at TIMESTAMPTZ NOT NULL,
    ended_at TIMESTAMPTZ NOT NULL,
    activity_type TEXT NOT NULL DEFAULT 'fishing',
    bank_name TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS detected_spots_mmsi_idx ON detected_spots (mmsi, started_at DESC);

-- User preferences
CREATE TABLE IF NOT EXISTS user_preferences (
    key TEXT NOT NULL PRIMARY KEY,
    value JSONB NOT NULL DEFAULT '{}',
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
