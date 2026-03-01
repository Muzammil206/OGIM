-- Step 1: Enable PostGIS extension (run this once)
CREATE EXTENSION IF NOT EXISTS postgis;

-- Step 2: Create a unified table for all GeoJSON features
CREATE TABLE IF NOT EXISTS geojson_features (
    id            SERIAL PRIMARY KEY,
    source_file   TEXT,                        -- which .geojson file it came from
    feature_type  TEXT,                        -- Point, Polygon, LineString, etc.
    properties    JSONB,                       -- all GeoJSON feature properties
    geometry      GEOMETRY(Geometry, 4326),    -- spatial column, WGS84 (standard for GeoJSON)
    created_at    TIMESTAMPTZ DEFAULT NOW()
);

-- Step 3: Create spatial index for fast map/query performance
CREATE INDEX IF NOT EXISTS idx_geojson_geometry
    ON geojson_features USING GIST (geometry);

-- Step 4: Index on properties for fast JSON queries
CREATE INDEX IF NOT EXISTS idx_geojson_properties
    ON geojson_features USING GIN (properties);

-- Step 5: Index on source file for filtering by file
CREATE INDEX IF NOT EXISTS idx_geojson_source_file
    ON geojson_features (source_file);