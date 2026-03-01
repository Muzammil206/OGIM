/**
 * OGIM — Bun API Server
 * Serves GeoJSON from Neon PostGIS for the MapLibre frontend
 *
 * Usage:
 *   DATABASE_URL="postgresql://..." bun server.ts
 *
 * Endpoints:
 *   GET /api/layers                     — list all available layers + feature counts
 *   GET /api/layers/:name               — full GeoJSON FeatureCollection for a layer
 *   GET /api/layers/:name?bbox=w,s,e,n  — spatially filtered by bounding box
 *   GET /api/search?q=term&field=name   — search features by property
 *   GET /health                         — health check
 */

import postgres from "postgres";

// ─── DB ───────────────────────────────────────────────────────────────────

const DATABASE_URL = process.env.DATABASE_URL;

if (!DATABASE_URL) {
  console.error("❌  DATABASE_URL env variable is required");
  process.exit(1);
}

const sql = postgres(DATABASE_URL, {
  ssl: "require",
  max: 10,
  idle_timeout: 30,
});

// ─── HELPERS ──────────────────────────────────────────────────────────────

// Map source_file → friendly layer name
const FILE_TO_LAYER: Record<string, string> = {
  "NG_OGIM_pipelines.geojson":           "pipelines",
  "NG_OGIM_lng_facilities.geojson":      "lng_facilities",
  "NG_OGIM_refineries.geojson":          "refineries",
  "NG_OGIM_wells.geojson":               "wells",
  "NG_OGIM_compressor_stations.geojson": "compressor_stations",
  "NG_OGIM_flaring_detections.geojson":  "flaring_detections",
  "NG_OGIM_petroleum_terminals.geojson": "petroleum_terminals",
};

const LAYER_TO_FILE: Record<string, string> = Object.fromEntries(
  Object.entries(FILE_TO_LAYER).map(([k, v]) => [v, k])
);

function json(data: unknown, status = 200) {
  return new Response(JSON.stringify(data), {
    status,
    headers: {
      "Content-Type": "application/json",
      "Access-Control-Allow-Origin": "*", // allow browser fetch
    },
  });
}

function error(msg: string, status = 400) {
  return json({ error: msg }, status);
}

// Build a GeoJSON FeatureCollection from DB rows
function toFeatureCollection(rows: any[]) {
  return {
    type: "FeatureCollection",
    features: rows.map((row) => ({
      type: "Feature",
      id: row.id,
      geometry: JSON.parse(row.geometry),
      properties: {
        ...row.properties,
        _id: row.id,
        _source: row.source_file,
        _type: row.feature_type,
      },
    })),
  };
}

// ─── ROUTES ───────────────────────────────────────────────────────────────

async function handleLayers() {
  const rows = await sql`
    SELECT
      source_file,
      feature_type,
      COUNT(*)::int AS count
    FROM geojson_features
    GROUP BY source_file, feature_type
    ORDER BY source_file
  `;

  // Group by source_file
  const grouped: Record<string, any> = {};
  for (const row of rows) {
    const name = FILE_TO_LAYER[row.source_file] || row.source_file;
    if (!grouped[name]) {
      grouped[name] = { name, source_file: row.source_file, types: [], total: 0 };
    }
    grouped[name].types.push({ geometry_type: row.feature_type, count: row.count });
    grouped[name].total += row.count;
  }

  return json({ layers: Object.values(grouped) });
}

async function handleLayerData(layerName: string, bbox?: string) {
  const sourceFile = LAYER_TO_FILE[layerName];
  if (!sourceFile) return error(`Unknown layer: ${layerName}`, 404);

  let rows;

  if (bbox) {
    // Validate bbox: west,south,east,north
    const parts = bbox.split(",").map(Number);
    if (parts.length !== 4 || parts.some(isNaN)) {
      return error("Invalid bbox. Expected: west,south,east,north");
    }
    const [west, south, east, north] = parts;

    rows = await sql`
      SELECT
        id,
        source_file,
        feature_type,
        properties,
        ST_AsGeoJSON(geometry)::text AS geometry
      FROM geojson_features
      WHERE source_file = ${sourceFile}
        AND geometry && ST_MakeEnvelope(${west}, ${south}, ${east}, ${north}, 4326)
      ORDER BY id
    `;
  } else {
    rows = await sql`
      SELECT
        id,
        source_file,
        feature_type,
        properties,
        ST_AsGeoJSON(geometry)::text AS geometry
      FROM geojson_features
      WHERE source_file = ${sourceFile}
      ORDER BY id
    `;
  }

  return json(toFeatureCollection(rows));
}

async function handleSearch(q: string, field: string, layerName?: string) {
  if (!q || q.length < 2) return error("Query must be at least 2 characters");

  const pattern = `%${q}%`;

  let rows;
  if (layerName) {
    const sourceFile = LAYER_TO_FILE[layerName];
    if (!sourceFile) return error(`Unknown layer: ${layerName}`, 404);

    rows = await sql`
      SELECT
        id,
        source_file,
        feature_type,
        properties,
        ST_AsGeoJSON(geometry)::text AS geometry
      FROM geojson_features
      WHERE source_file = ${sourceFile}
        AND properties->>${field} ILIKE ${pattern}
      LIMIT 50
    `;
  } else {
    rows = await sql`
      SELECT
        id,
        source_file,
        feature_type,
        properties,
        ST_AsGeoJSON(geometry)::text AS geometry
      FROM geojson_features
      WHERE properties->>${field} ILIKE ${pattern}
      LIMIT 50
    `;
  }

  return json(toFeatureCollection(rows));
}

// ─── ROUTER ───────────────────────────────────────────────────────────────

Bun.serve({
  port: process.env.PORT || 3000,

  async fetch(req) {
    const url = new URL(req.url);
    const path = url.pathname;

    // CORS preflight
    if (req.method === "OPTIONS") {
      return new Response(null, {
        headers: {
          "Access-Control-Allow-Origin": "*",
          "Access-Control-Allow-Methods": "GET, OPTIONS",
          "Access-Control-Allow-Headers": "Content-Type",
        },
      });
    }

    try {
      // GET /health
      if (path === "/health") {
        await sql`SELECT 1`;
        return json({ status: "ok", db: "connected", timestamp: new Date().toISOString() });
      }

      // GET /api/layers
      if (path === "/api/layers" && req.method === "GET") {
        return await handleLayers();
      }

      // GET /api/layers/:name
      const layerMatch = path.match(/^\/api\/layers\/([a-z_]+)$/);
      if (layerMatch && req.method === "GET") {
        const layerName = layerMatch[1];
        const bbox = url.searchParams.get("bbox") || undefined;
        return await handleLayerData(layerName, bbox);
      }

      // GET /api/search?q=...&field=...&layer=...
      if (path === "/api/search" && req.method === "GET") {
        const q = url.searchParams.get("q") || "";
        const field = url.searchParams.get("field") || "name";
        const layer = url.searchParams.get("layer") || undefined;
        return await handleSearch(q, field, layer);
      }

      return error("Not found", 404);

    } catch (err: any) {
      console.error("Server error:", err);
      return error(`Internal server error: ${err.message}`, 500);
    }
  },
});

console.log(`
╔══════════════════════════════════════════╗
║   OGIM API SERVER — Bun + Neon PostGIS   ║
╠══════════════════════════════════════════╣
║  http://localhost:${process.env.PORT || 3000}                   ║
╠══════════════════════════════════════════╣
║  GET /api/layers                         ║
║  GET /api/layers/:name                   ║
║  GET /api/layers/:name?bbox=w,s,e,n      ║
║  GET /api/search?q=...&field=...         ║
║  GET /health                             ║
╚══════════════════════════════════════════╝
`);