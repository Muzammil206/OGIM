#!/usr/bin/env bun

/**
 * GeoJSON → Neon (PostGIS) Importer — Bun edition
 *
 * Usage:
 *   bun import.ts --dir ./geojson --connection "postgresql://user:pass@host/db"
 *
 * Or set env variable:
 *   DATABASE_URL="postgresql://..." bun import.ts --dir ./geojson
 *
 * Install dependencies first:
 *   bun add postgres
 */

import postgres from "postgres";
import fs from "fs";
import path from "path";

// ─── Config ────────────────────────────────────────────────────────────────

const args = process.argv.slice(2);
const getArg = (flag: string) => {
  const i = args.indexOf(flag);
  return i !== -1 ? args[i + 1] : null;
};

const GEOJSON_DIR = getArg("--dir") || "./geojson";
const CONNECTION_STRING = getArg("--connection") || process.env.DATABASE_URL;
const BATCH_SIZE = parseInt(getArg("--batch") ?? "100", 10);

if (!CONNECTION_STRING) {
  console.error("❌  No connection string provided.");
  console.error(
    '   Use --connection "postgresql://..." or set DATABASE_URL env var.'
  );
  process.exit(1);
}

// ─── DB Client (postgres.js — works perfectly with Bun) ────────────────────

const sql = postgres(CONNECTION_STRING, {
  ssl: "require", // required for Neon
  max: 5,
});

// ─── Types ─────────────────────────────────────────────────────────────────

interface GeoJSONGeometry {
  type: string;
  coordinates: unknown;
}

interface GeoJSONFeature {
  type: "Feature";
  geometry: GeoJSONGeometry | null;
  properties: Record<string, unknown> | null;
}

interface GeoJSONFeatureCollection {
  type: "FeatureCollection";
  features: GeoJSONFeature[];
}

interface ImportRow {
  sourceFile: string;
  featureType: string;
  properties: Record<string, unknown>;
  geometry: GeoJSONGeometry;
}

// ─── Helpers ───────────────────────────────────────────────────────────────

function readGeoJSONFiles(dir: string): string[] {
  if (!fs.existsSync(dir)) {
    throw new Error(`Directory not found: ${dir}`);
  }
  return fs
    .readdirSync(dir)
    .filter((f) => f.endsWith(".geojson") || f.endsWith(".json"))
    .map((f) => path.join(dir, f));
}

function parseGeoJSON(filePath: string): GeoJSONFeature[] {
  const raw = fs.readFileSync(filePath, "utf-8");
  const data = JSON.parse(raw) as GeoJSONFeature | GeoJSONFeatureCollection | GeoJSONGeometry;

  if (data.type === "FeatureCollection") {
    return (data as GeoJSONFeatureCollection).features || [];
  } else if (data.type === "Feature") {
    return [data as GeoJSONFeature];
  } else {
    // Bare geometry — wrap it
    return [{ type: "Feature", geometry: data as GeoJSONGeometry, properties: {} }];
  }
}

async function insertBatch(rows: ImportRow[]): Promise<void> {
  if (rows.length === 0) return;

  // Build values array for postgres.js bulk insert
  const values = rows.map((r) => ({
    source_file: r.sourceFile,
    feature_type: r.featureType,
    properties: r.properties,
    geometry: JSON.stringify(r.geometry),
  }));

  await sql`
    INSERT INTO geojson_features (source_file, feature_type, properties, geometry)
    SELECT
      v.source_file,
      v.feature_type,
      v.properties::jsonb,
      ST_SetSRID(ST_GeomFromGeoJSON(v.geometry), 4326)
    FROM (
      SELECT
        unnest(${values.map((v) => v.source_file)}::text[])       AS source_file,
        unnest(${values.map((v) => v.feature_type)}::text[])      AS feature_type,
        unnest(${values.map((v) => JSON.stringify(v.properties))}::text[]) AS properties,
        unnest(${values.map((v) => v.geometry)}::text[])          AS geometry
    ) v
  `;
}

// ─── Main ──────────────────────────────────────────────────────────────────

async function main() {
  console.log("🔌  Connecting to Neon...");

  // Test connection
  await sql`SELECT 1`;
  console.log("✅  Connected.\n");

  const files = readGeoJSONFiles(GEOJSON_DIR);
  if (files.length === 0) {
    console.warn(`⚠️  No .geojson files found in: ${GEOJSON_DIR}`);
    process.exit(0);
  }

  console.log(`📂  Found ${files.length} file(s) to import:\n`);
  files.forEach((f) => console.log(`   • ${path.basename(f)}`));
  console.log();

  let totalInserted = 0;
  let totalSkipped = 0;

  for (const filePath of files) {
    const fileName = path.basename(filePath);
    console.log(`⏳  Processing: ${fileName}`);

    let features: GeoJSONFeature[];
    try {
      features = parseGeoJSON(filePath);
    } catch (err) {
      console.error(`   ❌  Failed to parse ${fileName}: ${(err as Error).message}`);
      continue;
    }

    console.log(`   📍  ${features.length} feature(s) found`);

    const rows: ImportRow[] = [];
    let skipped = 0;

    for (const feature of features) {
      if (!feature.geometry) {
        skipped++;
        continue;
      }
      rows.push({
        sourceFile: fileName,
        featureType: feature.geometry.type,
        properties: feature.properties || {},
        geometry: feature.geometry,
      });
    }

    // Insert in batches
    for (let i = 0; i < rows.length; i += BATCH_SIZE) {
      const batch = rows.slice(i, i + BATCH_SIZE);
      try {
        await insertBatch(batch);
        process.stdout.write(
          `\r   ✅  Inserted ${Math.min(i + BATCH_SIZE, rows.length)} / ${rows.length}`
        );
      } catch (err) {
        console.error(`\n   ❌  Batch insert failed: ${(err as Error).message}`);
        throw err;
      }
    }

    console.log(
      `\n   ✅  Done — ${rows.length} inserted, ${skipped} skipped (null geometry)\n`
    );
    totalInserted += rows.length;
    totalSkipped += skipped;
  }

  console.log("─".repeat(50));
  console.log(`🎉  Import complete!`);
  console.log(`   Total inserted : ${totalInserted}`);
  console.log(`   Total skipped  : ${totalSkipped}`);
  console.log("─".repeat(50));

  await sql.end();
}

main().catch(async (err) => {
  console.error("\n❌  Fatal error:", err.message);
  await sql.end().catch(() => {});
  process.exit(1);
});