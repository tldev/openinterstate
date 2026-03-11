use sqlx::PgPool;

/// Import exits in bulk for PBF extract ingestion.
/// Clears existing exits first, then inserts in a transaction.
pub async fn import_exits(
    pool: &PgPool,
    exits: &[crate::parser::ParsedExit],
    replace_existing: bool,
) -> Result<(), anyhow::Error> {
    if replace_existing {
        // Clear dependent tables first to satisfy FKs before replacing exits.
        sqlx::query("DELETE FROM exit_corridors")
            .execute(pool)
            .await?;
        sqlx::query("DELETE FROM highway_edges")
            .execute(pool)
            .await?;
        sqlx::query("DELETE FROM exit_poi_candidates")
            .execute(pool)
            .await?;
        sqlx::query("DELETE FROM exits").execute(pool).await?;
    }

    let mut tx = pool.begin().await?;
    for (i, exit) in exits.iter().enumerate() {
        sqlx::query(
            "INSERT INTO exits (id, osm_type, osm_id, state, ref, name, highway, direction, geom, tags_json) \
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8, ST_SetSRID(ST_MakePoint($9, $10), 4326), $11::jsonb) \
             ON CONFLICT (id) DO UPDATE SET \
               ref = EXCLUDED.ref, name = EXCLUDED.name, highway = EXCLUDED.highway, \
               direction = EXCLUDED.direction, geom = EXCLUDED.geom, tags_json = EXCLUDED.tags_json"
        )
        .bind(&exit.id)
        .bind(&exit.osm_type)
        .bind(exit.osm_id)
        .bind(&exit.state)
        .bind(&exit.r#ref)
        .bind(&exit.name)
        .bind(&exit.highway)
        .bind(&exit.direction)
        .bind(exit.lon)
        .bind(exit.lat)
        .bind(&exit.tags_json)
        .execute(&mut *tx)
        .await?;

        if (i + 1) % 10000 == 0 {
            tracing::info!("  exits: {}/{}", i + 1, exits.len());
        }
    }
    tx.commit().await?;
    tracing::info!("  Imported {} exits", exits.len());
    Ok(())
}

/// Import POIs in bulk for PBF extract ingestion.
/// Clears existing POIs first, then inserts in a transaction.
pub async fn import_pois(
    pool: &PgPool,
    pois: &[crate::parser::ParsedPOI],
    replace_existing: bool,
) -> Result<(), anyhow::Error> {
    if replace_existing {
        sqlx::query("DELETE FROM pois").execute(pool).await?;
    }

    let mut tx = pool.begin().await?;
    for (i, poi) in pois.iter().enumerate() {
        sqlx::query(
            "INSERT INTO pois (id, osm_type, osm_id, state, category, name, display_name, brand, geom, tags_json) \
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8, ST_SetSRID(ST_MakePoint($9, $10), 4326), $11::jsonb) \
             ON CONFLICT (id) DO UPDATE SET \
               category = EXCLUDED.category, name = EXCLUDED.name, display_name = EXCLUDED.display_name, \
               brand = EXCLUDED.brand, geom = EXCLUDED.geom, tags_json = EXCLUDED.tags_json"
        )
        .bind(&poi.id)
        .bind(&poi.osm_type)
        .bind(poi.osm_id)
        .bind(&poi.state)
        .bind(&poi.category)
        .bind(&poi.name)
        .bind(&poi.display_name)
        .bind(&poi.brand)
        .bind(poi.lon)
        .bind(poi.lat)
        .bind(&poi.tags_json)
        .execute(&mut *tx)
        .await?;

        if (i + 1) % 50000 == 0 {
            tracing::info!("  POIs: {}/{}", i + 1, pois.len());
        }
    }
    tx.commit().await?;
    tracing::info!("  Imported {} POIs", pois.len());
    sync_osm_overlay_from_pois(pool).await?;
    Ok(())
}

pub async fn update_meta(pool: &PgPool, states: &[String]) -> Result<(), anyhow::Error> {
    let now = chrono::Utc::now().to_rfc3339();
    let states_str = states.join(",");

    let upsert = "INSERT INTO meta (key, value) VALUES ($1, $2) ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value";
    sqlx::query(upsert)
        .bind("schema_version")
        .bind(openinterstate_core::schema::PRODUCT_SCHEMA_VERSION.to_string())
        .execute(pool)
        .await?;
    sqlx::query(upsert)
        .bind("generated_at")
        .bind(&now)
        .execute(pool)
        .await?;
    sqlx::query(upsert)
        .bind("source")
        .bind("© OpenStreetMap contributors (ODbL). Data sourced from OSM PBF extracts and processed locally.")
        .execute(pool)
        .await?;
    sqlx::query(upsert)
        .bind("states")
        .bind(&states_str)
        .execute(pool)
        .await?;

    Ok(())
}

/// Sync the current OSM rows in `pois` into provider overlay tables.
///
/// Canonical ID strategy for now: OSM POI IDs are canonical IDs.
/// Future providers can map to an existing canonical ID during conflation.
pub async fn sync_osm_overlay_from_pois(pool: &PgPool) -> Result<(), anyhow::Error> {
    sqlx::query(
        "CREATE TABLE IF NOT EXISTS poi_canonical ( \
            id TEXT PRIMARY KEY, \
            category TEXT, \
            name TEXT, \
            display_name TEXT, \
            brand TEXT, \
            geom GEOMETRY(Point, 4326) NOT NULL, \
            tags_json JSONB, \
            updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW() \
        )",
    )
    .execute(pool)
    .await?;

    sqlx::query(
        "CREATE TABLE IF NOT EXISTS poi_provider_observations ( \
            provider TEXT NOT NULL, \
            provider_poi_id TEXT NOT NULL, \
            canonical_poi_id TEXT NOT NULL REFERENCES poi_canonical(id), \
            category TEXT, \
            name TEXT, \
            display_name TEXT, \
            brand TEXT, \
            geom GEOMETRY(Point, 4326) NOT NULL, \
            tags_json JSONB, \
            first_seen_at TIMESTAMPTZ NOT NULL DEFAULT NOW(), \
            last_seen_at TIMESTAMPTZ NOT NULL DEFAULT NOW(), \
            PRIMARY KEY (provider, provider_poi_id) \
        )",
    )
    .execute(pool)
    .await?;

    // Upsert canonical OSM-backed POIs.
    sqlx::query(
        "INSERT INTO poi_canonical (id, category, name, display_name, brand, geom, tags_json, updated_at) \
         SELECT p.id, p.category, p.name, p.display_name, p.brand, p.geom, p.tags_json, NOW() \
         FROM pois p \
         ON CONFLICT (id) DO UPDATE SET \
           category = EXCLUDED.category, \
           name = EXCLUDED.name, \
           display_name = EXCLUDED.display_name, \
           brand = EXCLUDED.brand, \
           geom = EXCLUDED.geom, \
           tags_json = EXCLUDED.tags_json, \
           updated_at = NOW()",
    )
    .execute(pool)
    .await?;

    // Upsert OSM provider observations.
    sqlx::query(
        "INSERT INTO poi_provider_observations \
           (provider, provider_poi_id, canonical_poi_id, category, name, display_name, brand, geom, tags_json, first_seen_at, last_seen_at) \
         SELECT 'osm', p.id, p.id, p.category, p.name, p.display_name, p.brand, p.geom, p.tags_json, NOW(), NOW() \
         FROM pois p \
         ON CONFLICT (provider, provider_poi_id) DO UPDATE SET \
           canonical_poi_id = EXCLUDED.canonical_poi_id, \
           category = EXCLUDED.category, \
           name = EXCLUDED.name, \
           display_name = EXCLUDED.display_name, \
           brand = EXCLUDED.brand, \
           geom = EXCLUDED.geom, \
           tags_json = EXCLUDED.tags_json, \
           last_seen_at = NOW()",
    )
    .execute(pool)
    .await?;

    Ok(())
}
