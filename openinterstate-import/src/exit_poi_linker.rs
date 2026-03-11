mod reachability;

use std::collections::HashSet;

use sqlx::PgPool;

pub use reachability::{
    score_and_filter_poi_reachability, score_and_filter_poi_reachability_for_exits,
};

const EXIT_BATCH_SIZE: usize = 2_000;
const POI_BATCH_SIZE: usize = 6_000;

/// Link exits to nearby POIs using PostGIS spatial queries.
///
/// Uses a bbox pre-filter (`&&` + `ST_Expand`) to hit the GIST index, then
/// refines with `ST_DWithin` on geography for precise meter-based distance.
/// Processes in batches for progress reporting.
///
/// Returns the number of links created.
pub async fn link_exits_to_pois(pool: &PgPool, radius_meters: i32) -> Result<usize, anyhow::Error> {
    // Clear existing links
    sqlx::query("DELETE FROM exit_poi_candidates")
        .execute(pool)
        .await?;

    // Count total exits
    let (total,): (i64,) = sqlx::query_as("SELECT COUNT(*) FROM exits")
        .fetch_one(pool)
        .await?;
    let total = total as usize;

    if total == 0 {
        return Ok(0);
    }

    // Degree expansion for bbox pre-filter.
    // 800m at 48°N ≈ 0.0108°. Use 0.012° as safe overestimate for all US latitudes.
    // This is wider than the actual radius, so no valid pairs are missed.
    let degree_expansion = (radius_meters as f64 / 1000.0) * 0.015;

    let batch_size: usize = 1000;
    let num_batches = total.div_ceil(batch_size);
    let mut total_links: usize = 0;

    tracing::info!(
        "Linking {} exits to POIs (radius={}m, {} batches of {})...",
        total,
        radius_meters,
        num_batches,
        batch_size
    );

    for batch in 0..num_batches {
        let offset = (batch * batch_size) as i64;

        let result = sqlx::query(&format!(
            r#"
            INSERT INTO exit_poi_candidates (exit_id, poi_id, category, distance_m, rank)
            SELECT exit_id, poi_id, category, distance_m, rank
            FROM (
                SELECT
                    e.id AS exit_id,
                    p.id AS poi_id,
                    p.category,
                    ROUND(ST_Distance(e.geom::geography, p.geom::geography))::integer AS distance_m,
                    ROW_NUMBER() OVER (
                        PARTITION BY e.id, p.category
                        ORDER BY ST_Distance(e.geom, p.geom)
                    ) AS rank
                FROM (SELECT id, geom FROM exits ORDER BY id LIMIT {batch_size} OFFSET $1) e
                JOIN pois p
                  ON p.geom && ST_Expand(e.geom, {degree_expansion})
                 AND ST_DWithin(e.geom::geography, p.geom::geography, {radius_meters})
                WHERE p.category IS NOT NULL
            ) ranked
            WHERE rank <= 12
            "#,
        ))
        .bind(offset)
        .execute(pool)
        .await?;

        total_links += result.rows_affected() as usize;

        let exits_done = ((batch + 1) * batch_size).min(total);
        let pct = (exits_done as f64 / total as f64 * 100.0).round() as u32;

        // Progress bar
        let bar_width = 40;
        let filled = (bar_width as f64 * exits_done as f64 / total as f64).round() as usize;
        let bar: String = format!("[{}{}]", "#".repeat(filled), "-".repeat(bar_width - filled));

        tracing::info!(
            "  {} {}% ({}/{} exits, {} links so far)",
            bar,
            pct,
            exits_done,
            total,
            total_links
        );
    }

    // Deduplicate same-brand POIs within 50m at each exit.
    // Catches OSM node-inside-building dupes, individual EV charger stalls, and
    // canopy+area mappings for the same physical location.
    let dedup_result = sqlx::query(
        r#"
        DELETE FROM exit_poi_candidates
        WHERE (exit_id, poi_id) IN (
            SELECT ep2.exit_id, ep2.poi_id
            FROM exit_poi_candidates ep1
            JOIN pois p1 ON p1.id = ep1.poi_id
            JOIN exit_poi_candidates ep2 ON ep2.exit_id = ep1.exit_id
            JOIN pois p2 ON p2.id = ep2.poi_id
            WHERE p1.id < p2.id
              AND p1.brand = p2.brand
              AND p1.brand IS NOT NULL AND p1.brand != ''
              AND p1.category = p2.category
              AND ST_DWithin(p1.geom::geography, p2.geom::geography, 50)
        )
        "#,
    )
    .execute(pool)
    .await?;

    let deduped = dedup_result.rows_affected();
    tracing::info!(
        "Removed {} duplicate POI links (same brand within 50m)",
        deduped
    );

    let final_total = total_links - deduped as usize;

    Ok(final_total)
}

/// Find exits that are within link radius of the provided POI IDs.
pub async fn exits_near_pois(
    pool: &PgPool,
    poi_ids: &[String],
    radius_meters: i32,
) -> Result<Vec<String>, anyhow::Error> {
    if poi_ids.is_empty() {
        return Ok(Vec::new());
    }

    let mut exits: HashSet<String> = HashSet::new();
    let degree_expansion = (radius_meters as f64 / 1000.0) * 0.015;

    for chunk in poi_ids.chunks(POI_BATCH_SIZE) {
        let rows: Vec<(String,)> = sqlx::query_as(
            "SELECT DISTINCT e.id \
             FROM UNNEST($1::text[]) AS ids(poi_id) \
             JOIN pois p ON p.id = ids.poi_id \
             JOIN exits e \
               ON e.geom && ST_Expand(p.geom, $2) \
              AND ST_DWithin(e.geom::geography, p.geom::geography, $3)",
        )
        .bind(chunk)
        .bind(degree_expansion)
        .bind(radius_meters as f64)
        .fetch_all(pool)
        .await?;

        for (exit_id,) in rows {
            exits.insert(exit_id);
        }
    }

    let mut out: Vec<String> = exits.into_iter().collect();
    out.sort();
    Ok(out)
}

/// Recompute exit->POI links for a subset of exits.
///
/// This preserves ranking semantics (`top 12 per category`) for each touched
/// exit while avoiding a global relink, and keeps existing reachability scores
/// for unchanged link pairs so interrupted runs can resume efficiently.
pub async fn relink_exits_subset(
    pool: &PgPool,
    exit_ids: &[String],
    radius_meters: i32,
) -> Result<usize, anyhow::Error> {
    if exit_ids.is_empty() {
        return Ok(0);
    }

    let degree_expansion = degree_expansion_for_radius(radius_meters);

    // Remove only stale links for touched exits (pairs no longer in top-12/radius).
    for chunk in exit_ids.chunks(EXIT_BATCH_SIZE) {
        delete_stale_links_for_exit_chunk(pool, chunk, radius_meters, degree_expansion).await?;
    }

    let mut total_links = 0usize;
    // Upsert new/current links for touched exits.
    for chunk in exit_ids.chunks(EXIT_BATCH_SIZE) {
        total_links +=
            upsert_links_for_exit_chunk(pool, chunk, radius_meters, degree_expansion).await?;
    }

    let mut deduped = 0usize;
    for chunk in exit_ids.chunks(EXIT_BATCH_SIZE) {
        deduped += dedupe_links_for_exit_chunk(pool, chunk).await?;
    }

    Ok(total_links.saturating_sub(deduped))
}

fn degree_expansion_for_radius(radius_meters: i32) -> f64 {
    (radius_meters as f64 / 1000.0) * 0.015
}

async fn delete_stale_links_for_exit_chunk(
    pool: &PgPool,
    exit_ids: &[String],
    radius_meters: i32,
    degree_expansion: f64,
) -> Result<(), anyhow::Error> {
    sqlx::query(&format!(
        r#"
        WITH ranked AS (
            SELECT
                e.id AS exit_id,
                p.id AS poi_id,
                p.category,
                ROUND(ST_Distance(e.geom::geography, p.geom::geography))::integer AS distance_m,
                ROW_NUMBER() OVER (
                    PARTITION BY e.id, p.category
                    ORDER BY ST_Distance(e.geom, p.geom)
                ) AS rank
            FROM exits e
            JOIN pois p
              ON p.geom && ST_Expand(e.geom, {degree_expansion})
             AND ST_DWithin(e.geom::geography, p.geom::geography, {radius_meters})
            WHERE p.category IS NOT NULL
              AND e.id = ANY($1)
        ),
        new_links AS (
            SELECT exit_id, poi_id
            FROM ranked
            WHERE rank <= 12
        )
        DELETE FROM exit_poi_candidates ep
        WHERE ep.exit_id = ANY($1)
          AND NOT EXISTS (
              SELECT 1
              FROM new_links nl
              WHERE nl.exit_id = ep.exit_id
                AND nl.poi_id = ep.poi_id
          )
        "#,
    ))
    .bind(exit_ids)
    .execute(pool)
    .await?;

    Ok(())
}

async fn upsert_links_for_exit_chunk(
    pool: &PgPool,
    exit_ids: &[String],
    radius_meters: i32,
    degree_expansion: f64,
) -> Result<usize, anyhow::Error> {
    let (chunk_links,): (i64,) = sqlx::query_as(&format!(
        r#"
        WITH ranked AS (
            SELECT
                e.id AS exit_id,
                p.id AS poi_id,
                p.category,
                ROUND(ST_Distance(e.geom::geography, p.geom::geography))::integer AS distance_m,
                ROW_NUMBER() OVER (
                    PARTITION BY e.id, p.category
                    ORDER BY ST_Distance(e.geom, p.geom)
                ) AS rank
            FROM exits e
            JOIN pois p
              ON p.geom && ST_Expand(e.geom, {degree_expansion})
             AND ST_DWithin(e.geom::geography, p.geom::geography, {radius_meters})
            WHERE p.category IS NOT NULL
              AND e.id = ANY($1)
        ),
        new_links AS (
            SELECT exit_id, poi_id, category, distance_m, rank
            FROM ranked
            WHERE rank <= 12
        ),
        upserted AS (
            INSERT INTO exit_poi_candidates (exit_id, poi_id, category, distance_m, rank)
            SELECT exit_id, poi_id, category, distance_m, rank
            FROM new_links
            ON CONFLICT (exit_id, poi_id) DO UPDATE
            SET category = EXCLUDED.category,
                distance_m = EXCLUDED.distance_m,
                rank = EXCLUDED.rank
            WHERE exit_poi_candidates.category IS DISTINCT FROM EXCLUDED.category
               OR exit_poi_candidates.distance_m IS DISTINCT FROM EXCLUDED.distance_m
               OR exit_poi_candidates.rank IS DISTINCT FROM EXCLUDED.rank
        )
        SELECT COUNT(*)::bigint
        FROM new_links
        "#,
    ))
    .bind(exit_ids)
    .fetch_one(pool)
    .await?;

    Ok(chunk_links as usize)
}

async fn dedupe_links_for_exit_chunk(
    pool: &PgPool,
    exit_ids: &[String],
) -> Result<usize, anyhow::Error> {
    let result = sqlx::query(
        r#"
        DELETE FROM exit_poi_candidates
        WHERE (exit_id, poi_id) IN (
            SELECT ep2.exit_id, ep2.poi_id
            FROM exit_poi_candidates ep1
            JOIN pois p1 ON p1.id = ep1.poi_id
            JOIN exit_poi_candidates ep2 ON ep2.exit_id = ep1.exit_id
            JOIN pois p2 ON p2.id = ep2.poi_id
            WHERE ep1.exit_id = ANY($1)
              AND p1.id < p2.id
              AND p1.brand = p2.brand
              AND p1.brand IS NOT NULL AND p1.brand != ''
              AND p1.category = p2.category
              AND ST_DWithin(p1.geom::geography, p2.geom::geography, 50)
        )
        "#,
    )
    .bind(exit_ids)
    .execute(pool)
    .await?;

    Ok(result.rows_affected() as usize)
}
