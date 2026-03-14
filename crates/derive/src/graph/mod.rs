mod component_ids;
mod compress;
mod directions;
pub mod relation_corridors;

use std::collections::HashMap;
use std::path::Path;

use openinterstate_core::highway_ref::{is_interstate_highway_ref, normalize_highway_ref};
use sqlx::PgPool;

use crate::canonical_types::{ParsedExit, ParsedHighway};
use crate::interstate_relations::load_relation_refs_by_way;

use component_ids::stabilize_component_ids;
use compress::compress_highway_graph;

/// Build compressed highway graph from `osm2pgsql_v2_*` tables and write
/// `highway_edges` + `exit_corridors`.
///
/// 1. Load highways and exits from osm2pgsql canonical tables
/// 2. Group ways by highway ref and adopt unlabeled high-class connector paths
/// 3. Build directed adjacency graph per highway
/// 4. Detect connected components (separates EB/WB carriageways)
/// 5. Walk directed edges between stop nodes to create compressed edges
/// 6. Compute cardinal direction per component
/// 7. Write edges into `highway_edges` and corridor entries into `exit_corridors`
pub async fn build_graph(
    pool: &PgPool,
    interstate_relation_cache: &Path,
) -> Result<usize, anyhow::Error> {
    let relation_refs_by_way = load_relation_refs_by_way(interstate_relation_cache)?;
    tracing::info!(
        "Loaded Interstate relation memberships for {} way ids",
        relation_refs_by_way.len()
    );

    tracing::info!("Loading highways from osm2pgsql_v2_highways...");
    let highways = load_highways(pool, &relation_refs_by_way).await?;
    tracing::info!("Loaded {} highway ways", highways.len());

    tracing::info!("Loading exits from osm2pgsql_v2_exits_nodes...");
    let exits = load_exits(pool).await?;
    tracing::info!("Loaded {} exits", exits.len());

    tracing::info!("Truncating highway_edges and exit_corridors...");
    sqlx::query("TRUNCATE highway_edges, exit_corridors")
        .execute(pool)
        .await?;

    let (mut edges, mut corridor_entries) = compress_highway_graph(&highways, &exits);
    stabilize_component_ids(pool, &mut edges, &mut corridor_entries).await?;

    tracing::info!(
        "Graph compression: {} edges, {} corridor entries",
        edges.len(),
        corridor_entries.len()
    );

    // Write edges
    let mut tx = pool.begin().await?;
    for (i, edge) in edges.iter().enumerate() {
        sqlx::query(
            "INSERT INTO highway_edges \
             (id, highway, component, start_node, end_node, length_m, \
              geom, min_lat, max_lat, min_lon, max_lon, polyline_json, source_way_ids_json, direction) \
             VALUES ($1, $2, $3, $4, $5, $6, \
              ST_GeomFromText($7, 4326), $8, $9, $10, $11, $12, $13, $14) \
             ON CONFLICT (id) DO NOTHING",
        )
        .bind(&edge.id)
        .bind(&edge.highway)
        .bind(edge.component)
        .bind(edge.start_node)
        .bind(edge.end_node)
        .bind(edge.length_m)
        .bind(&edge.geom_wkt)
        .bind(edge.min_lat)
        .bind(edge.max_lat)
        .bind(edge.min_lon)
        .bind(edge.max_lon)
        .bind(&edge.polyline_json)
        .bind(&edge.source_way_ids_json)
        .bind(&edge.direction)
        .execute(&mut *tx)
        .await?;

        if (i + 1) % 10_000 == 0 {
            tracing::info!("  edges: {}/{}", i + 1, edges.len());
        }
    }
    tx.commit().await?;

    // Write corridor entries
    let mut tx = pool.begin().await?;
    let mut inserted = 0_usize;
    for (i, entry) in corridor_entries.iter().enumerate() {
        let result = sqlx::query(
            "INSERT INTO exit_corridors (exit_id, highway, graph_component, graph_node, direction) \
             VALUES ($1, $2, $3, $4, $5) \
             ON CONFLICT (exit_id, highway) DO UPDATE SET \
               graph_component = EXCLUDED.graph_component, \
               graph_node = EXCLUDED.graph_node, \
               direction = EXCLUDED.direction",
        )
        .bind(&entry.exit_id)
        .bind(&entry.highway)
        .bind(entry.component)
        .bind(entry.node_id)
        .bind(&entry.direction)
        .execute(&mut *tx)
        .await?;
        if result.rows_affected() > 0 {
            inserted += 1;
        }

        if (i + 1) % 10_000 == 0 {
            tracing::info!("  corridor entries: {}/{}", i + 1, corridor_entries.len());
        }
    }
    tx.commit().await?;
    tracing::info!("  Inserted {} exit_corridors entries", inserted);

    Ok(edges.len())
}

// ============================================================================
// DB loading from osm2pgsql canonical tables
// ============================================================================

type HighwayRow = (
    i64,            // way_id
    String,         // highway type
    Option<String>, // ref / int_ref
    Option<String>, // oneway
    Vec<i64>,       // node_ids
    String,         // geom as GeoJSON
);

async fn load_highways(
    pool: &PgPool,
    relation_refs_by_way: &HashMap<i64, Vec<String>>,
) -> Result<Vec<ParsedHighway>, anyhow::Error> {
    let relation_way_ids: Vec<i64> = relation_refs_by_way.keys().copied().collect();
    let rows: Vec<HighwayRow> = if relation_way_ids.is_empty() {
        sqlx::query_as(
            "SELECT way_id, highway, \
                    NULLIF(TRIM(BOTH ';' FROM CONCAT_WS(';', NULLIF(BTRIM(ref), ''), NULLIF(BTRIM(tags ->> 'int_ref'), ''))), '') AS ref_text, \
                    oneway, node_ids, \
             ST_AsGeoJSON(geom)::text \
             FROM osm2pgsql_v2_highways \
             WHERE highway IN ('motorway', 'motorway_link', 'trunk', 'trunk_link') \
               AND node_ids IS NOT NULL \
               AND array_length(node_ids, 1) >= 2",
        )
        .fetch_all(pool)
        .await?
    } else {
        sqlx::query_as(
            "SELECT way_id, highway, \
                    NULLIF(TRIM(BOTH ';' FROM CONCAT_WS(';', NULLIF(BTRIM(ref), ''), NULLIF(BTRIM(tags ->> 'int_ref'), ''))), '') AS ref_text, \
                    oneway, node_ids, \
             ST_AsGeoJSON(geom)::text \
             FROM osm2pgsql_v2_highways \
             WHERE (highway IN ('motorway', 'motorway_link', 'trunk', 'trunk_link') \
                    OR way_id = ANY($1)) \
               AND node_ids IS NOT NULL \
               AND array_length(node_ids, 1) >= 2",
        )
        .bind(&relation_way_ids)
        .fetch_all(pool)
        .await?
    };

    let mut highways = Vec::with_capacity(rows.len());
    for row in rows {
        let (way_id, highway_type, ref_raw, oneway_raw, node_ids, geojson) = row;

        let mut refs: Vec<String> = ref_raw
            .as_deref()
            .unwrap_or("")
            .split(';')
            .filter_map(|r| normalize_highway_ref(r.trim()))
            .collect();
        if let Some(relation_refs) = relation_refs_by_way.get(&way_id) {
            refs.extend(relation_refs.iter().cloned());
        }
        refs.sort();
        refs.dedup();
        let has_interstate_ref = refs
            .iter()
            .any(|reference| is_interstate_highway_ref(reference));
        let has_explicit_ref = !refs.is_empty();

        if !has_interstate_ref && has_explicit_ref {
            continue;
        }

        let geometry = parse_geojson_coords(&geojson);
        if geometry.len() < 2 || geometry.len() != node_ids.len() {
            continue;
        }

        let oneway_tag = oneway_raw.as_deref().unwrap_or("");
        let mut nodes = node_ids;
        let mut geom = geometry;
        let is_oneway = match oneway_tag {
            "no" => false,
            "-1" | "yes" | "1" => true,
            _ => highway_type == "motorway" || highway_type == "motorway_link",
        };
        if oneway_tag == "-1" {
            nodes.reverse();
            geom.reverse();
        }

        highways.push(ParsedHighway {
            way_id,
            refs,
            nodes,
            geometry: geom,
            highway_type,
            is_oneway,
        });
    }

    Ok(highways)
}

async fn load_exits(pool: &PgPool) -> Result<Vec<ParsedExit>, anyhow::Error> {
    let rows: Vec<(i64, Option<String>, Option<String>, f64, f64)> = sqlx::query_as(
        "SELECT en.node_id, en.ref, en.name, \
         ST_Y(en.geom) AS lat, ST_X(en.geom) AS lon \
         FROM osm2pgsql_v2_exits_nodes en",
    )
    .fetch_all(pool)
    .await?;

    Ok(rows
        .into_iter()
        .map(|(node_id, _ref_val, _name, _lat, _lon)| ParsedExit {
            id: format!("node/{}", node_id),
            osm_id: node_id,
        })
        .collect())
}

/// Parse [[lon, lat], ...] coordinates from a GeoJSON LineString.
fn parse_geojson_coords(geojson: &str) -> Vec<(f64, f64)> {
    // Quick parse: extract the coordinates array from {"type":"LineString","coordinates":[[lon,lat],...]}
    let Some(start) = geojson.find("coordinates") else {
        return Vec::new();
    };
    let rest = &geojson[start..];
    let Some(arr_start) = rest.find("[[") else {
        return Vec::new();
    };
    let Some(arr_end) = rest.find("]]") else {
        return Vec::new();
    };
    let inner = &rest[arr_start + 1..arr_end + 1]; // "[lon,lat],[lon,lat],..."

    inner
        .split("],[")
        .filter_map(|pair| {
            let pair = pair.trim_start_matches('[').trim_end_matches(']');
            let mut parts = pair.split(',');
            let lon: f64 = parts.next()?.trim().parse().ok()?;
            let lat: f64 = parts.next()?.trim().parse().ok()?;
            Some((lat, lon))
        })
        .collect()
}
