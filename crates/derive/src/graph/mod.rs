mod component_ids;
mod compress;
mod directions;
pub mod relation_corridors;

use std::collections::HashMap;
use std::path::Path;

use openinterstate_core::highway_ref::{is_interstate_highway_ref, normalize_highway_ref};
use sqlx::PgPool;

use crate::canonical_types::{ParsedExit, ParsedHighway};
use crate::interstate_relations::{
    load_interstate_relation_members, relation_refs_by_way, route_signatures_by_highway_and_way,
};

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
    let relation_members = load_interstate_relation_members(interstate_relation_cache)?;
    let relation_refs_by_way = relation_refs_by_way(&relation_members);
    let route_signatures_by_highway_and_way =
        route_signatures_by_highway_and_way(&relation_members);
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

    let (mut edges, mut corridor_entries) = compress_highway_graph(
        &highways,
        &exits,
        &route_signatures_by_highway_and_way,
    );
    stabilize_component_ids(pool, &mut edges, &mut corridor_entries).await?;

    tracing::info!(
        "Graph compression: {} edges, {} corridor entries",
        edges.len(),
        corridor_entries.len()
    );

    // Write edges in batches using UNNEST for bulk insert
    const EDGE_BATCH: usize = 5_000;
    let mut tx = pool.begin().await?;
    for (batch_idx, chunk) in edges.chunks(EDGE_BATCH).enumerate() {
        let mut ids = Vec::with_capacity(chunk.len());
        let mut highways = Vec::with_capacity(chunk.len());
        let mut components = Vec::with_capacity(chunk.len());
        let mut start_nodes = Vec::with_capacity(chunk.len());
        let mut end_nodes = Vec::with_capacity(chunk.len());
        let mut lengths = Vec::with_capacity(chunk.len());
        let mut geom_wkts = Vec::with_capacity(chunk.len());
        let mut min_lats = Vec::with_capacity(chunk.len());
        let mut max_lats = Vec::with_capacity(chunk.len());
        let mut min_lons = Vec::with_capacity(chunk.len());
        let mut max_lons = Vec::with_capacity(chunk.len());
        let mut polylines = Vec::with_capacity(chunk.len());
        let mut source_ways = Vec::with_capacity(chunk.len());
        let mut directions: Vec<Option<String>> = Vec::with_capacity(chunk.len());

        for edge in chunk {
            ids.push(edge.id.as_str());
            highways.push(edge.highway.as_str());
            components.push(edge.component);
            start_nodes.push(edge.start_node);
            end_nodes.push(edge.end_node);
            lengths.push(edge.length_m);
            geom_wkts.push(edge.geom_wkt.as_str());
            min_lats.push(edge.min_lat);
            max_lats.push(edge.max_lat);
            min_lons.push(edge.min_lon);
            max_lons.push(edge.max_lon);
            polylines.push(edge.polyline_json.as_str());
            source_ways.push(edge.source_way_ids_json.as_str());
            directions.push(edge.direction.clone());
        }

        sqlx::query(
            "INSERT INTO highway_edges \
             (id, highway, component, start_node, end_node, length_m, \
              geom, min_lat, max_lat, min_lon, max_lon, polyline_json, source_way_ids_json, direction) \
             SELECT id, highway, component, start_node, end_node, length_m, \
              ST_GeomFromText(geom_wkt, 4326), min_lat, max_lat, min_lon, max_lon, \
              polyline_json, source_way_ids_json, direction \
             FROM UNNEST($1::text[], $2::text[], $3::int[], $4::int8[], $5::int8[], $6::int[], \
              $7::text[], $8::float8[], $9::float8[], $10::float8[], $11::float8[], \
              $12::text[], $13::text[], $14::text[]) \
              AS t(id, highway, component, start_node, end_node, length_m, \
               geom_wkt, min_lat, max_lat, min_lon, max_lon, polyline_json, source_way_ids_json, direction) \
             ON CONFLICT (id) DO NOTHING",
        )
        .bind(&ids)
        .bind(&highways)
        .bind(&components)
        .bind(&start_nodes)
        .bind(&end_nodes)
        .bind(&lengths)
        .bind(&geom_wkts)
        .bind(&min_lats)
        .bind(&max_lats)
        .bind(&min_lons)
        .bind(&max_lons)
        .bind(&polylines)
        .bind(&source_ways)
        .bind(&directions)
        .execute(&mut *tx)
        .await?;

        let done = (batch_idx + 1) * EDGE_BATCH;
        if done % 10_000 < EDGE_BATCH {
            tracing::info!("  edges: {}/{}", done.min(edges.len()), edges.len());
        }
    }
    tx.commit().await?;

    // Write corridor entries in batches using UNNEST
    const CORRIDOR_BATCH: usize = 5_000;
    let mut tx = pool.begin().await?;
    for (batch_idx, chunk) in corridor_entries.chunks(CORRIDOR_BATCH).enumerate() {
        let mut exit_ids = Vec::with_capacity(chunk.len());
        let mut c_highways = Vec::with_capacity(chunk.len());
        let mut c_components = Vec::with_capacity(chunk.len());
        let mut c_node_ids = Vec::with_capacity(chunk.len());
        let mut c_directions: Vec<Option<String>> = Vec::with_capacity(chunk.len());

        for entry in chunk {
            exit_ids.push(entry.exit_id.as_str());
            c_highways.push(entry.highway.as_str());
            c_components.push(entry.component);
            c_node_ids.push(entry.node_id);
            c_directions.push(entry.direction.clone());
        }

        sqlx::query(
            "INSERT INTO exit_corridors (exit_id, highway, graph_component, graph_node, direction) \
             SELECT exit_id, highway, graph_component, graph_node, direction \
             FROM UNNEST($1::text[], $2::text[], $3::int[], $4::int8[], $5::text[]) \
              AS t(exit_id, highway, graph_component, graph_node, direction) \
             ON CONFLICT (exit_id, highway) DO UPDATE SET \
               graph_component = EXCLUDED.graph_component, \
               graph_node = EXCLUDED.graph_node, \
               direction = EXCLUDED.direction",
        )
        .bind(&exit_ids)
        .bind(&c_highways)
        .bind(&c_components)
        .bind(&c_node_ids)
        .bind(&c_directions)
        .execute(&mut *tx)
        .await?;

        let done = (batch_idx + 1) * CORRIDOR_BATCH;
        if done % 10_000 < CORRIDOR_BATCH {
            tracing::info!(
                "  corridor entries: {}/{}",
                done.min(corridor_entries.len()),
                corridor_entries.len()
            );
        }
    }
    tx.commit().await?;
    tracing::info!(
        "  Inserted {} exit_corridors entries",
        corridor_entries.len()
    );

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
