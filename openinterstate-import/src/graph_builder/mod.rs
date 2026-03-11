mod component_ids;
mod compress;
pub mod corridors;
mod directions;

use openinterstate_core::highway_ref::normalize_highway_ref;
use sqlx::PgPool;

use crate::parser::{ParsedExit, ParsedHighway};

use component_ids::stabilize_component_ids;
use compress::compress_highway_graph;

#[derive(Debug, Clone, Copy)]
pub struct CorridorHealStats {
    pub updated_rows: u64,
    pub unresolved_no_edge: i64,
    pub unresolved_ambiguous: i64,
}

/// Build compressed highway graph from `osm2pgsql_v2_*` tables and write
/// `highway_edges` + `exit_corridors`.
///
/// 1. Load highways and exits from osm2pgsql canonical tables
/// 2. Group ways by highway ref (include motorway_link only when it carries a ref)
/// 3. Build directed adjacency graph per highway
/// 4. Detect connected components (separates EB/WB carriageways)
/// 5. Walk directed edges between stop nodes to create compressed edges
/// 6. Compute cardinal direction per component
/// 7. Write edges into `highway_edges` and corridor entries into `exit_corridors`
pub async fn build_graph(pool: &PgPool) -> Result<usize, anyhow::Error> {
    tracing::info!("Loading highways from osm2pgsql_v2_highways...");
    let highways = load_highways(pool).await?;
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
              geom, min_lat, max_lat, min_lon, max_lon, polyline_json, direction) \
             VALUES ($1, $2, $3, $4, $5, $6, \
              ST_GeomFromText($7, 4326), $8, $9, $10, $11, $12, $13) \
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
    Option<String>, // ref
    Option<String>, // oneway
    Vec<i64>,       // node_ids
    String,         // geom as GeoJSON
);

async fn load_highways(pool: &PgPool) -> Result<Vec<ParsedHighway>, anyhow::Error> {
    let rows: Vec<HighwayRow> = sqlx::query_as(
        "SELECT way_id, highway, ref, oneway, node_ids, \
         ST_AsGeoJSON(geom)::text \
         FROM osm2pgsql_v2_highways \
         WHERE node_ids IS NOT NULL AND array_length(node_ids, 1) >= 2",
    )
    .fetch_all(pool)
    .await?;

    let mut highways = Vec::with_capacity(rows.len());
    for row in rows {
        let (way_id, highway_type, ref_raw, oneway_raw, node_ids, geojson) = row;

        // NOTE: Do NOT add motorway_link here. Ref'd motorway_links at
        // interchanges bridge EB/WB carriageways, creating merged components
        // that the directional split shatters into many fragments. Instead,
        // use corridor-level bridge merge on non-terminal nodes (corridors.rs).
        let refs: Vec<String> = if highway_type == "motorway" || highway_type == "trunk" {
            ref_raw
                .as_deref()
                .unwrap_or("")
                .split(';')
                .filter_map(|r| normalize_highway_ref(r.trim()))
                .collect()
        } else {
            Vec::new()
        };

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
            id: format!("way/{}", way_id),
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
        .map(|(node_id, ref_val, name, lat, lon)| ParsedExit {
            id: format!("node/{}", node_id),
            osm_type: "node".to_string(),
            osm_id: node_id,
            lat,
            lon,
            state: None,
            r#ref: ref_val,
            name,
            highway: None,
            direction: None,
            tags_json: None,
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

pub async fn heal_exit_corridors_from_graph_nodes(
    pool: &PgPool,
) -> Result<CorridorHealStats, anyhow::Error> {
    let updated_rows = sqlx::query(
        "WITH component_match AS ( \
             SELECT ec.exit_id, ec.highway, \
                    MIN(h.component) AS graph_component, \
                    COUNT(DISTINCT h.component) AS component_count \
             FROM exit_corridors ec \
             JOIN highway_edges h \
               ON h.highway = ec.highway \
              AND (h.start_node = ec.graph_node OR h.end_node = ec.graph_node) \
             GROUP BY ec.exit_id, ec.highway \
         ), component_direction AS ( \
             SELECT highway, component, MAX(direction) AS direction \
             FROM highway_edges \
             GROUP BY highway, component \
         ), updates AS ( \
             SELECT cm.exit_id, cm.highway, cm.graph_component, cd.direction \
             FROM component_match cm \
             LEFT JOIN component_direction cd \
               ON cd.highway = cm.highway \
              AND cd.component = cm.graph_component \
             WHERE cm.component_count = 1 \
         ) \
         UPDATE exit_corridors ec \
         SET graph_component = u.graph_component, \
             direction = COALESCE(u.direction, ec.direction) \
         FROM updates u \
         WHERE ec.exit_id = u.exit_id \
           AND ec.highway = u.highway \
           AND (ec.graph_component <> u.graph_component \
             OR ec.direction IS DISTINCT FROM COALESCE(u.direction, ec.direction))",
    )
    .execute(pool)
    .await?
    .rows_affected();

    let (unresolved_no_edge,): (i64,) = sqlx::query_as(
        "SELECT COUNT(*) \
         FROM exit_corridors ec \
         WHERE NOT EXISTS ( \
             SELECT 1 \
             FROM highway_edges h \
             WHERE h.highway = ec.highway \
               AND (h.start_node = ec.graph_node OR h.end_node = ec.graph_node) \
         )",
    )
    .fetch_one(pool)
    .await?;

    let (unresolved_ambiguous,): (i64,) = sqlx::query_as(
        "SELECT COUNT(*) FROM ( \
             SELECT ec.exit_id, ec.highway \
             FROM exit_corridors ec \
             JOIN highway_edges h \
               ON h.highway = ec.highway \
              AND (h.start_node = ec.graph_node OR h.end_node = ec.graph_node) \
             GROUP BY ec.exit_id, ec.highway \
             HAVING COUNT(DISTINCT h.component) > 1 \
         ) ambiguous",
    )
    .fetch_one(pool)
    .await?;

    Ok(CorridorHealStats {
        updated_rows,
        unresolved_no_edge,
        unresolved_ambiguous,
    })
}
