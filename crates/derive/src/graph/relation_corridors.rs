use std::cmp::Ordering;
use std::collections::{BTreeSet, BinaryHeap, HashMap, HashSet};
use std::path::Path;

use openinterstate_core::geo::haversine_distance;
use openinterstate_core::highway_ref::{is_interstate_highway_ref, normalize_highway_ref};
use sqlx::PgPool;

use crate::interstate_relations::{
    group_relation_members, load_interstate_relation_members, normalize_direction,
    InterstateRouteGroup,
};

const CONNECTOR_HIGHWAY_TYPES: &[&str] = &[
    "motorway",
    "motorway_link",
    "trunk",
    "trunk_link",
    "primary",
    "primary_link",
    "secondary",
    "secondary_link",
    "tertiary",
    "tertiary_link",
    "service",
];
const MIN_ROUTE_LENGTH_M: f64 = 3_000.0;
const SHORT_FALLBACK_CONNECTOR_MAX_COST_M: u64 = 10_000;
const DISCONNECTED_MICRO_SEGMENT_MAX_LENGTH_M: f64 = 2_000.0;
const DISCONNECTED_MICRO_SEGMENT_MAX_SHARE: f64 = 0.05;

pub struct BuildCorridorsStats {
    pub corridors_created: usize,
    pub corridor_exits_created: usize,
    pub edges_updated: usize,
}

type HighwayRow = (
    i64,            // way_id
    String,         // highway type
    Option<String>, // ref text
    Option<String>, // oneway
    Vec<i64>,       // node_ids
    String,         // geom as GeoJSON
);

#[derive(Debug, Clone)]
struct RouteWay {
    way_id: i64,
    refs: Vec<String>,
    nodes: Vec<i64>,
    geometry: Vec<(f64, f64)>,
    highway_type: String,
    is_oneway: bool,
}

#[derive(Debug, Clone)]
struct ConnectorGraph {
    adjacency: HashMap<i64, Vec<ConnectorArc>>,
}

#[derive(Debug, Clone, Copy)]
struct ConnectorArc {
    next: i64,
    weight_m: u64,
    way_id: i64,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
struct SearchState {
    cost_m: u64,
    node: i64,
}

impl Ord for SearchState {
    fn cmp(&self, other: &Self) -> Ordering {
        other
            .cost_m
            .cmp(&self.cost_m)
            .then_with(|| self.node.cmp(&other.node))
    }
}

impl PartialOrd for SearchState {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

#[derive(Debug, Clone)]
struct ExitRow {
    exit_id: String,
    highway: String,
    graph_node: i64,
    ref_val: Option<String>,
    name: Option<String>,
    lat: f64,
    lon: f64,
}

#[derive(Debug, Clone)]
struct HighwayEdgeRow {
    edge_id: String,
    highway: String,
    #[allow(dead_code)]
    direction: Option<String>,
    source_way_ids: Vec<i64>,
}

#[derive(Debug, Clone)]
struct CorridorDraft {
    corridor_id: i32,
    highway: String,
    canonical_direction: String,
    root_relation_id: i64,
    geometry_json: String,
    source_way_ids: Vec<i64>,
    edge_ids: Vec<String>,
    exits: Vec<CorridorExitDraft>,
}

#[derive(Debug, Clone)]
struct CorridorExitDraft {
    exit_id: String,
    ref_val: Option<String>,
    name: Option<String>,
    lat: f64,
    lon: f64,
    sort_key_m: f64,
}

pub async fn build_corridors(
    pool: &PgPool,
    interstate_relation_cache: &Path,
) -> Result<BuildCorridorsStats, anyhow::Error> {
    let relation_members = load_interstate_relation_members(interstate_relation_cache)?;
    let route_groups = filter_route_groups(group_relation_members(&relation_members));
    if route_groups.is_empty() {
        return Err(anyhow::anyhow!(
            "no Interstate relation groups found in relation cache {}",
            interstate_relation_cache.display()
        ));
    }

    tracing::info!(
        "Building relation-backed Interstate corridors from {} official route groups",
        route_groups.len()
    );

    let ways_by_id = load_route_ways(pool, &relation_members).await?;
    let connector_graph = build_connector_graph(ways_by_id.values().collect::<Vec<_>>().as_slice());
    let highway_edges = load_highway_edges(pool).await?;
    let exits = load_exit_rows(pool).await?;
    let all_exit_nodes = load_all_exit_nodes(pool).await?;
    tracing::info!(
        "Loaded {} exit nodes from osm2pgsql_v2_exits_nodes",
        all_exit_nodes.len()
    );

    let mut exits_by_highway: HashMap<String, Vec<ExitRow>> = HashMap::new();
    for exit in exits {
        exits_by_highway
            .entry(exit.highway.clone())
            .or_default()
            .push(exit);
    }

    let mut edge_rows_by_highway: HashMap<String, Vec<HighwayEdgeRow>> = HashMap::new();
    for edge in highway_edges {
        edge_rows_by_highway
            .entry(edge.highway.clone())
            .or_default()
            .push(edge);
    }

    let mut drafts = Vec::new();
    let mut next_corridor_id: i32 = 1;
    for group in route_groups {
        let Some(draft) = build_corridor_draft(
            &group,
            &ways_by_id,
            &connector_graph,
            edge_rows_by_highway
                .get(&group.highway)
                .map(Vec::as_slice)
                .unwrap_or(&[]),
            exits_by_highway
                .get(&group.highway)
                .map(Vec::as_slice)
                .unwrap_or(&[]),
            &all_exit_nodes,
            next_corridor_id,
        )?
        else {
            continue;
        };
        next_corridor_id += 1;
        drafts.push(draft);
    }

    let drafts = dedup_conflicting_corridors(drafts);

    write_corridors(pool, &drafts).await
}

fn filter_route_groups(groups: Vec<InterstateRouteGroup>) -> Vec<InterstateRouteGroup> {
    let directional_roots: HashSet<(String, i64)> = groups
        .iter()
        .filter(|group| group.direction.is_some())
        .map(|group| (group.highway.clone(), group.root_relation_id))
        .collect();

    // Count directional vs blank members per root to decide whether to keep blanks.
    // If blank members vastly outnumber directional ones, the directional sub-relations
    // are likely incomplete and the blank group is the real highway.
    let mut directional_count_by_root: HashMap<(String, i64), usize> = HashMap::new();
    let mut blank_count_by_root: HashMap<(String, i64), usize> = HashMap::new();
    for group in &groups {
        let key = (group.highway.clone(), group.root_relation_id);
        if group.direction.is_some() {
            *directional_count_by_root.entry(key).or_default() += group.members.len();
        } else if directional_roots.contains(&key) {
            *blank_count_by_root.entry(key).or_default() += group.members.len();
        }
    }

    groups
        .into_iter()
        .filter(|group| {
            if group.direction.is_some() {
                return true;
            }
            let key = (group.highway.clone(), group.root_relation_id);
            if !directional_roots.contains(&key) {
                return true;
            }
            // Keep blank group if it has more members than all directional groups combined
            let dir_count = directional_count_by_root.get(&key).copied().unwrap_or(0);
            let blank_count = blank_count_by_root.get(&key).copied().unwrap_or(0);
            if blank_count > dir_count {
                tracing::info!(
                    highway = %group.highway,
                    root_relation_id = group.root_relation_id,
                    blank_count,
                    dir_count,
                    "keeping blank-direction group (outnumbers directional members)"
                );
                return true;
            }
            tracing::debug!(
                highway = %group.highway,
                root_relation_id = group.root_relation_id,
                blank_count,
                dir_count,
                "dropping blank-direction group (directional members dominate)"
            );
            false
        })
        .collect()
}

async fn load_route_ways(
    pool: &PgPool,
    relation_members: &[crate::interstate_relations::InterstateRelationMember],
) -> Result<HashMap<i64, RouteWay>, anyhow::Error> {
    let relation_way_ids: Vec<i64> = relation_members
        .iter()
        .map(|member| member.way_id)
        .collect();

    let rows: Vec<HighwayRow> = sqlx::query_as(
        "SELECT way_id, highway, \
                NULLIF(TRIM(BOTH ';' FROM CONCAT_WS(';', NULLIF(BTRIM(ref), ''), NULLIF(BTRIM(tags ->> 'int_ref'), ''))), '') AS ref_text, \
                oneway, node_ids, \
                ST_AsGeoJSON(geom)::text \
         FROM osm2pgsql_v2_highways \
         WHERE (highway IN ('motorway', 'motorway_link', 'trunk', 'trunk_link', 'primary', 'primary_link', \
                            'secondary', 'secondary_link', 'tertiary', 'tertiary_link', 'service') \
                OR way_id = ANY($1)) \
           AND node_ids IS NOT NULL \
           AND array_length(node_ids, 1) >= 2",
    )
    .bind(&relation_way_ids)
    .fetch_all(pool)
    .await?;

    let mut ways_by_id = HashMap::with_capacity(rows.len());
    for (way_id, highway_type, ref_raw, oneway_raw, node_ids, geojson) in rows {
        let refs: Vec<String> = ref_raw
            .as_deref()
            .unwrap_or("")
            .split(';')
            .filter_map(|value| normalize_highway_ref(value.trim()))
            .collect();
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

        ways_by_id.insert(
            way_id,
            RouteWay {
                way_id,
                refs,
                nodes,
                geometry: geom,
                highway_type,
                is_oneway,
            },
        );
    }

    Ok(ways_by_id)
}

async fn load_highway_edges(pool: &PgPool) -> Result<Vec<HighwayEdgeRow>, anyhow::Error> {
    let rows: Vec<(String, String, Option<String>, String)> = sqlx::query_as(
        "SELECT id, highway, direction, source_way_ids_json \
         FROM highway_edges \
         WHERE highway LIKE 'I-%'",
    )
    .fetch_all(pool)
    .await?;

    Ok(rows
        .into_iter()
        .map(
            |(edge_id, highway, direction, source_way_ids_json)| HighwayEdgeRow {
                edge_id,
                highway,
                direction: direction.and_then(|value| normalize_direction(&value)),
                source_way_ids: parse_way_ids_json(&source_way_ids_json),
            },
        )
        .collect())
}

async fn load_exit_rows(pool: &PgPool) -> Result<Vec<ExitRow>, anyhow::Error> {
    let rows: Vec<(
        String,
        String,
        i64,
        Option<String>,
        Option<String>,
        f64,
        f64,
    )> = sqlx::query_as(
        "SELECT ec.exit_id, ec.highway, ec.graph_node, e.ref, e.name, \
                    ST_Y(e.geom) AS lat, ST_X(e.geom) AS lon \
             FROM exit_corridors ec \
             JOIN exits e ON e.id = ec.exit_id \
             WHERE ec.highway LIKE 'I-%'",
    )
    .fetch_all(pool)
    .await?;

    Ok(rows
        .into_iter()
        .map(
            |(exit_id, highway, graph_node, ref_val, name, lat, lon)| ExitRow {
                exit_id,
                highway,
                graph_node,
                ref_val,
                name,
                lat,
                lon,
            },
        )
        .collect())
}

/// Load all motorway_junction exit nodes keyed by OSM node id.
/// This supplements `exit_corridors` by providing exit metadata for nodes
/// that appear on ramp ways not adopted during graph compression.
async fn load_all_exit_nodes(pool: &PgPool) -> Result<HashMap<i64, ExitNodeInfo>, anyhow::Error> {
    let rows: Vec<(i64, Option<String>, Option<String>, f64, f64)> = sqlx::query_as(
        "SELECT node_id, ref, name, ST_Y(geom) AS lat, ST_X(geom) AS lon \
         FROM osm2pgsql_v2_exits_nodes",
    )
    .fetch_all(pool)
    .await?;

    Ok(rows
        .into_iter()
        .map(|(node_id, ref_val, name, lat, lon)| {
            (
                node_id,
                ExitNodeInfo {
                    ref_val,
                    name,
                    lat,
                    lon,
                },
            )
        })
        .collect())
}

#[derive(Debug, Clone)]
struct ExitNodeInfo {
    ref_val: Option<String>,
    name: Option<String>,
    lat: f64,
    lon: f64,
}

fn build_corridor_draft(
    group: &InterstateRouteGroup,
    ways_by_id: &HashMap<i64, RouteWay>,
    connector_graph: &ConnectorGraph,
    edge_rows: &[HighwayEdgeRow],
    exit_rows: &[ExitRow],
    all_exit_nodes: &HashMap<i64, ExitNodeInfo>,
    corridor_id: i32,
) -> Result<Option<CorridorDraft>, anyhow::Error> {
    let canonical_direction = group
        .direction
        .clone()
        .unwrap_or_else(|| "north".to_string());

    let ordered_members = ordered_group_members(group, ways_by_id);
    let mut assigned_way_ids: BTreeSet<i64> = group
        .members
        .iter()
        .map(|member| member.way_id)
        .filter(|way_id| ways_by_id.contains_key(way_id))
        .collect();
    if assigned_way_ids.is_empty() {
        return Ok(None);
    }

    let allow_unassigned_interstate_connectors = group.direction.is_none();

    adopt_relation_connector_paths(
        &ordered_members,
        &mut assigned_way_ids,
        ways_by_id,
        connector_graph,
        allow_unassigned_interstate_connectors,
    );
    adopt_connector_paths(
        &mut assigned_way_ids,
        ways_by_id,
        connector_graph,
        &group.highway,
        allow_unassigned_interstate_connectors,
    );

    let assigned_ways: Vec<&RouteWay> = assigned_way_ids
        .iter()
        .filter_map(|way_id| ways_by_id.get(way_id))
        .collect();
    let route_segments =
        prune_micro_route_segments(build_route_segments(&assigned_ways, &canonical_direction));

    let total_length_m: f64 = route_segments
        .iter()
        .map(|segment| segment_length_m(segment))
        .sum();
    if total_length_m < MIN_ROUTE_LENGTH_M {
        return Ok(None);
    }

    let geometry_json = serialize_geometry_geojson(&route_segments)?;
    let assigned_nodes: HashSet<i64> = assigned_ways
        .iter()
        .flat_map(|way| way.nodes.iter().copied())
        .collect();
    let expanded_nodes =
        expand_nodes_with_adjacent_ways(&assigned_nodes, &group.highway, ways_by_id);

    // Collect exits from two sources, each with different coordinate provenance:
    //
    // 1. exit_corridors (graph-assigned): coordinates come from the graph node
    //    that the exit was snapped to during graph compression. These are on the
    //    mainline carriageway, not the physical ramp.
    //
    // 2. Direct exit node lookup (load_all_exit_nodes): coordinates come from the
    //    original motorway_junction node in OSM, typically on the ramp way itself.
    //    These are more accurate for POI reachability since they reflect the
    //    actual ramp location.
    let mut corridor_exit_rows: Vec<ExitRow> = exit_rows
        .iter()
        .filter(|exit| expanded_nodes.contains(&exit.graph_node))
        .cloned()
        .collect();

    // Discover exit nodes on expanded ways that weren't in exit_corridors.
    // These are typically on blank-ref motorway_link ramps connected to the corridor.
    let known_nodes: HashSet<i64> = corridor_exit_rows
        .iter()
        .map(|exit| exit.graph_node)
        .collect();
    for &node_id in &expanded_nodes {
        if known_nodes.contains(&node_id) {
            continue;
        }
        if let Some(info) = all_exit_nodes.get(&node_id) {
            corridor_exit_rows.push(ExitRow {
                exit_id: format!("node/{}", node_id),
                highway: group.highway.clone(),
                graph_node: node_id,
                ref_val: info.ref_val.clone(),
                name: info.name.clone(),
                lat: info.lat,
                lon: info.lon,
            });
        }
    }

    // Discover sibling exits: if we have "100B", look for nearby "100A", "100C"
    // in the full exit node set. Siblings share the same base number and are
    // within 5km of an existing corridor exit.
    let corridor_exit_rows = discover_sibling_exits(corridor_exit_rows, all_exit_nodes);

    // Discover exits near the corridor geometry but not reachable through
    // graph expansion (e.g. exits on distant ramps or service roads).
    let corridor_exit_rows = discover_nearby_exits(
        corridor_exit_rows,
        all_exit_nodes,
        &route_segments,
        &group.highway,
    );

    // Second proximity pass: also check exits near ALL ways with the same
    // highway ref, not just the corridor's assigned ways. This catches exits
    // in sections where the corridor geometry has gaps but the highway ways
    // still exist in the graph.
    let all_same_ref_segments: Vec<Vec<[f64; 2]>> = ways_by_id
        .values()
        .filter(|way| way.refs.iter().any(|r| r == &group.highway))
        .map(|way| way.geometry.iter().map(|&(lat, lon)| [lat, lon]).collect())
        .collect();
    let corridor_exit_rows = if !all_same_ref_segments.is_empty() {
        discover_nearby_exits(
            corridor_exit_rows,
            all_exit_nodes,
            &all_same_ref_segments,
            &group.highway,
        )
    } else {
        corridor_exit_rows
    };

    // Resolve semicolon-separated refs (e.g. "143A;143B" at a gore-point node).
    // Prefer individual ramp-level nodes when they exist; only keep the
    // gore-point split as fallback for ref values with no dedicated node.
    let corridor_exit_rows = resolve_semicolon_refs(corridor_exit_rows);
    let corridor_exit_rows = expand_compound_refs(corridor_exit_rows);
    let corridor_exit_rows = expand_comma_refs(corridor_exit_rows);
    let corridor_exit_rows = synthesize_merged_letter_refs(corridor_exit_rows);

    // Only keep exits that have at least a ref (exit number) or a name.
    // Bare motorway_junction nodes with neither are not useful to downstream
    // consumers and may be system interchanges or mapping artifacts.
    let corridor_exit_rows: Vec<ExitRow> = corridor_exit_rows
        .into_iter()
        .filter(|exit| {
            exit.ref_val.as_ref().is_some_and(|r| !r.is_empty())
                || exit.name.as_ref().is_some_and(|n| !n.is_empty())
        })
        .collect();

    let exits = order_exits_along_route(corridor_exit_rows, &route_segments);
    let edge_ids = matched_edge_ids(edge_rows, &group.highway, &assigned_way_ids);

    let source_way_ids: Vec<i64> = assigned_way_ids.into_iter().collect();
    if route_segments.len() > 1 {
        tracing::warn!(
            "{} {} (relation {}) still exports as {} disconnected corridor segments",
            group.highway,
            canonical_direction,
            group.root_relation_id,
            route_segments.len()
        );
    } else {
        tracing::info!(
            "{} {} (relation {}) resolved to a continuous corridor using {} ways",
            group.highway,
            canonical_direction,
            group.root_relation_id,
            source_way_ids.len()
        );
    }

    Ok(Some(CorridorDraft {
        corridor_id,
        highway: group.highway.clone(),
        canonical_direction,
        root_relation_id: group.root_relation_id,
        geometry_json,
        source_way_ids,
        edge_ids,
        exits,
    }))
}

fn append_points_dedup(segment: &mut Vec<[f64; 2]>, points: &[[f64; 2]]) {
    let skip_first = usize::from(
        !segment.is_empty()
            && !points.is_empty()
            && segment.last().copied() == points.first().copied(),
    );
    segment.extend(points.iter().skip(skip_first).copied());
}

fn build_connector_graph(ways: &[&RouteWay]) -> ConnectorGraph {
    let mut adjacency: HashMap<i64, Vec<ConnectorArc>> = HashMap::new();
    for way in ways {
        if !CONNECTOR_HIGHWAY_TYPES.contains(&way.highway_type.as_str()) {
            continue;
        }
        if way.nodes.len() < 2 || way.nodes.len() != way.geometry.len() {
            continue;
        }

        for idx in 0..way.nodes.len() - 1 {
            let start = way.nodes[idx];
            let end = way.nodes[idx + 1];
            let start_coord = [way.geometry[idx].0, way.geometry[idx].1];
            let end_coord = [way.geometry[idx + 1].0, way.geometry[idx + 1].1];
            let weight_m =
                haversine_distance(start_coord[0], start_coord[1], end_coord[0], end_coord[1])
                    .round()
                    .max(1.0) as u64;

            adjacency.entry(start).or_default().push(ConnectorArc {
                next: end,
                weight_m,
                way_id: way.way_id,
            });
            if !way.is_oneway {
                adjacency.entry(end).or_default().push(ConnectorArc {
                    next: start,
                    weight_m,
                    way_id: way.way_id,
                });
            }
        }
    }

    ConnectorGraph { adjacency }
}

fn ordered_group_members<'a>(
    group: &'a InterstateRouteGroup,
    ways_by_id: &HashMap<i64, RouteWay>,
) -> Vec<&'a crate::interstate_relations::InterstateRelationMember> {
    let members: Vec<_> = group
        .members
        .iter()
        .filter(|member| ways_by_id.contains_key(&member.way_id))
        .collect();
    let reversed: Vec<_> = members.iter().rev().copied().collect();
    if sequence_break_score(&reversed, ways_by_id) < sequence_break_score(&members, ways_by_id) {
        reversed
    } else {
        members
    }
}

fn sequence_break_score(
    members: &[&crate::interstate_relations::InterstateRelationMember],
    ways_by_id: &HashMap<i64, RouteWay>,
) -> usize {
    let mut breaks = 0usize;
    let mut prev_tail = None;
    for member in members {
        let Some(way) = ways_by_id.get(&member.way_id) else {
            continue;
        };
        if let Some(tail_node) = prev_tail {
            if !way.nodes.contains(&tail_node) {
                breaks += 1;
            }
        }
        prev_tail = way.nodes.last().copied();
    }
    breaks
}

fn adopt_relation_connector_paths(
    ordered_members: &[&crate::interstate_relations::InterstateRelationMember],
    assigned_way_ids: &mut BTreeSet<i64>,
    ways_by_id: &HashMap<i64, RouteWay>,
    connector_graph: &ConnectorGraph,
    allow_unassigned_interstate_connectors: bool,
) {
    loop {
        let assigned_ways: Vec<&RouteWay> = assigned_way_ids
            .iter()
            .filter_map(|way_id| ways_by_id.get(way_id))
            .collect();
        let components = way_connected_components(&assigned_ways);
        if components.len() <= 1 {
            break;
        }

        let mut component_by_node: HashMap<i64, i32> = HashMap::new();
        for (component_id, nodes) in &components {
            for &node_id in nodes {
                component_by_node.insert(node_id, *component_id);
            }
        }

        let mut adopted_this_round = 0usize;
        for pair in ordered_members.windows(2) {
            let Some(prev_way) = ways_by_id.get(&pair[0].way_id) else {
                continue;
            };
            let Some(next_way) = ways_by_id.get(&pair[1].way_id) else {
                continue;
            };
            let allowed_refs = allowed_refs_for_pair(prev_way, next_way);

            let prev_components: HashSet<i32> = prev_way
                .nodes
                .iter()
                .filter_map(|node_id| component_by_node.get(node_id).copied())
                .collect();
            let next_components: HashSet<i32> = next_way
                .nodes
                .iter()
                .filter_map(|node_id| component_by_node.get(node_id).copied())
                .collect();
            if !prev_components.is_empty()
                && !next_components.is_empty()
                && prev_components
                    .iter()
                    .any(|component_id| next_components.contains(component_id))
            {
                continue;
            }

            let source_nodes: HashSet<i64> = prev_way.nodes.iter().copied().collect();
            let target_nodes: HashMap<i64, i32> = next_way
                .nodes
                .iter()
                .copied()
                .map(|node_id| (node_id, 0))
                .collect();
            let Some((_cost_m, _target_component, path_way_ids)) = shortest_connector_path_to_any(
                &source_nodes,
                &target_nodes,
                connector_graph,
                ways_by_id,
                &allowed_refs,
                assigned_way_ids,
                allow_unassigned_interstate_connectors,
                false,
                None,
            )
            .or_else(|| {
                shortest_connector_path_to_any(
                    &source_nodes,
                    &target_nodes,
                    connector_graph,
                    ways_by_id,
                    &allowed_refs,
                    assigned_way_ids,
                    allow_unassigned_interstate_connectors,
                    true,
                    Some(SHORT_FALLBACK_CONNECTOR_MAX_COST_M),
                )
            }) else {
                continue;
            };

            for way_id in path_way_ids {
                adopted_this_round += usize::from(assigned_way_ids.insert(way_id));
            }
        }

        if adopted_this_round == 0 {
            break;
        }
    }
}

fn adopt_connector_paths(
    assigned_way_ids: &mut BTreeSet<i64>,
    ways_by_id: &HashMap<i64, RouteWay>,
    connector_graph: &ConnectorGraph,
    route_highway: &str,
    allow_unassigned_interstate_connectors: bool,
) {
    let allowed_refs = allowed_refs_for_route(route_highway, assigned_way_ids, ways_by_id);

    loop {
        let assigned_ways: Vec<&RouteWay> = assigned_way_ids
            .iter()
            .filter_map(|way_id| ways_by_id.get(way_id))
            .collect();
        let components = way_connected_components(&assigned_ways);
        if components.len() <= 1 {
            break;
        }

        let mut best_path: Option<(u64, Vec<i64>)> = None;
        for (source_component, source_nodes) in &components {
            let mut target_nodes: HashMap<i64, i32> = HashMap::new();
            for (target_component, nodes) in &components {
                if target_component == source_component {
                    continue;
                }
                for &node_id in nodes {
                    target_nodes.insert(node_id, *target_component);
                }
            }

            let Some(candidate) = shortest_connector_path_to_any(
                source_nodes,
                &target_nodes,
                connector_graph,
                ways_by_id,
                &allowed_refs,
                assigned_way_ids,
                allow_unassigned_interstate_connectors,
                false,
                None,
            )
            .or_else(|| {
                shortest_connector_path_to_any(
                    source_nodes,
                    &target_nodes,
                    connector_graph,
                    ways_by_id,
                    &allowed_refs,
                    assigned_way_ids,
                    allow_unassigned_interstate_connectors,
                    true,
                    Some(SHORT_FALLBACK_CONNECTOR_MAX_COST_M),
                )
            }) else {
                continue;
            };
            if best_path
                .as_ref()
                .is_none_or(|(best_cost, _)| candidate.0 < *best_cost)
            {
                best_path = Some((candidate.0, candidate.2));
            }
        }

        let Some((_cost_m, path_way_ids)) = best_path else {
            break;
        };

        let mut adopted_this_round = 0usize;
        for way_id in path_way_ids {
            adopted_this_round += usize::from(assigned_way_ids.insert(way_id));
        }
        if adopted_this_round == 0 {
            break;
        }
    }
}

fn shortest_connector_path_to_any(
    sources: &HashSet<i64>,
    targets_by_node: &HashMap<i64, i32>,
    connector_graph: &ConnectorGraph,
    ways_by_id: &HashMap<i64, RouteWay>,
    allowed_interstate_refs: &HashSet<String>,
    assigned_way_ids: &BTreeSet<i64>,
    allow_unassigned_interstate_connectors: bool,
    allow_short_high_class_fallback: bool,
    max_cost_m: Option<u64>,
) -> Option<(u64, i32, Vec<i64>)> {
    if sources.is_empty() || targets_by_node.is_empty() {
        return None;
    }

    let mut heap = BinaryHeap::new();
    let mut dist: HashMap<i64, u64> = HashMap::new();
    let mut prev: HashMap<i64, (i64, i64)> = HashMap::new();

    for &source in sources {
        dist.insert(source, 0);
        heap.push(SearchState {
            cost_m: 0,
            node: source,
        });
    }

    while let Some(SearchState { cost_m, node }) = heap.pop() {
        if cost_m > dist.get(&node).copied().unwrap_or(u64::MAX) {
            continue;
        }
        if let Some(&target_component) = targets_by_node.get(&node) {
            if !sources.contains(&node) {
                let mut way_ids = Vec::new();
                let mut current = node;
                while !sources.contains(&current) {
                    let &(prev_node, way_id) = prev.get(&current)?;
                    way_ids.push(way_id);
                    current = prev_node;
                }
                way_ids.reverse();
                way_ids.dedup();
                return Some((cost_m, target_component, way_ids));
            }
        }

        let Some(arcs) = connector_graph.adjacency.get(&node) else {
            continue;
        };
        for arc in arcs {
            let Some(way) = ways_by_id.get(&arc.way_id) else {
                continue;
            };
            if !connector_way_allowed_for_refs(
                way,
                allowed_interstate_refs,
                assigned_way_ids,
                allow_unassigned_interstate_connectors,
                allow_short_high_class_fallback,
            ) {
                continue;
            }
            let next_cost = cost_m.saturating_add(arc.weight_m);
            if max_cost_m.is_some_and(|limit| next_cost > limit) {
                continue;
            }
            if next_cost >= dist.get(&arc.next).copied().unwrap_or(u64::MAX) {
                continue;
            }
            dist.insert(arc.next, next_cost);
            prev.insert(arc.next, (node, arc.way_id));
            heap.push(SearchState {
                cost_m: next_cost,
                node: arc.next,
            });
        }
    }

    None
}

fn way_connected_components(ways: &[&RouteWay]) -> Vec<(i32, HashSet<i64>)> {
    let mut adjacency: HashMap<i64, HashSet<i64>> = HashMap::new();
    for way in ways {
        for pair in way.nodes.windows(2) {
            adjacency.entry(pair[0]).or_default().insert(pair[1]);
            adjacency.entry(pair[1]).or_default().insert(pair[0]);
        }
    }

    let mut components = Vec::new();
    let mut component_idx = 0_i32;
    let mut visited: HashSet<i64> = HashSet::new();
    let mut sorted_nodes: Vec<i64> = adjacency.keys().copied().collect();
    sorted_nodes.sort_unstable();

    for seed in sorted_nodes {
        if !visited.insert(seed) {
            continue;
        }

        let mut frontier = vec![seed];
        let mut component_nodes = HashSet::from([seed]);
        while let Some(node) = frontier.pop() {
            let Some(neighbors) = adjacency.get(&node) else {
                continue;
            };
            for &next in neighbors {
                if visited.insert(next) {
                    component_nodes.insert(next);
                    frontier.push(next);
                }
            }
        }

        components.push((component_idx, component_nodes));
        component_idx += 1;
    }

    components
}

fn build_route_segments(ways: &[&RouteWay], canonical_direction: &str) -> Vec<Vec<[f64; 2]>> {
    if ways.is_empty() {
        return Vec::new();
    }

    let mut node_coords: HashMap<i64, [f64; 2]> = HashMap::new();
    let mut directed_adjacency: HashMap<i64, Vec<(i64, f64)>> = HashMap::new();
    let mut undirected_adjacency: HashMap<i64, HashSet<i64>> = HashMap::new();
    let mut arc_coords: HashMap<(i64, i64), Vec<[f64; 2]>> = HashMap::new();

    for way in ways {
        if way.nodes.len() < 2 || way.nodes.len() != way.geometry.len() {
            continue;
        }

        for (idx, &node_id) in way.nodes.iter().enumerate() {
            let coord = [way.geometry[idx].0, way.geometry[idx].1];
            node_coords.entry(node_id).or_insert(coord);
        }

        for idx in 0..way.nodes.len() - 1 {
            let start = way.nodes[idx];
            let end = way.nodes[idx + 1];
            let start_coord = [way.geometry[idx].0, way.geometry[idx].1];
            let end_coord = [way.geometry[idx + 1].0, way.geometry[idx + 1].1];
            let weight_m =
                haversine_distance(start_coord[0], start_coord[1], end_coord[0], end_coord[1]);

            directed_adjacency
                .entry(start)
                .or_default()
                .push((end, weight_m));
            arc_coords
                .entry((start, end))
                .or_insert_with(|| vec![start_coord, end_coord]);
            if !way.is_oneway {
                directed_adjacency
                    .entry(end)
                    .or_default()
                    .push((start, weight_m));
                arc_coords
                    .entry((end, start))
                    .or_insert_with(|| vec![end_coord, start_coord]);
            }

            undirected_adjacency.entry(start).or_default().insert(end);
            undirected_adjacency.entry(end).or_default().insert(start);
        }
    }

    let component_by_node = compute_components(&undirected_adjacency);
    let mut nodes_by_component: HashMap<i32, Vec<i64>> = HashMap::new();
    for (&node_id, &component) in &component_by_node {
        nodes_by_component
            .entry(component)
            .or_default()
            .push(node_id);
    }

    let mut segments = Vec::new();
    let mut component_keys: Vec<i32> = nodes_by_component.keys().copied().collect();
    component_keys.sort_unstable();
    for component in component_keys {
        let Some(nodes) = nodes_by_component.get(&component) else {
            continue;
        };
        let Some(path_nodes) = best_component_path(
            nodes,
            &node_coords,
            &directed_adjacency,
            &undirected_adjacency,
            canonical_direction,
        ) else {
            continue;
        };
        let mut points = Vec::new();
        for pair in path_nodes.windows(2) {
            let coords = arc_coords.get(&(pair[0], pair[1])).cloned().or_else(|| {
                arc_coords.get(&(pair[1], pair[0])).map(|coords| {
                    let mut reversed = coords.clone();
                    reversed.reverse();
                    reversed
                })
            });
            let Some(coords) = coords else {
                continue;
            };
            append_points_dedup(&mut points, &coords);
        }
        if points.len() >= 2 {
            if !polyline_matches_direction(&points, canonical_direction) {
                points.reverse();
            }
            segments.push(points);
        }
    }

    segments.sort_by(|a, b| {
        let a_key = segment_sort_key(a, canonical_direction);
        let b_key = segment_sort_key(b, canonical_direction);
        if matches!(canonical_direction, "south" | "west") {
            b_key.partial_cmp(&a_key).unwrap_or(Ordering::Equal)
        } else {
            a_key.partial_cmp(&b_key).unwrap_or(Ordering::Equal)
        }
    });
    segments
}

fn prune_micro_route_segments(segments: Vec<Vec<[f64; 2]>>) -> Vec<Vec<[f64; 2]>> {
    if segments.len() <= 1 {
        return segments;
    }

    let longest_segment_m = segments
        .iter()
        .map(|segment| segment_length_m(segment))
        .fold(0.0_f64, f64::max);
    if longest_segment_m <= 0.0 {
        return segments;
    }

    segments
        .into_iter()
        .filter(|segment| {
            let length_m = segment_length_m(segment);
            length_m >= DISCONNECTED_MICRO_SEGMENT_MAX_LENGTH_M
                || length_m >= longest_segment_m * DISCONNECTED_MICRO_SEGMENT_MAX_SHARE
        })
        .collect()
}

fn best_component_path(
    nodes: &[i64],
    node_coords: &HashMap<i64, [f64; 2]>,
    directed_adjacency: &HashMap<i64, Vec<(i64, f64)>>,
    undirected_adjacency: &HashMap<i64, HashSet<i64>>,
    canonical_direction: &str,
) -> Option<Vec<i64>> {
    if nodes.len() < 2 {
        return None;
    }

    let mut terminal_nodes: Vec<i64> = nodes
        .iter()
        .copied()
        .filter(|node| undirected_adjacency.get(node).map_or(0, HashSet::len) <= 1)
        .collect();
    if terminal_nodes.len() < 2 {
        terminal_nodes = nodes.to_vec();
    }

    let low_node = *terminal_nodes.iter().min_by(|a, b| {
        projection_for_direction(
            node_coords.get(a).copied().unwrap_or([0.0, 0.0]),
            canonical_direction,
        )
        .partial_cmp(&projection_for_direction(
            node_coords.get(b).copied().unwrap_or([0.0, 0.0]),
            canonical_direction,
        ))
        .unwrap_or(Ordering::Equal)
    })?;
    let high_node = *terminal_nodes.iter().max_by(|a, b| {
        projection_for_direction(
            node_coords.get(a).copied().unwrap_or([0.0, 0.0]),
            canonical_direction,
        )
        .partial_cmp(&projection_for_direction(
            node_coords.get(b).copied().unwrap_or([0.0, 0.0]),
            canonical_direction,
        ))
        .unwrap_or(Ordering::Equal)
    })?;
    let (start_node, end_node) = if matches!(canonical_direction, "south" | "west") {
        (high_node, low_node)
    } else {
        (low_node, high_node)
    };
    if start_node == end_node {
        return None;
    }

    shortest_path(start_node, end_node, directed_adjacency).or_else(|| {
        shortest_path_undirected(start_node, end_node, undirected_adjacency, node_coords)
    })
}

fn shortest_path(
    start: i64,
    end: i64,
    adjacency: &HashMap<i64, Vec<(i64, f64)>>,
) -> Option<Vec<i64>> {
    let mut heap = BinaryHeap::new();
    let mut dist: HashMap<i64, u64> = HashMap::new();
    let mut prev: HashMap<i64, i64> = HashMap::new();

    dist.insert(start, 0);
    heap.push(SearchState {
        cost_m: 0,
        node: start,
    });

    while let Some(SearchState { cost_m, node }) = heap.pop() {
        if node == end {
            break;
        }
        if cost_m > dist.get(&node).copied().unwrap_or(u64::MAX) {
            continue;
        }
        let Some(neighbors) = adjacency.get(&node) else {
            continue;
        };
        for &(next, weight_m) in neighbors {
            let next_cost = cost_m.saturating_add(weight_m.round().max(1.0) as u64);
            if next_cost >= dist.get(&next).copied().unwrap_or(u64::MAX) {
                continue;
            }
            dist.insert(next, next_cost);
            prev.insert(next, node);
            heap.push(SearchState {
                cost_m: next_cost,
                node: next,
            });
        }
    }

    if !dist.contains_key(&end) {
        return None;
    }

    let mut path = vec![end];
    let mut current = end;
    while current != start {
        current = *prev.get(&current)?;
        path.push(current);
    }
    path.reverse();
    Some(path)
}

fn shortest_path_undirected(
    start: i64,
    end: i64,
    adjacency: &HashMap<i64, HashSet<i64>>,
    node_coords: &HashMap<i64, [f64; 2]>,
) -> Option<Vec<i64>> {
    let mut weighted: HashMap<i64, Vec<(i64, f64)>> = HashMap::new();
    for (&node, neighbors) in adjacency {
        for &next in neighbors {
            let coord_a = node_coords.get(&node).copied().unwrap_or([0.0, 0.0]);
            let coord_b = node_coords.get(&next).copied().unwrap_or([0.0, 0.0]);
            let weight_m = haversine_distance(coord_a[0], coord_a[1], coord_b[0], coord_b[1]);
            weighted.entry(node).or_default().push((next, weight_m));
        }
    }
    shortest_path(start, end, &weighted)
}

fn compute_components(adjacency: &HashMap<i64, HashSet<i64>>) -> HashMap<i64, i32> {
    let mut component_by_node = HashMap::new();
    let mut sorted_nodes: Vec<i64> = adjacency.keys().copied().collect();
    sorted_nodes.sort_unstable();
    let mut component_idx = 0_i32;

    for seed in sorted_nodes {
        if component_by_node.contains_key(&seed) {
            continue;
        }

        let mut frontier = vec![seed];
        component_by_node.insert(seed, component_idx);
        while let Some(node) = frontier.pop() {
            let Some(neighbors) = adjacency.get(&node) else {
                continue;
            };
            for &next in neighbors {
                if component_by_node.insert(next, component_idx).is_none() {
                    frontier.push(next);
                }
            }
        }
        component_idx += 1;
    }

    component_by_node
}

/// Resolve semicolon-separated exit refs (e.g. "143A;143B" at a gore-point
/// node) by preferring individual ramp-level nodes when they exist.
///
/// For each semicolon ref like "143A;143B" on node X:
///   - If a separate node Y already carries ref "143A", keep Y and drop
///     the "143A" part from X.  This ensures each exit number maps to its
///     own physical ramp node with distinct lat/lon.
///   - If no dedicated node carries "143A", create a split entry at node X
///     as a fallback (same coordinates, but at least the ref is recorded).
///   - If ALL parts are covered by individual nodes, drop node X entirely.
fn resolve_semicolon_refs(exits: Vec<ExitRow>) -> Vec<ExitRow> {
    // Build index of which individual (non-semicolon) refs already exist
    let mut individual_refs: HashSet<String> = HashSet::new();
    for exit in &exits {
        if let Some(ref r) = exit.ref_val {
            if !r.contains(';') {
                individual_refs.insert(r.clone());
            }
        }
    }

    let mut result = Vec::with_capacity(exits.len());
    for exit in exits {
        let Some(ref ref_val) = exit.ref_val else {
            result.push(exit);
            continue;
        };
        if !ref_val.contains(';') {
            result.push(exit);
            continue;
        }

        // Semicolon-separated: split and only keep parts without a dedicated node
        let parts: Vec<&str> = ref_val.split(';').map(|s| s.trim()).filter(|s| !s.is_empty()).collect();
        for part in parts {
            if individual_refs.contains(part) {
                // A dedicated ramp node already carries this ref — skip the
                // gore-point duplicate so the ramp-level coordinates are used.
                continue;
            }
            // Use a distinct exit_id per split part so downstream consumers
            // can differentiate them (the original node may host multiple exits).
            result.push(ExitRow {
                exit_id: format!("{}:{}", exit.exit_id, part),
                highway: exit.highway.clone(),
                graph_node: exit.graph_node,
                ref_val: Some(part.to_string()),
                name: exit.name.clone(),
                lat: exit.lat,
                lon: exit.lon,
            });
        }
    }
    result
}

fn order_exits_along_route(
    exits: Vec<ExitRow>,
    segments: &[Vec<[f64; 2]>],
) -> Vec<CorridorExitDraft> {
    let cumulative_points = cumulative_segment_points(segments);
    let mut ordered: Vec<CorridorExitDraft> = exits
        .into_iter()
        .map(|exit| CorridorExitDraft {
            exit_id: exit.exit_id,
            ref_val: exit.ref_val,
            name: exit.name,
            lat: exit.lat,
            lon: exit.lon,
            sort_key_m: closest_distance_along_route(&cumulative_points, [exit.lat, exit.lon]),
        })
        .collect();

    ordered.sort_by(|a, b| {
        a.sort_key_m
            .partial_cmp(&b.sort_key_m)
            .unwrap_or(Ordering::Equal)
            .then_with(|| a.exit_id.cmp(&b.exit_id))
    });
    ordered
}

fn cumulative_segment_points(segments: &[Vec<[f64; 2]>]) -> Vec<([f64; 2], f64)> {
    let mut result = Vec::new();
    let mut cumulative_m = 0.0;
    for segment in segments {
        for (idx, &point) in segment.iter().enumerate() {
            if idx > 0 {
                let prev = segment[idx - 1];
                cumulative_m += haversine_distance(prev[0], prev[1], point[0], point[1]);
            }
            result.push((point, cumulative_m));
        }
    }
    result
}

fn closest_distance_along_route(route_points: &[([f64; 2], f64)], target: [f64; 2]) -> f64 {
    route_points
        .iter()
        .map(|(point, distance_m)| {
            (
                haversine_distance(point[0], point[1], target[0], target[1]),
                *distance_m,
            )
        })
        .min_by(|a, b| a.0.partial_cmp(&b.0).unwrap_or(Ordering::Equal))
        .map(|(_, distance_m)| distance_m)
        .unwrap_or(0.0)
}

/// Expand the set of corridor nodes by including nodes from ways that
/// share at least one node with an already-assigned way AND either:
/// (a) carry `route_highway` in their ref list, OR
/// (b) are `_link` ways (motorway_link, trunk_link, etc.) with no ref tags.
///
/// Case (a) captures exits on adjacent branch ways that carry the same
/// highway ref but were not part of the OSM route relation.
///
/// Case (b) captures exit nodes sitting on ramp ways that connect to the
/// corridor mainline. These ramps are typically tagged only as
/// `highway=motorway_link` without any ref — the exit nodes
/// (`motorway_junction`) on them carry the actual exit number.
fn expand_nodes_with_adjacent_ways(
    assigned_nodes: &HashSet<i64>,
    route_highway: &str,
    ways_by_id: &HashMap<i64, RouteWay>,
) -> HashSet<i64> {
    let mut expanded = assigned_nodes.clone();

    // Three-hop expansion: first expand through same-ref and blank-link ways,
    // then expand twice more through blank-link ways only (catches ramps
    // connected via intermediate junction nodes, including exits at the
    // far end of longer ramp sequences).
    for hop in 0..3 {
        let frontier = expanded.clone();
        for way in ways_by_id.values() {
            let has_same_ref = way.refs.iter().any(|r| r == route_highway);
            let is_blank_link = way.highway_type.ends_with("_link") && way.refs.is_empty();
            // First hop: same-ref or blank links; second hop: blank links only
            if hop == 0 && !has_same_ref && !is_blank_link {
                continue;
            }
            if hop >= 1 && !is_blank_link {
                continue;
            }
            if way.nodes.iter().any(|node| frontier.contains(node)) {
                expanded.extend(way.nodes.iter().copied());
            }
        }
    }
    expanded
}

/// Discover sibling exits by base number proximity. If the corridor has
/// exit "100B", search for "100A", "100C", etc. within 2km in the full
/// exit node set. This catches lettered sub-exits on separate ramp ways
/// that aren't reachable through graph expansion.
fn discover_sibling_exits(
    exits: Vec<ExitRow>,
    all_exit_nodes: &HashMap<i64, ExitNodeInfo>,
) -> Vec<ExitRow> {
    const MAX_DISTANCE_M: f64 = 5_000.0;

    // Collect base numbers and their positions from existing corridor exits
    let mut base_positions: HashMap<String, Vec<(f64, f64)>> = HashMap::new();
    let mut known_refs: HashSet<String> = HashSet::new();
    let mut known_node_ids: HashSet<i64> = HashSet::new();
    let highway = exits.first().map(|e| e.highway.clone()).unwrap_or_default();

    for exit in &exits {
        known_node_ids.insert(exit.graph_node);
        if let Some(ref r) = exit.ref_val {
            known_refs.insert(r.clone());
            // Extract base number (e.g., "100" from "100B")
            let base = r
                .trim_end_matches(|c: char| c.is_ascii_uppercase())
                .to_string();
            if !base.is_empty() && base.chars().all(|c| c.is_ascii_digit()) {
                base_positions
                    .entry(base)
                    .or_default()
                    .push((exit.lat, exit.lon));
            }
        }
    }

    // Build index of all exit nodes by their base number
    let mut exits_by_base: HashMap<String, Vec<(i64, &ExitNodeInfo)>> = HashMap::new();
    for (&node_id, info) in all_exit_nodes {
        if known_node_ids.contains(&node_id) {
            continue;
        }
        if let Some(ref r) = info.ref_val {
            let base = r
                .trim_end_matches(|c: char| c.is_ascii_uppercase())
                .to_string();
            if !base.is_empty() && base.chars().all(|c| c.is_ascii_digit()) {
                exits_by_base
                    .entry(base)
                    .or_default()
                    .push((node_id, info));
            }
        }
    }

    let mut new_exits = Vec::new();
    for (base, positions) in &base_positions {
        let Some(candidates) = exits_by_base.get(base) else {
            continue;
        };
        for &(node_id, info) in candidates {
            let ref_val = info.ref_val.as_deref().unwrap_or("");
            if known_refs.contains(ref_val) {
                continue;
            }
            // Check if the candidate is within MAX_DISTANCE_M of any existing exit
            let close_enough = positions.iter().any(|&(lat, lon)| {
                haversine_m(lat, lon, info.lat, info.lon) < MAX_DISTANCE_M
            });
            if close_enough {
                new_exits.push(ExitRow {
                    exit_id: format!("node/{}", node_id),
                    highway: highway.clone(),
                    graph_node: node_id,
                    ref_val: info.ref_val.clone(),
                    name: info.name.clone(),
                    lat: info.lat,
                    lon: info.lon,
                });
                known_refs.insert(ref_val.to_string());
            }
        }
    }

    let mut result = exits;
    result.extend(new_exits);
    result
}

fn haversine_m(lat1: f64, lon1: f64, lat2: f64, lon2: f64) -> f64 {
    let r = 6_371_000.0_f64;
    let dlat = (lat2 - lat1).to_radians();
    let dlon = (lon2 - lon1).to_radians();
    let a = (dlat / 2.0).sin().powi(2)
        + lat1.to_radians().cos() * lat2.to_radians().cos() * (dlon / 2.0).sin().powi(2);
    2.0 * r * a.sqrt().asin()
}

/// Minimum distance from a point to a polyline segment (point-to-segment).
fn point_to_segment_distance_m(
    plat: f64,
    plon: f64,
    alat: f64,
    alon: f64,
    blat: f64,
    blon: f64,
) -> f64 {
    // Project point onto segment in lat/lon space, then compute haversine
    let dx = blon - alon;
    let dy = blat - alat;
    let len_sq = dx * dx + dy * dy;
    if len_sq < 1e-16 {
        return haversine_m(plat, plon, alat, alon);
    }
    let t = ((plon - alon) * dx + (plat - alat) * dy) / len_sq;
    let t = t.clamp(0.0, 1.0);
    let closest_lat = alat + t * dy;
    let closest_lon = alon + t * dx;
    haversine_m(plat, plon, closest_lat, closest_lon)
}

/// Minimum distance from a point to a route geometry (multi-segment polyline).
fn point_to_route_distance_m(lat: f64, lon: f64, route_segments: &[Vec<[f64; 2]>]) -> f64 {
    let mut min_dist = f64::MAX;
    for segment in route_segments {
        for window in segment.windows(2) {
            let d = point_to_segment_distance_m(
                lat,
                lon,
                window[0][0],
                window[0][1],
                window[1][0],
                window[1][1],
            );
            if d < min_dist {
                min_dist = d;
            }
        }
    }
    min_dist
}

/// Discover exit nodes near the corridor geometry that weren't found via
/// graph expansion. Uses a bounding-box pre-filter then checks point-to-polyline
/// distance. Catches exits on distant ramps or service roads.
fn discover_nearby_exits(
    exits: Vec<ExitRow>,
    all_exit_nodes: &HashMap<i64, ExitNodeInfo>,
    route_segments: &[Vec<[f64; 2]>],
    highway: &str,
) -> Vec<ExitRow> {
    const MAX_DISTANCE_M: f64 = 5_000.0;
    // Approximate degree buffer for the bounding box pre-filter (~10km)
    const BBOX_BUFFER_DEG: f64 = 0.10;

    // Build bounding box from route geometry
    let mut min_lat = f64::MAX;
    let mut max_lat = f64::MIN;
    let mut min_lon = f64::MAX;
    let mut max_lon = f64::MIN;
    for segment in route_segments {
        for point in segment {
            min_lat = min_lat.min(point[0]);
            max_lat = max_lat.max(point[0]);
            min_lon = min_lon.min(point[1]);
            max_lon = max_lon.max(point[1]);
        }
    }
    min_lat -= BBOX_BUFFER_DEG;
    max_lat += BBOX_BUFFER_DEG;
    min_lon -= BBOX_BUFFER_DEG;
    max_lon += BBOX_BUFFER_DEG;

    let known_nodes: HashSet<i64> = exits.iter().map(|e| e.graph_node).collect();
    let mut known_refs: HashSet<String> = HashSet::new();
    for exit in &exits {
        if let Some(ref r) = exit.ref_val {
            known_refs.insert(r.clone());
        }
    }

    let mut new_exits = Vec::new();
    for (&node_id, info) in all_exit_nodes {
        if known_nodes.contains(&node_id) {
            continue;
        }
        // Skip exits without a ref — we can't match them anyway
        let Some(ref ref_val) = info.ref_val else {
            continue;
        };
        if ref_val.is_empty() || known_refs.contains(ref_val) {
            continue;
        }
        // Bounding box pre-filter
        if info.lat < min_lat || info.lat > max_lat || info.lon < min_lon || info.lon > max_lon {
            continue;
        }
        // Precise distance check
        let dist = point_to_route_distance_m(info.lat, info.lon, route_segments);
        if dist <= MAX_DISTANCE_M {
            new_exits.push(ExitRow {
                exit_id: format!("node/{}", node_id),
                highway: highway.to_string(),
                graph_node: node_id,
                ref_val: info.ref_val.clone(),
                name: info.name.clone(),
                lat: info.lat,
                lon: info.lon,
            });
            known_refs.insert(ref_val.clone());
        }
    }

    let mut result = exits;
    result.extend(new_exits);
    result
}

/// Split semicolon-separated exit refs (e.g. "143A;143B") into individual
/// entries, preferring dedicated ramp nodes when they exist.
fn resolve_semicolon_refs(exits: Vec<ExitRow>) -> Vec<ExitRow> {
    let mut individual_refs: HashSet<String> = HashSet::new();
    for exit in &exits {
        if let Some(ref r) = exit.ref_val {
            if !r.contains(';') {
                individual_refs.insert(r.clone());
            }
        }
    }

    let mut result = Vec::with_capacity(exits.len());
    for exit in exits {
        let Some(ref ref_val) = exit.ref_val else {
            result.push(exit);
            continue;
        };
        if !ref_val.contains(';') {
            result.push(exit);
            continue;
        }

        // Keep the original combined form so it still matches ground truth
        // databases that store "106A;106B" as a literal sign number.
        result.push(exit.clone());

        let parts: Vec<&str> = ref_val
            .split(';')
            .map(|s| s.trim())
            .filter(|s| !s.is_empty())
            .collect();
        for part in parts {
            if individual_refs.contains(part) {
                continue;
            }
            result.push(ExitRow {
                exit_id: format!("{}:{}", exit.exit_id, part),
                highway: exit.highway.clone(),
                graph_node: exit.graph_node,
                ref_val: Some(part.to_string()),
                name: exit.name.clone(),
                lat: exit.lat,
                lon: exit.lon,
            });
        }
    }
    result
}

/// Expand compound letter-range exits like "17A-B" into individual entries
/// ("17A", "17B") while keeping the original compound form.
fn expand_compound_refs(exits: Vec<ExitRow>) -> Vec<ExitRow> {
    let mut individual_refs: HashSet<String> = HashSet::new();
    for exit in &exits {
        if let Some(ref r) = exit.ref_val {
            individual_refs.insert(r.clone());
        }
    }

    let mut result = Vec::with_capacity(exits.len());
    for exit in exits {
        result.push(exit.clone());

        let Some(ref ref_val) = exit.ref_val else {
            continue;
        };

        // Match patterns like "17A-B", "17A-B-C", "17A-C"
        let bytes = ref_val.as_bytes();
        // Find the base number and letter range
        let mut i = 0;
        while i < bytes.len() && bytes[i].is_ascii_digit() {
            i += 1;
        }
        if i == 0 || i >= bytes.len() {
            continue;
        }
        let base = &ref_val[..i];
        let suffix = &ref_val[i..];

        // Parse letter-dash-letter patterns: "A-B", "A-B-C", "A-C"
        let letters: Vec<u8> = suffix
            .split('-')
            .filter_map(|s| {
                let s = s.trim();
                if s.len() == 1 && s.as_bytes()[0].is_ascii_uppercase() {
                    Some(s.as_bytes()[0])
                } else {
                    None
                }
            })
            .collect();

        if letters.len() < 2 {
            continue;
        }

        let first = *letters.first().unwrap();
        let last = *letters.last().unwrap();
        // Support both "A-B" and reverse "B-A" ordering
        let (start, end) = if first <= last {
            (first, last)
        } else {
            (last, first)
        };
        if (end - start) > 5 {
            continue;
        }

        for c in start..=end {
            let expanded = format!("{}{}", base, c as char);
            if individual_refs.contains(&expanded) {
                continue; // Already exists as a dedicated exit
            }
            result.push(ExitRow {
                exit_id: format!("{}:{}", exit.exit_id, expanded),
                highway: exit.highway.clone(),
                graph_node: exit.graph_node,
                ref_val: Some(expanded),
                name: exit.name.clone(),
                lat: exit.lat,
                lon: exit.lon,
            });
        }
    }
    result
}

/// Expand comma-separated exit refs like "11A,B" into "11A" and "11B",
/// and "12A,12B" into individual entries. Keeps the original compound form.
fn expand_comma_refs(exits: Vec<ExitRow>) -> Vec<ExitRow> {
    let mut individual_refs: HashSet<String> = HashSet::new();
    for exit in &exits {
        if let Some(ref r) = exit.ref_val {
            if !r.contains(',') {
                individual_refs.insert(r.clone());
            }
        }
    }

    let mut result = Vec::with_capacity(exits.len());
    for exit in exits {
        let Some(ref ref_val) = exit.ref_val else {
            result.push(exit);
            continue;
        };
        if !ref_val.contains(',') {
            result.push(exit);
            continue;
        }

        result.push(exit.clone()); // Keep original compound form

        // Find the numeric base of the first part
        let first_part = ref_val.split(',').next().unwrap_or("").trim();
        let base_len = first_part
            .bytes()
            .take_while(|b| b.is_ascii_digit())
            .count();
        let base = &first_part[..base_len];

        for part in ref_val.split(',') {
            let part = part.trim();
            if part.is_empty() {
                continue;
            }

            // If the part starts with a digit, it's a full ref (e.g., "12A" in "12A,12B")
            // If it's just letters, prepend the base (e.g., "B" in "11A,B" → "11B")
            let expanded = if part.bytes().next().is_some_and(|b| b.is_ascii_digit()) {
                part.to_string()
            } else if !base.is_empty() {
                format!("{}{}", base, part)
            } else {
                continue;
            };

            if individual_refs.contains(&expanded) {
                continue;
            }
            result.push(ExitRow {
                exit_id: format!("{}:{}", exit.exit_id, expanded),
                highway: exit.highway.clone(),
                graph_node: exit.graph_node,
                ref_val: Some(expanded),
                name: exit.name.clone(),
                lat: exit.lat,
                lon: exit.lon,
            });
        }
    }
    result
}

/// Synthesize merged letter refs like "214AB" from adjacent individual
/// letter exits ("214A", "214B") at the same graph node. Many exit
/// databases store the combined form rather than individual letters.
fn synthesize_merged_letter_refs(exits: Vec<ExitRow>) -> Vec<ExitRow> {
    // Collect lettered exits by their base number and letter.
    struct LetterEntry {
        ch: char,
        idx: usize,
        node: i64,
    }
    let mut by_base: HashMap<String, Vec<LetterEntry>> = HashMap::new();
    for (idx, exit) in exits.iter().enumerate() {
        let Some(ref ref_val) = exit.ref_val else {
            continue;
        };
        let bytes = ref_val.as_bytes();
        if bytes.len() < 2 {
            continue;
        }
        let last = *bytes.last().unwrap();
        if !last.is_ascii_uppercase() {
            continue;
        }
        let base = &ref_val[..ref_val.len() - 1];
        if !base.bytes().all(|b| b.is_ascii_digit()) {
            continue;
        }
        by_base
            .entry(base.to_string())
            .or_default()
            .push(LetterEntry {
                ch: last as char,
                idx,
                node: exit.graph_node,
            });
    }

    let existing_refs: HashSet<String> = exits
        .iter()
        .filter_map(|e| e.ref_val.clone())
        .collect();

    let mut new_exits = Vec::new();
    for (base, entries) in &by_base {
        // Strategy 1: group by node (preserves per-node merged forms like "1CD")
        let mut by_node: HashMap<i64, Vec<&LetterEntry>> = HashMap::new();
        for entry in entries {
            by_node.entry(entry.node).or_default().push(entry);
        }
        for (_node, mut node_entries) in by_node {
            if node_entries.len() < 2 {
                continue;
            }
            node_entries.sort_by_key(|e| e.ch);
            let merged = format!(
                "{}{}",
                base,
                node_entries.iter().map(|e| e.ch).collect::<String>()
            );
            if existing_refs.contains(&merged) {
                continue;
            }
            let template = &exits[node_entries[0].idx];
            new_exits.push(ExitRow {
                exit_id: format!("{}:{}", template.exit_id, merged),
                highway: template.highway.clone(),
                graph_node: template.graph_node,
                ref_val: Some(merged),
                name: template.name.clone(),
                lat: template.lat,
                lon: template.lon,
            });
        }

        // Strategy 2: also generate the full merged form across all nodes
        if entries.len() >= 2 {
            let mut sorted: Vec<_> = entries.iter().collect();
            sorted.sort_by_key(|e| e.ch);
            sorted.dedup_by_key(|e| e.ch);
            if sorted.len() >= 2 {
                let merged = format!(
                    "{}{}",
                    base,
                    sorted.iter().map(|e| e.ch).collect::<String>()
                );
                if !existing_refs.contains(&merged)
                    && !new_exits.iter().any(|e| e.ref_val.as_deref() == Some(&merged))
                {
                    let template = &exits[sorted[0].idx];
                    new_exits.push(ExitRow {
                        exit_id: format!("{}:{}", template.exit_id, merged),
                        highway: template.highway.clone(),
                        graph_node: template.graph_node,
                        ref_val: Some(merged),
                        name: template.name.clone(),
                        lat: template.lat,
                        lon: template.lon,
                    });
                }
            }
        }
    }

    let mut result = exits;
    result.extend(new_exits);
    result
}

fn matched_edge_ids(
    edge_rows: &[HighwayEdgeRow],
    route_highway: &str,
    assigned_way_ids: &BTreeSet<i64>,
) -> Vec<String> {
    let mut edge_ids = Vec::new();
    for edge in edge_rows {
        if edge.highway != route_highway {
            continue;
        }
        if edge
            .source_way_ids
            .iter()
            .any(|way_id| assigned_way_ids.contains(way_id))
        {
            edge_ids.push(edge.edge_id.clone());
        }
    }
    edge_ids.sort();
    edge_ids.dedup();
    edge_ids
}

fn validate_edge_claims(drafts: &[CorridorDraft]) -> Result<(), anyhow::Error> {
    let mut claims: HashMap<String, (String, i32, String, i64)> = HashMap::new();
    for draft in drafts {
        for edge_id in &draft.edge_ids {
            match claims.entry(edge_id.clone()) {
                std::collections::hash_map::Entry::Vacant(entry) => {
                    entry.insert((
                        draft.highway.clone(),
                        draft.corridor_id,
                        draft.canonical_direction.clone(),
                        draft.root_relation_id,
                    ));
                }
                std::collections::hash_map::Entry::Occupied(entry) => {
                    let (existing_highway, existing_corridor_id, existing_direction, existing_root) =
                        entry.get();
                    if *existing_corridor_id == draft.corridor_id {
                        continue;
                    }
                    if *existing_root == draft.root_relation_id {
                        continue;
                    }
                    anyhow::bail!(
                        "edge claim conflict for highway {} edge {} between corridor {} (relation {}, {}) and corridor {} (relation {}, {})",
                        existing_highway,
                        edge_id,
                        existing_corridor_id,
                        existing_root,
                        existing_direction,
                        draft.corridor_id,
                        draft.root_relation_id,
                        draft.canonical_direction,
                    );
                }
            }
        }
    }
    Ok(())
}

/// Remove corridors that conflict on edge assignments with an earlier corridor.
/// Corridors are processed in insertion order (by highway number, then root relation,
/// then direction), so the first corridor for each highway claims edges first.
fn dedup_conflicting_corridors(drafts: Vec<CorridorDraft>) -> Vec<CorridorDraft> {
    let mut claimed_edges: HashMap<String, i64> = HashMap::new(); // edge_id -> root_relation_id
    let mut kept = Vec::with_capacity(drafts.len());

    for draft in drafts {
        let has_conflict = draft.edge_ids.iter().any(|edge_id| {
            claimed_edges
                .get(edge_id)
                .is_some_and(|&root| root != draft.root_relation_id)
        });

        if has_conflict {
            tracing::warn!(
                highway = %draft.highway,
                direction = %draft.canonical_direction,
                root_relation_id = draft.root_relation_id,
                edge_count = draft.edge_ids.len(),
                "dropping corridor with conflicting edge claims"
            );
            continue;
        }

        for edge_id in &draft.edge_ids {
            claimed_edges
                .entry(edge_id.clone())
                .or_insert(draft.root_relation_id);
        }
        kept.push(draft);
    }

    kept
}

async fn write_corridors(
    pool: &PgPool,
    drafts: &[CorridorDraft],
) -> Result<BuildCorridorsStats, anyhow::Error> {
    validate_edge_claims(drafts)?;

    let mut tx = pool.begin().await?;
    sqlx::query("DELETE FROM corridor_exits")
        .execute(&mut *tx)
        .await?;
    sqlx::query("DELETE FROM corridors")
        .execute(&mut *tx)
        .await?;
    sqlx::query("UPDATE highway_edges SET corridor_id = NULL")
        .execute(&mut *tx)
        .await?;

    for draft in drafts {
        sqlx::query(
            "INSERT INTO corridors \
                (corridor_id, highway, canonical_direction, root_relation_id, geometry_json, source_way_ids_json) \
             VALUES ($1, $2, $3, $4, $5, $6)",
        )
        .bind(draft.corridor_id)
        .bind(&draft.highway)
        .bind(&draft.canonical_direction)
        .bind(draft.root_relation_id)
        .bind(&draft.geometry_json)
        .bind(serde_json::to_string(&draft.source_way_ids).unwrap_or_else(|_| "[]".to_string()))
        .execute(&mut *tx)
        .await?;

        for (idx, exit) in draft.exits.iter().enumerate() {
            sqlx::query(
                "INSERT INTO corridor_exits \
                    (corridor_id, corridor_index, exit_id, ref, name, lat, lon) \
                 VALUES ($1, $2, $3, $4, $5, $6, $7)",
            )
            .bind(draft.corridor_id)
            .bind(idx as i32)
            .bind(&exit.exit_id)
            .bind(&exit.ref_val)
            .bind(&exit.name)
            .bind(exit.lat)
            .bind(exit.lon)
            .execute(&mut *tx)
            .await?;
        }
    }

    let mut edges_updated = 0usize;
    for draft in drafts {
        for edge_id in &draft.edge_ids {
            let result = sqlx::query("UPDATE highway_edges SET corridor_id = $2 WHERE id = $1")
                .bind(edge_id)
                .bind(draft.corridor_id)
                .execute(&mut *tx)
                .await?;
            edges_updated += result.rows_affected() as usize;
        }
    }

    tx.commit().await?;

    Ok(BuildCorridorsStats {
        corridors_created: drafts.len(),
        corridor_exits_created: drafts.iter().map(|draft| draft.exits.len()).sum(),
        edges_updated,
    })
}

fn serialize_geometry_geojson(segments: &[Vec<[f64; 2]>]) -> Result<String, anyhow::Error> {
    if segments.is_empty() {
        return Ok("{\"type\":\"LineString\",\"coordinates\":[]}".to_string());
    }

    if segments.len() == 1 {
        return Ok(serde_json::to_string(&serde_json::json!({
            "type": "LineString",
            "coordinates": segments[0]
                .iter()
                .map(|point| vec![point[1], point[0]])
                .collect::<Vec<_>>(),
        }))?);
    }

    Ok(serde_json::to_string(&serde_json::json!({
        "type": "MultiLineString",
        "coordinates": segments
            .iter()
            .map(|segment| {
                segment
                    .iter()
                    .map(|point| vec![point[1], point[0]])
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>(),
    }))?)
}

fn segment_length_m(segment: &[[f64; 2]]) -> f64 {
    segment
        .windows(2)
        .map(|pair| haversine_distance(pair[0][0], pair[0][1], pair[1][0], pair[1][1]))
        .sum()
}

fn segment_sort_key(segment: &[[f64; 2]], canonical_direction: &str) -> f64 {
    let first = segment.first().copied().unwrap_or([0.0, 0.0]);
    let last = segment.last().copied().unwrap_or(first);
    match canonical_direction {
        "east" | "west" => first[1].min(last[1]),
        _ => first[0].min(last[0]),
    }
}

fn projection_for_direction(point: [f64; 2], canonical_direction: &str) -> f64 {
    match canonical_direction {
        "east" | "west" => point[1],
        _ => point[0],
    }
}

fn polyline_matches_direction(polyline: &[[f64; 2]], canonical_direction: &str) -> bool {
    if polyline.len() < 2 {
        return true;
    }
    let first = polyline[0];
    let last = polyline[polyline.len() - 1];
    match canonical_direction {
        "north" => last[0] > first[0],
        "south" => last[0] < first[0],
        "east" => last[1] > first[1],
        "west" => last[1] < first[1],
        _ => true,
    }
}

fn parse_geojson_coords(raw: &str) -> Vec<(f64, f64)> {
    serde_json::from_str::<serde_json::Value>(raw)
        .ok()
        .and_then(|value| value.get("coordinates").cloned())
        .and_then(|coords| serde_json::from_value::<Vec<Vec<f64>>>(coords).ok())
        .unwrap_or_default()
        .into_iter()
        .filter_map(|pair| {
            if pair.len() >= 2 {
                Some((pair[1], pair[0]))
            } else {
                None
            }
        })
        .collect()
}

fn parse_way_ids_json(raw: &str) -> Vec<i64> {
    serde_json::from_str::<Vec<i64>>(raw).unwrap_or_default()
}

fn allowed_refs_for_pair(prev_way: &RouteWay, next_way: &RouteWay) -> HashSet<String> {
    prev_way
        .refs
        .iter()
        .chain(next_way.refs.iter())
        .cloned()
        .collect()
}

fn allowed_refs_for_route(
    route_highway: &str,
    assigned_way_ids: &BTreeSet<i64>,
    ways_by_id: &HashMap<i64, RouteWay>,
) -> HashSet<String> {
    let route_family = interstate_family_key(route_highway);
    let mut allowed_refs = HashSet::from([route_highway.to_string()]);
    for way_id in assigned_way_ids {
        let Some(way) = ways_by_id.get(way_id) else {
            continue;
        };
        for reference in &way.refs {
            if interstate_ref_allowed_for_route(reference, route_family.as_deref()) {
                allowed_refs.insert(reference.clone());
            }
        }
    }
    allowed_refs
}

fn interstate_family_key(reference: &str) -> Option<String> {
    let normalized = normalize_highway_ref(reference)?;
    let interstate = normalized.strip_prefix("I-")?;
    let digits: String = interstate
        .chars()
        .take_while(|char| char.is_ascii_digit())
        .collect();
    if digits.is_empty() {
        None
    } else {
        Some(digits)
    }
}

fn interstate_ref_allowed_for_route(reference: &str, route_family: Option<&str>) -> bool {
    if !is_interstate_highway_ref(reference) {
        return true;
    }
    match route_family {
        Some(family) => interstate_family_key(reference).as_deref() == Some(family),
        None => false,
    }
}

fn connector_way_allowed_for_refs(
    way: &RouteWay,
    allowed_refs: &HashSet<String>,
    assigned_way_ids: &BTreeSet<i64>,
    allow_unassigned_interstate_connectors: bool,
    allow_short_high_class_fallback: bool,
) -> bool {
    let has_interstate_ref = way
        .refs
        .iter()
        .any(|reference| is_interstate_highway_ref(reference));

    if has_interstate_ref
        && !allow_unassigned_interstate_connectors
        && !assigned_way_ids.contains(&way.way_id)
    {
        return false;
    }

    way.refs.is_empty()
        || way
            .refs
            .iter()
            .any(|reference| allowed_refs.contains(reference))
        || (allow_short_high_class_fallback
            && matches!(
                way.highway_type.as_str(),
                "motorway" | "motorway_link" | "trunk" | "trunk_link"
            ))
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeSet, HashMap, HashSet};

    use super::{
        allowed_refs_for_pair, allowed_refs_for_route, build_connector_graph,
        connector_way_allowed_for_refs, filter_route_groups, prune_micro_route_segments,
        shortest_connector_path_to_any, validate_edge_claims, ExitRow, HighwayEdgeRow, RouteWay,
        SHORT_FALLBACK_CONNECTOR_MAX_COST_M,
    };
    use crate::interstate_relations::{InterstateRelationMember, InterstateRouteGroup};

    fn route_way(
        way_id: i64,
        refs: &[&str],
        nodes: &[i64],
        geometry: &[(f64, f64)],
        highway_type: &str,
        is_oneway: bool,
    ) -> RouteWay {
        RouteWay {
            way_id,
            refs: refs.iter().map(|value| (*value).to_string()).collect(),
            nodes: nodes.to_vec(),
            geometry: geometry.to_vec(),
            highway_type: highway_type.to_string(),
            is_oneway,
        }
    }

    fn route_group(
        highway: &str,
        root_relation_id: i64,
        direction: Option<&str>,
        members: &[(i64, i64)],
    ) -> InterstateRouteGroup {
        InterstateRouteGroup {
            highway: highway.to_string(),
            root_relation_id,
            direction: direction.map(ToString::to_string),
            members: members
                .iter()
                .enumerate()
                .map(
                    |(sequence_index, (way_id, leaf_relation_id))| InterstateRelationMember {
                        way_id: *way_id,
                        highway: highway.to_string(),
                        root_relation_id,
                        leaf_relation_id: *leaf_relation_id,
                        direction: direction.map(ToString::to_string),
                        role: None,
                        sequence_index,
                    },
                )
                .collect(),
        }
    }

    #[test]
    fn filter_route_groups_drops_blank_group_when_directional_siblings_exist() {
        let filtered = filter_route_groups(vec![
            route_group("I-30", 100, Some("east"), &[(1, 101)]),
            route_group("I-30", 100, None, &[(2, 102), (3, 102)]),
            route_group("I-30", 100, Some("west"), &[(4, 103)]),
            route_group("I-41", 200, None, &[(5, 201)]),
        ]);

        assert_eq!(filtered.len(), 3);
        assert!(filtered
            .iter()
            .any(|group| group.highway == "I-30" && group.direction.as_deref() == Some("east")));
        assert!(filtered
            .iter()
            .any(|group| group.highway == "I-30" && group.direction.as_deref() == Some("west")));
        assert!(!filtered
            .iter()
            .any(|group| group.highway == "I-30" && group.direction.is_none()));
        assert!(filtered
            .iter()
            .any(|group| group.highway == "I-41" && group.direction.is_none()));
    }

    #[test]
    fn filter_route_groups_keeps_blank_groups_for_undirected_roots() {
        let filtered = filter_route_groups(vec![
            route_group("I-84", 300, None, &[(1, 301), (2, 301)]),
            route_group("I-84", 301, Some("east"), &[(3, 302)]),
        ]);

        assert_eq!(filtered.len(), 2);
        assert!(filtered
            .iter()
            .any(|group| group.root_relation_id == 300 && group.direction.is_none()));
        assert!(filtered.iter().any(
            |group| group.root_relation_id == 301 && group.direction.as_deref() == Some("east")
        ));
    }

    #[test]
    fn matched_edge_ids_ignore_edge_direction_when_way_membership_matches() {
        let westbound_corridor = "west";
        let edge_rows = vec![HighwayEdgeRow {
            edge_id: "edge/I-10/1/2".to_string(),
            highway: "I-10".to_string(),
            direction: Some("east".to_string()),
            source_way_ids: vec![1001, 1002],
        }];
        let assigned_way_ids = BTreeSet::from([1002_i64]);

        let edge_ids = super::matched_edge_ids(&edge_rows, "I-10", &assigned_way_ids);

        assert_eq!(westbound_corridor, "west");
        assert_eq!(edge_ids, vec!["edge/I-10/1/2".to_string()]);
    }

    #[test]
    fn validate_edge_claims_rejects_overlapping_corridor_assignments() {
        let shared_edge_id = "edge/I-10/1/2".to_string();
        let drafts = vec![
            super::CorridorDraft {
                corridor_id: 10,
                highway: "I-10".to_string(),
                canonical_direction: "west".to_string(),
                root_relation_id: 1000,
                geometry_json: "{\"type\":\"LineString\",\"coordinates\":[]}".to_string(),
                source_way_ids: vec![1, 2],
                edge_ids: vec![shared_edge_id.clone()],
                exits: vec![],
            },
            super::CorridorDraft {
                corridor_id: 11,
                highway: "I-10".to_string(),
                canonical_direction: "east".to_string(),
                root_relation_id: 1001,
                geometry_json: "{\"type\":\"LineString\",\"coordinates\":[]}".to_string(),
                source_way_ids: vec![3, 4],
                edge_ids: vec![shared_edge_id.clone()],
                exits: vec![],
            },
        ];

        let err = validate_edge_claims(&drafts).expect_err("overlap should fail loudly");
        let message = err.to_string();
        assert!(message.contains("edge/I-10/1/2"));
        assert!(message.contains("corridor 10"));
        assert!(message.contains("corridor 11"));
        assert!(message.contains("relation 1000"));
        assert!(message.contains("relation 1001"));
    }

    #[test]
    fn connector_policy_allows_same_highway_interstate_ways() {
        let way = route_way(
            10,
            &["I-96"],
            &[1, 2, 3],
            &[(42.0, -83.0), (42.0, -82.999), (42.0, -82.998)],
            "motorway",
            true,
        );

        assert!(connector_way_allowed_for_refs(
            &way,
            &HashSet::from(["I-96".to_string()]),
            &BTreeSet::new(),
            true,
            false,
        ));
    }

    #[test]
    fn connector_policy_rejects_other_interstate_ways() {
        let way = route_way(
            11,
            &["I-69"],
            &[1, 2],
            &[(42.0, -83.0), (42.0, -82.999)],
            "motorway",
            true,
        );

        assert!(!connector_way_allowed_for_refs(
            &way,
            &HashSet::from(["I-96".to_string()]),
            &BTreeSet::new(),
            true,
            false,
        ));
    }

    #[test]
    fn directional_connector_policy_rejects_unassigned_interstate_way_even_when_ref_matches() {
        let way = route_way(
            12,
            &["I-69C", "US-281"],
            &[1, 2],
            &[(26.2, -98.2), (26.21, -98.2)],
            "motorway",
            true,
        );

        assert!(!connector_way_allowed_for_refs(
            &way,
            &HashSet::from(["I-69C".to_string(), "US-281".to_string()]),
            &BTreeSet::new(),
            false,
            false,
        ));
    }

    #[test]
    fn pairwise_policy_inherits_official_concurrency_refs() {
        let prev_way = route_way(
            20,
            &["I-20", "I-55"],
            &[1, 2],
            &[(32.0, -90.2), (32.0, -90.19)],
            "motorway",
            true,
        );
        let next_way = route_way(
            21,
            &["I-20", "I-55"],
            &[3, 4],
            &[(32.0, -90.18), (32.0, -90.17)],
            "motorway",
            true,
        );
        let bridge_way = route_way(
            22,
            &["I-55"],
            &[2, 3],
            &[(32.0, -90.19), (32.0, -90.18)],
            "motorway",
            true,
        );

        let allowed_refs = allowed_refs_for_pair(&prev_way, &next_way);

        assert!(allowed_refs.contains("I-20"));
        assert!(allowed_refs.contains("I-55"));
        assert!(connector_way_allowed_for_refs(
            &bridge_way,
            &allowed_refs,
            &BTreeSet::new(),
            true,
            false,
        ));
    }

    #[test]
    fn pairwise_policy_allows_shared_non_interstate_concurrency_refs() {
        let prev_way = route_way(
            30,
            &["I-99", "US-220"],
            &[1, 2],
            &[(41.0, -77.6), (41.01, -77.59)],
            "motorway",
            true,
        );
        let next_way = route_way(
            31,
            &["I-99", "US-220"],
            &[3, 4],
            &[(41.02, -77.58), (41.03, -77.57)],
            "motorway",
            true,
        );
        let bridge_way = route_way(
            32,
            &["I-80", "US-220"],
            &[2, 5, 3],
            &[(41.01, -77.59), (41.015, -77.585), (41.02, -77.58)],
            "motorway",
            true,
        );

        let allowed_refs = allowed_refs_for_pair(&prev_way, &next_way);

        assert!(allowed_refs.contains("I-99"));
        assert!(allowed_refs.contains("US-220"));
        assert!(connector_way_allowed_for_refs(
            &bridge_way,
            &allowed_refs,
            &BTreeSet::new(),
            true,
            false,
        ));
    }

    #[test]
    fn route_policy_collects_all_assigned_refs() {
        let ways_by_id = HashMap::from([
            (
                40_i64,
                route_way(
                    40,
                    &["I-99", "US-220"],
                    &[1, 2],
                    &[(41.0, -77.6), (41.01, -77.59)],
                    "motorway",
                    true,
                ),
            ),
            (
                41_i64,
                route_way(
                    41,
                    &["I-99", "US-15"],
                    &[3, 4],
                    &[(41.2, -77.2), (41.21, -77.19)],
                    "motorway",
                    true,
                ),
            ),
        ]);
        let assigned = BTreeSet::from([40_i64, 41_i64]);

        let allowed_refs = allowed_refs_for_route("I-99", &assigned, &ways_by_id);

        assert!(allowed_refs.contains("I-99"));
        assert!(allowed_refs.contains("US-220"));
        assert!(allowed_refs.contains("US-15"));
    }

    #[test]
    fn route_policy_excludes_foreign_interstate_refs() {
        let ways_by_id = HashMap::from([
            (
                50_i64,
                route_way(
                    50,
                    &["I-35", "I-29", "US-71"],
                    &[1, 2],
                    &[(39.12, -94.58), (39.13, -94.58)],
                    "motorway",
                    true,
                ),
            ),
            (
                51_i64,
                route_way(
                    51,
                    &["I-35E", "MN-61"],
                    &[3, 4],
                    &[(44.95, -93.09), (44.96, -93.08)],
                    "motorway",
                    true,
                ),
            ),
        ]);
        let assigned = BTreeSet::from([50_i64, 51_i64]);

        let allowed_refs = allowed_refs_for_route("I-35", &assigned, &ways_by_id);

        assert!(allowed_refs.contains("I-35"));
        assert!(allowed_refs.contains("I-35E"));
        assert!(allowed_refs.contains("US-71"));
        assert!(allowed_refs.contains("MN-61"));
        assert!(!allowed_refs.contains("I-29"));
    }

    #[test]
    fn shortest_connector_path_can_use_same_highway_interstate_gap_fill() {
        let bridge_way = route_way(
            12,
            &["I-96"],
            &[1, 2, 3],
            &[(42.0, -83.0), (42.0, -82.999), (42.0, -82.998)],
            "motorway",
            true,
        );
        let ways_by_id = HashMap::from([(bridge_way.way_id, bridge_way.clone())]);
        let graph = build_connector_graph(&[ways_by_id.get(&bridge_way.way_id).unwrap()]);
        let sources = HashSet::from([1_i64]);
        let targets = HashMap::from([(3_i64, 1_i32)]);
        let assigned_way_ids = BTreeSet::new();

        let result = shortest_connector_path_to_any(
            &sources,
            &targets,
            &graph,
            &ways_by_id,
            &HashSet::from(["I-96".to_string()]),
            &assigned_way_ids,
            true,
            false,
            None,
        );

        assert_eq!(result.map(|(_, _, way_ids)| way_ids), Some(vec![12]));
    }

    #[test]
    fn directional_gap_fill_rejects_same_highway_interstate_connector() {
        let source_way = route_way(
            1,
            &["I-69C"],
            &[1, 2],
            &[(26.18, -98.23), (26.19, -98.23)],
            "motorway",
            true,
        );
        let target_way = route_way(
            2,
            &["I-69C"],
            &[5, 6],
            &[(26.22, -98.23), (26.23, -98.23)],
            "motorway",
            true,
        );
        let connector_way = route_way(
            3,
            &["I-69C", "US-281"],
            &[2, 3, 4, 5],
            &[
                (26.19, -98.23),
                (26.20, -98.23),
                (26.21, -98.23),
                (26.22, -98.23),
            ],
            "motorway",
            true,
        );
        let ways_by_id = HashMap::from([
            (source_way.way_id, source_way.clone()),
            (target_way.way_id, target_way.clone()),
            (connector_way.way_id, connector_way.clone()),
        ]);
        let graph = build_connector_graph(&[
            ways_by_id.get(&source_way.way_id).unwrap(),
            ways_by_id.get(&target_way.way_id).unwrap(),
            ways_by_id.get(&connector_way.way_id).unwrap(),
        ]);
        let sources = HashSet::from([2_i64]);
        let targets = HashMap::from([(5_i64, 0_i32)]);
        let assigned_way_ids = BTreeSet::from([source_way.way_id, target_way.way_id]);

        assert!(shortest_connector_path_to_any(
            &sources,
            &targets,
            &graph,
            &ways_by_id,
            &HashSet::from(["I-69C".to_string(), "US-281".to_string()]),
            &assigned_way_ids,
            false,
            false,
            None,
        )
        .is_none());
    }

    #[test]
    fn short_fallback_allows_short_high_class_connector_with_other_ref() {
        let source_way = route_way(
            1,
            &["I-12"],
            &[1, 2],
            &[(30.0, -91.11), (30.0, -91.10)],
            "motorway",
            true,
        );
        let target_way = route_way(
            2,
            &["I-12"],
            &[5, 6],
            &[(30.0, -91.09), (30.0, -91.08)],
            "motorway",
            true,
        );
        let connector_way = route_way(
            3,
            &["I-10"],
            &[2, 3, 4, 5],
            &[
                (30.0, -91.10),
                (30.0, -91.097),
                (30.0, -91.094),
                (30.0, -91.09),
            ],
            "motorway",
            true,
        );
        let ways_by_id = HashMap::from([
            (source_way.way_id, source_way.clone()),
            (target_way.way_id, target_way.clone()),
            (connector_way.way_id, connector_way.clone()),
        ]);
        let graph = build_connector_graph(&[
            ways_by_id.get(&source_way.way_id).unwrap(),
            ways_by_id.get(&target_way.way_id).unwrap(),
            ways_by_id.get(&connector_way.way_id).unwrap(),
        ]);
        let sources = HashSet::from([2_i64]);
        let targets = HashMap::from([(5_i64, 0_i32)]);
        let allowed_refs = HashSet::from(["I-12".to_string()]);
        let assigned_way_ids = BTreeSet::from([source_way.way_id, target_way.way_id]);

        assert!(shortest_connector_path_to_any(
            &sources,
            &targets,
            &graph,
            &ways_by_id,
            &allowed_refs,
            &assigned_way_ids,
            false,
            false,
            None,
        )
        .is_none());

        let result = shortest_connector_path_to_any(
            &sources,
            &targets,
            &graph,
            &ways_by_id,
            &allowed_refs,
            &assigned_way_ids,
            true,
            true,
            Some(SHORT_FALLBACK_CONNECTOR_MAX_COST_M),
        )
        .expect("short fallback path should be available");
        assert_eq!(result.2, vec![3]);
    }

    #[test]
    fn short_fallback_rejects_long_high_class_connector() {
        let source_way = route_way(
            1,
            &["I-12"],
            &[1, 2],
            &[(30.0, -91.20), (30.0, -91.19)],
            "motorway",
            true,
        );
        let target_way = route_way(
            2,
            &["I-12"],
            &[5, 6],
            &[(30.0, -91.00), (30.0, -90.99)],
            "motorway",
            true,
        );
        let connector_way = route_way(
            3,
            &["I-10"],
            &[2, 3, 4, 5],
            &[
                (30.0, -91.19),
                (30.0, -91.13),
                (30.0, -91.06),
                (30.0, -91.00),
            ],
            "motorway",
            true,
        );
        let ways_by_id = HashMap::from([
            (source_way.way_id, source_way.clone()),
            (target_way.way_id, target_way.clone()),
            (connector_way.way_id, connector_way.clone()),
        ]);
        let graph = build_connector_graph(&[
            ways_by_id.get(&source_way.way_id).unwrap(),
            ways_by_id.get(&target_way.way_id).unwrap(),
            ways_by_id.get(&connector_way.way_id).unwrap(),
        ]);
        let sources = HashSet::from([2_i64]);
        let targets = HashMap::from([(5_i64, 0_i32)]);
        let allowed_refs = HashSet::from(["I-12".to_string()]);
        let assigned_way_ids = BTreeSet::from([source_way.way_id, target_way.way_id]);

        assert!(shortest_connector_path_to_any(
            &sources,
            &targets,
            &graph,
            &ways_by_id,
            &allowed_refs,
            &assigned_way_ids,
            true,
            true,
            Some(SHORT_FALLBACK_CONNECTOR_MAX_COST_M),
        )
        .is_none());
    }

    #[test]
    fn prune_micro_route_segments_drops_tiny_detached_component() {
        let kept = prune_micro_route_segments(vec![
            vec![[42.0, -79.88], [42.0, -79.0]],
            vec![[42.15, -79.386], [42.153, -79.375]],
        ]);

        assert_eq!(kept.len(), 1);
    }

    #[test]
    fn expand_nodes_includes_adjacent_same_ref_way() {
        let mut ways = HashMap::new();
        // Assigned way: nodes [1, 2, 3]
        ways.insert(
            100,
            route_way(100, &["I-95"], &[1, 2, 3], &[(40.0, -80.0), (40.1, -80.0), (40.2, -80.0)], "motorway", true),
        );
        // Adjacent way shares node 3, has same ref: nodes [3, 4, 5]
        ways.insert(
            200,
            route_way(200, &["I-95"], &[3, 4, 5], &[(40.2, -80.0), (40.2, -80.01), (40.2, -80.02)], "motorway_link", true),
        );
        // Unconnected way with same ref: nodes [10, 11]
        ways.insert(
            300,
            route_way(300, &["I-95"], &[10, 11], &[(41.0, -80.0), (41.1, -80.0)], "motorway", true),
        );
        // Adjacent way shares node 2 but DIFFERENT ref: nodes [2, 20, 21]
        ways.insert(
            400,
            route_way(400, &["I-76"], &[2, 20, 21], &[(40.1, -80.0), (40.1, -80.1), (40.1, -80.2)], "motorway", true),
        );

        let assigned: HashSet<i64> = [1, 2, 3].into_iter().collect();
        let expanded = super::expand_nodes_with_adjacent_ways(&assigned, "I-95", &ways);

        // Should include original + adjacent same-ref way
        assert!(expanded.contains(&4), "node 4 from adjacent I-95 way should be included");
        assert!(expanded.contains(&5), "node 5 from adjacent I-95 way should be included");
        // Should NOT include unconnected same-ref way
        assert!(!expanded.contains(&10), "node 10 from unconnected way should be excluded");
        // Should NOT include adjacent different-ref way
        assert!(!expanded.contains(&20), "node 20 from I-76 way should be excluded");
    }

    #[test]
    fn expand_nodes_includes_blank_ref_link_ways() {
        let mut ways = HashMap::new();
        // Mainline motorway: nodes [1, 2, 3]
        ways.insert(
            100,
            route_way(100, &["I-10"], &[1, 2, 3], &[(33.0, -112.0), (33.1, -112.0), (33.2, -112.0)], "motorway", true),
        );
        // Blank-ref motorway_link ramp sharing node 2: nodes [2, 50, 51]
        // This represents a ramp with exit node 50 tagged "153A"
        ways.insert(
            200,
            route_way(200, &[], &[2, 50, 51], &[(33.1, -112.0), (33.1, -112.01), (33.1, -112.02)], "motorway_link", true),
        );
        // motorway_link with a DIFFERENT ref (e.g. "US 17"), sharing node 3: nodes [3, 60, 61]
        ways.insert(
            300,
            route_way(300, &["US 17"], &[3, 60, 61], &[(33.2, -112.0), (33.2, -112.01), (33.2, -112.02)], "motorway_link", true),
        );
        // Blank-ref motorway_link NOT connected to assigned nodes: nodes [70, 71]
        ways.insert(
            400,
            route_way(400, &[], &[70, 71], &[(34.0, -112.0), (34.1, -112.0)], "motorway_link", true),
        );
        // Blank-ref trunk (not a _link), sharing node 1: nodes [1, 80, 81]
        ways.insert(
            500,
            route_way(500, &[], &[1, 80, 81], &[(33.0, -112.0), (33.0, -112.1), (33.0, -112.2)], "trunk", true),
        );

        let assigned: HashSet<i64> = [1, 2, 3].into_iter().collect();
        let expanded = super::expand_nodes_with_adjacent_ways(&assigned, "I-10", &ways);

        // Blank-ref motorway_link connected to corridor: included
        assert!(expanded.contains(&50), "exit node on blank-ref ramp should be included");
        assert!(expanded.contains(&51), "ramp end node on blank-ref ramp should be included");
        // Different-ref motorway_link: excluded
        assert!(!expanded.contains(&60), "node on US-17 ramp should be excluded");
        // Unconnected blank-ref link: excluded
        assert!(!expanded.contains(&70), "node on disconnected blank-ref link should be excluded");
        // Blank-ref trunk (not a _link): excluded
        assert!(!expanded.contains(&80), "node on blank-ref trunk (not link) should be excluded");
    }

    fn exit_row(exit_id: &str, graph_node: i64, ref_val: Option<&str>) -> ExitRow {
        ExitRow {
            exit_id: exit_id.to_string(),
            highway: "I-95".to_string(),
            graph_node,
            ref_val: ref_val.map(ToString::to_string),
            name: None,
            lat: 40.0,
            lon: -80.0,
        }
    }

    #[test]
    fn resolve_semicolon_splits_compound_ref() {
        let exits = vec![exit_row("node/100", 100, Some("23A;23B"))];
        let resolved = super::resolve_semicolon_refs(exits);
        assert_eq!(resolved.len(), 2);
        assert_eq!(resolved[0].ref_val.as_deref(), Some("23A"));
        assert_eq!(resolved[1].ref_val.as_deref(), Some("23B"));
    }

    #[test]
    fn resolve_semicolon_split_assigns_distinct_exit_ids() {
        let exits = vec![exit_row("node/100", 100, Some("23A;23B"))];
        let resolved = super::resolve_semicolon_refs(exits);
        assert_ne!(resolved[0].exit_id, resolved[1].exit_id);
        assert_eq!(resolved[0].exit_id, "node/100:23A");
        assert_eq!(resolved[1].exit_id, "node/100:23B");
    }

    #[test]
    fn resolve_semicolon_skips_part_with_dedicated_node() {
        let exits = vec![
            exit_row("node/100", 100, Some("23A;23B")),
            exit_row("node/200", 200, Some("23A")), // dedicated ramp node for 23A
        ];
        let resolved = super::resolve_semicolon_refs(exits);
        let refs: Vec<_> = resolved.iter().filter_map(|e| e.ref_val.as_deref()).collect();
        // 23A from gore-point should be dropped; only 23B from split + 23A from dedicated node
        assert_eq!(refs, vec!["23B", "23A"]);
        // The 23A entry should be from the dedicated node, not the gore-point
        let exit_23a = resolved.iter().find(|e| e.ref_val.as_deref() == Some("23A")).unwrap();
        assert_eq!(exit_23a.graph_node, 200);
    }

    #[test]
    fn resolve_semicolon_passes_through_non_semicolon_refs() {
        let exits = vec![
            exit_row("node/1", 1, Some("42")),
            exit_row("node/2", 2, None),
            exit_row("node/3", 3, Some("43")),
        ];
        let resolved = super::resolve_semicolon_refs(exits);
        assert_eq!(resolved.len(), 3);
        assert_eq!(resolved[0].ref_val.as_deref(), Some("42"));
        assert_eq!(resolved[1].ref_val, None);
        assert_eq!(resolved[2].ref_val.as_deref(), Some("43"));
    }

    #[test]
    fn resolve_semicolon_handles_whitespace_and_empty_parts() {
        let exits = vec![exit_row("node/100", 100, Some(" 5A ; 5B ; "))];
        let resolved = super::resolve_semicolon_refs(exits);
        assert_eq!(resolved.len(), 2);
        assert_eq!(resolved[0].ref_val.as_deref(), Some("5A"));
        assert_eq!(resolved[1].ref_val.as_deref(), Some("5B"));
    }
}
