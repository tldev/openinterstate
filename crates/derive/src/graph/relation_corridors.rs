use std::cmp::Ordering;
use std::collections::{BTreeSet, BinaryHeap, HashMap, HashSet};
use std::path::Path;

use openinterstate_core::geo::haversine_distance;
use openinterstate_core::highway_ref::normalize_highway_ref;
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
const MIN_ROUTE_LENGTH_M: f64 = 50_000.0;
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
            next_corridor_id,
        )?
        else {
            continue;
        };
        next_corridor_id += 1;
        drafts.push(draft);
    }

    write_corridors(pool, &drafts).await
}

fn filter_route_groups(groups: Vec<InterstateRouteGroup>) -> Vec<InterstateRouteGroup> {
    let directional_roots: HashSet<(String, i64)> = groups
        .iter()
        .filter(|group| group.direction.is_some())
        .map(|group| (group.highway.clone(), group.root_relation_id))
        .collect();

    groups
        .into_iter()
        .filter(|group| {
            group.direction.is_some()
                || !directional_roots.contains(&(group.highway.clone(), group.root_relation_id))
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

fn build_corridor_draft(
    group: &InterstateRouteGroup,
    ways_by_id: &HashMap<i64, RouteWay>,
    connector_graph: &ConnectorGraph,
    edge_rows: &[HighwayEdgeRow],
    exit_rows: &[ExitRow],
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

    adopt_relation_connector_paths(
        &ordered_members,
        &mut assigned_way_ids,
        ways_by_id,
        connector_graph,
    );
    adopt_connector_paths(
        &mut assigned_way_ids,
        ways_by_id,
        connector_graph,
        &group.highway,
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
    let exits = order_exits_along_route(
        exit_rows
            .iter()
            .filter(|exit| assigned_nodes.contains(&exit.graph_node))
            .cloned()
            .collect(),
        &route_segments,
    );
    let edge_ids = matched_edge_ids(edge_rows, &assigned_way_ids, Some(&canonical_direction));

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

fn matched_edge_ids(
    edge_rows: &[HighwayEdgeRow],
    assigned_way_ids: &BTreeSet<i64>,
    canonical_direction: Option<&str>,
) -> Vec<String> {
    let wanted_direction = canonical_direction.and_then(normalize_direction);
    let mut edge_ids = Vec::new();
    for edge in edge_rows {
        if let Some(direction) = &wanted_direction {
            if edge.direction.as_deref() != Some(direction.as_str()) {
                continue;
            }
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

async fn write_corridors(
    pool: &PgPool,
    drafts: &[CorridorDraft],
) -> Result<BuildCorridorsStats, anyhow::Error> {
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
    let mut allowed_refs = HashSet::from([route_highway.to_string()]);
    for way_id in assigned_way_ids {
        let Some(way) = ways_by_id.get(way_id) else {
            continue;
        };
        allowed_refs.extend(way.refs.iter().cloned());
    }
    allowed_refs
}

fn connector_way_allowed_for_refs(
    way: &RouteWay,
    allowed_refs: &HashSet<String>,
    allow_short_high_class_fallback: bool,
) -> bool {
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
        connector_way_allowed_for_refs, prune_micro_route_segments, shortest_connector_path_to_any,
        RouteWay, SHORT_FALLBACK_CONNECTOR_MAX_COST_M,
    };

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

        let result = shortest_connector_path_to_any(
            &sources,
            &targets,
            &graph,
            &ways_by_id,
            &HashSet::from(["I-96".to_string()]),
            false,
            None,
        );

        assert_eq!(result.map(|(_, _, way_ids)| way_ids), Some(vec![12]));
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

        assert!(shortest_connector_path_to_any(
            &sources,
            &targets,
            &graph,
            &ways_by_id,
            &allowed_refs,
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

        assert!(shortest_connector_path_to_any(
            &sources,
            &targets,
            &graph,
            &ways_by_id,
            &allowed_refs,
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
}
