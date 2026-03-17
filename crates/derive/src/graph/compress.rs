use std::cmp::Ordering;
use std::collections::{hash_map::Entry, BTreeSet, BinaryHeap, HashMap, HashSet, VecDeque};

use openinterstate_core::geo::haversine_distance;
use openinterstate_core::highway_ref::is_interstate_highway_ref;

use crate::canonical_types::{ParsedExit, ParsedHighway};
use crate::interstate_relations::InterstateRouteSignature;

use super::directions::compute_component_directions;

/// A compressed highway edge ready for insertion into PostGIS.
pub(super) struct CompressedEdge {
    pub(super) id: String,
    pub(super) highway: String,
    pub(super) component: i32,
    pub(super) start_node: i64,
    pub(super) end_node: i64,
    pub(super) length_m: i32,
    pub(super) min_lat: f64,
    pub(super) max_lat: f64,
    pub(super) min_lon: f64,
    pub(super) max_lon: f64,
    pub(super) polyline_json: String,
    pub(super) source_way_ids_json: String,
    /// WKT LineString for PostGIS geom column.
    pub(super) geom_wkt: String,
    /// Cardinal direction of this edge's component ("N","S","E","W" or None).
    pub(super) direction: Option<String>,
}

/// A corridor entry: one exit belonging to one highway's graph.
pub(super) struct ExitCorridorEntry {
    pub(super) exit_id: String,
    pub(super) highway: String,
    pub(super) component: i32,
    pub(super) node_id: i64,
    pub(super) direction: Option<String>,
}

struct HighwayGraph {
    node_coords: HashMap<i64, (f64, f64)>,
    neighbors_directed: HashMap<i64, BTreeSet<i64>>,
    neighbors_undirected: HashMap<i64, BTreeSet<i64>>,
    arc_way_ids: HashMap<(i64, i64), BTreeSet<i64>>,
}

type RouteSignaturesByWay = HashMap<i64, Vec<InterstateRouteSignature>>;

struct ConnectorGraph {
    adjacency: HashMap<i64, Vec<ConnectorArc>>,
}

#[derive(Clone, Copy)]
struct ConnectorArc {
    next: i64,
    weight_m: u64,
    way_id: i64,
}

#[derive(Clone, Copy, Eq, PartialEq)]
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

/// Pure function: compress highway ways into directed edges.
pub(super) fn compress_highway_graph(
    highways: &[ParsedHighway],
    exits: &[ParsedExit],
    route_signatures_by_highway_and_way: &HashMap<String, RouteSignaturesByWay>,
) -> (Vec<CompressedEdge>, Vec<ExitCorridorEntry>) {
    let (all_exit_node_ids, exit_id_by_node) = build_exit_node_index(exits);
    let ways_by_highway = group_ways_by_highway(highways);

    let mut all_edges = Vec::new();
    let mut corridor_entries = Vec::new();

    let mut sorted_highways: Vec<String> = ways_by_highway.keys().cloned().collect();
    sorted_highways.sort();

    for highway in sorted_highways {
        let Some(highway_ways) = ways_by_highway.get(&highway) else {
            continue;
        };
        let Some(graph) = build_highway_graph(highway_ways) else {
            continue;
        };
        let route_signatures_by_way = route_signatures_by_highway_and_way.get(&highway);

        let component_by_node = compute_components(&graph.neighbors_undirected);
        let stop_nodes = identify_stop_nodes(
            &graph,
            &all_exit_node_ids,
            route_signatures_by_way,
        );
        let mut edges = walk_compressed_edges(
            &highway,
            &graph,
            &component_by_node,
            &stop_nodes,
            route_signatures_by_way,
        );

        let component_directions = compute_component_directions(&edges, &highway);
        apply_component_directions(&mut edges, &component_directions);
        corridor_entries.extend(build_corridor_entries(
            &highway,
            &component_by_node,
            &component_directions,
            &exit_id_by_node,
        ));

        all_edges.extend(edges);
    }

    (all_edges, corridor_entries)
}

fn build_exit_node_index(exits: &[ParsedExit]) -> (HashSet<i64>, HashMap<i64, Vec<String>>) {
    let mut all_exit_node_ids: HashSet<i64> = HashSet::new();
    let mut exit_id_by_node: HashMap<i64, Vec<String>> = HashMap::new();

    for exit in exits {
        let node_id = exit
            .id
            .split('/')
            .nth(1)
            .and_then(|value| value.parse::<i64>().ok())
            .unwrap_or(exit.osm_id);
        if node_id == 0 {
            continue;
        }

        all_exit_node_ids.insert(node_id);
        exit_id_by_node
            .entry(node_id)
            .or_default()
            .push(exit.id.clone());
    }

    (all_exit_node_ids, exit_id_by_node)
}

fn group_ways_by_highway(highways: &[ParsedHighway]) -> HashMap<String, Vec<&ParsedHighway>> {
    let mut assigned_way_ids: HashMap<String, BTreeSet<i64>> = HashMap::new();
    let mut ways_by_id: HashMap<i64, &ParsedHighway> = HashMap::new();
    let mut refless_motorways: Vec<&ParsedHighway> = Vec::new();
    let mut blank_ref_connectors: Vec<&ParsedHighway> = Vec::new();

    for way in highways {
        ways_by_id.insert(way.way_id, way);

        let interstate_refs: Vec<String> = way
            .refs
            .iter()
            .filter(|reference| is_interstate_highway_ref(reference))
            .cloned()
            .collect();
        if !interstate_refs.is_empty() {
            for highway in interstate_refs {
                assigned_way_ids
                    .entry(highway)
                    .or_default()
                    .insert(way.way_id);
            }
            continue;
        }

        if way.refs.is_empty() {
            blank_ref_connectors.push(way);
            if way.highway_type == "motorway" {
                refless_motorways.push(way);
            }
        }
    }

    adopt_refless_motorways_by_shared_nodes(&mut assigned_way_ids, &refless_motorways, &ways_by_id);

    let connector_graph = build_blank_ref_connector_graph(&blank_ref_connectors);
    adopt_blank_ref_connector_paths(&mut assigned_way_ids, &ways_by_id, &connector_graph);

    let mut ways_by_highway: HashMap<String, Vec<&ParsedHighway>> = HashMap::new();
    for (highway, way_ids) in assigned_way_ids {
        let mut ways: Vec<&ParsedHighway> = way_ids
            .into_iter()
            .filter_map(|way_id| ways_by_id.get(&way_id).copied())
            .collect();
        ways.sort_by_key(|way| way.way_id);
        ways_by_highway.insert(highway, ways);
    }

    ways_by_highway
}

fn adopt_refless_motorways_by_shared_nodes(
    assigned_way_ids: &mut HashMap<String, BTreeSet<i64>>,
    refless_motorways: &[&ParsedHighway],
    ways_by_id: &HashMap<i64, &ParsedHighway>,
) {
    if refless_motorways.is_empty() {
        return;
    }

    let mut node_highways: HashMap<i64, HashSet<String>> = HashMap::new();
    for (highway, way_ids) in assigned_way_ids.iter() {
        for way_id in way_ids {
            let Some(way) = ways_by_id.get(way_id).copied() else {
                continue;
            };
            for &node_id in &way.nodes {
                node_highways
                    .entry(node_id)
                    .or_default()
                    .insert(highway.clone());
            }
        }
    }

    let mut remaining = refless_motorways.to_vec();
    let mut total_assigned = 0usize;
    loop {
        let mut unmatched: Vec<&ParsedHighway> = Vec::new();
        let mut assigned_this_round = 0usize;
        for way in remaining {
            let mut matched: HashSet<String> = HashSet::new();
            for &node_id in &way.nodes {
                if let Some(highways) = node_highways.get(&node_id) {
                    matched.extend(highways.iter().cloned());
                }
            }
            if matched.is_empty() {
                unmatched.push(way);
                continue;
            }

            for highway in &matched {
                assigned_way_ids
                    .entry(highway.clone())
                    .or_default()
                    .insert(way.way_id);
            }
            for &node_id in &way.nodes {
                node_highways
                    .entry(node_id)
                    .or_default()
                    .extend(matched.iter().cloned());
            }
            assigned_this_round += 1;
        }
        total_assigned += assigned_this_round;
        if assigned_this_round == 0 {
            break;
        }
        remaining = unmatched;
    }

    if total_assigned > 0 {
        tracing::info!(
            "Adopted {} refless motorway ways via shared nodes",
            total_assigned
        );
    }
}

fn build_blank_ref_connector_graph(ways: &[&ParsedHighway]) -> ConnectorGraph {
    let mut adjacency: HashMap<i64, Vec<ConnectorArc>> = HashMap::new();

    for way in ways {
        if way.nodes.len() < 2 || way.geometry.len() < 2 || way.nodes.len() != way.geometry.len() {
            continue;
        }

        for idx in 0..way.nodes.len() - 1 {
            let start = way.nodes[idx];
            let end = way.nodes[idx + 1];
            let start_coord = way.geometry[idx];
            let end_coord = way.geometry[idx + 1];
            let weight_m =
                haversine_distance(start_coord.0, start_coord.1, end_coord.0, end_coord.1)
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

fn adopt_blank_ref_connector_paths(
    assigned_way_ids: &mut HashMap<String, BTreeSet<i64>>,
    ways_by_id: &HashMap<i64, &ParsedHighway>,
    connector_graph: &ConnectorGraph,
) {
    let mut highways: Vec<String> = assigned_way_ids.keys().cloned().collect();
    highways.sort();

    for highway in highways {
        let mut total_adopted = 0usize;

        loop {
            let Some(assigned_ids) = assigned_way_ids.get(&highway) else {
                break;
            };
            let assigned_ways: Vec<&ParsedHighway> = assigned_ids
                .iter()
                .filter_map(|way_id| ways_by_id.get(way_id).copied())
                .collect();
            let components = highway_connected_components(&assigned_ways);
            if components.len() <= 1 {
                break;
            }

            let mut best_path: Option<(u64, i32, Vec<i64>)> = None;
            for (source_comp, source_nodes, _) in &components {
                let mut target_components_by_node: HashMap<i64, i32> = HashMap::new();
                for (target_comp, target_nodes, _) in &components {
                    if target_comp == source_comp {
                        continue;
                    }
                    for &node_id in target_nodes {
                        target_components_by_node.insert(node_id, *target_comp);
                    }
                }

                let Some(candidate) = shortest_connector_path_to_any(
                    source_nodes,
                    &target_components_by_node,
                    connector_graph,
                ) else {
                    continue;
                };
                if best_path
                    .as_ref()
                    .is_none_or(|(best_cost, _, _)| candidate.0 < *best_cost)
                {
                    best_path = Some(candidate);
                }
            }

            let Some((_cost_m, target_comp, path_way_ids)) = best_path else {
                break;
            };

            let entry = assigned_way_ids.entry(highway.clone()).or_default();
            let mut adopted_this_round = 0usize;
            for way_id in path_way_ids {
                adopted_this_round += usize::from(entry.insert(way_id));
            }

            if adopted_this_round == 0 {
                tracing::debug!(
                    "{}: connector path to component {} yielded no new ways",
                    highway,
                    target_comp
                );
                break;
            }
            total_adopted += adopted_this_round;
        }

        if total_adopted > 0 {
            tracing::info!(
                "Adopted {} blank-ref connector ways via graph search for {}",
                total_adopted,
                highway
            );
        }
    }
}

fn highway_connected_components(ways: &[&ParsedHighway]) -> Vec<(i32, HashSet<i64>, usize)> {
    let Some(graph) = build_highway_graph(ways) else {
        return Vec::new();
    };
    let component_by_node = compute_components(&graph.neighbors_undirected);
    let mut nodes_by_component: HashMap<i32, HashSet<i64>> = HashMap::new();
    let mut way_count_by_component: HashMap<i32, usize> = HashMap::new();

    for way in ways {
        let Some(first_node) = way.nodes.first() else {
            continue;
        };
        let Some(&component) = component_by_node.get(first_node) else {
            continue;
        };
        *way_count_by_component.entry(component).or_default() += 1;
        nodes_by_component
            .entry(component)
            .or_default()
            .extend(way.nodes.iter().copied());
    }

    let mut components: Vec<(i32, HashSet<i64>, usize)> = nodes_by_component
        .into_iter()
        .map(|(component, nodes)| {
            (
                component,
                nodes,
                way_count_by_component.get(&component).copied().unwrap_or(0),
            )
        })
        .collect();
    components.sort_by_key(|(component, _, _)| *component);
    components
}

fn shortest_connector_path_to_any(
    sources: &HashSet<i64>,
    targets_by_node: &HashMap<i64, i32>,
    connector_graph: &ConnectorGraph,
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
            let next_cost = cost_m.saturating_add(arc.weight_m);
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

fn build_highway_graph(highway_ways: &[&ParsedHighway]) -> Option<HighwayGraph> {
    let mut node_coords: HashMap<i64, (f64, f64)> = HashMap::new();
    let mut neighbors_directed: HashMap<i64, BTreeSet<i64>> = HashMap::new();
    let mut neighbors_undirected: HashMap<i64, BTreeSet<i64>> = HashMap::new();
    let mut arc_way_ids: HashMap<(i64, i64), BTreeSet<i64>> = HashMap::new();

    for way in highway_ways {
        if way.nodes.len() < 2 || way.geometry.len() < 2 || way.nodes.len() != way.geometry.len() {
            continue;
        }

        for (idx, &node_id) in way.nodes.iter().enumerate() {
            node_coords.entry(node_id).or_insert(way.geometry[idx]);
        }

        for idx in 0..way.nodes.len() - 1 {
            let start = way.nodes[idx];
            let end = way.nodes[idx + 1];

            neighbors_directed.entry(start).or_default().insert(end);
            arc_way_ids
                .entry((start, end))
                .or_default()
                .insert(way.way_id);
            if !way.is_oneway {
                neighbors_directed.entry(end).or_default().insert(start);
                arc_way_ids
                    .entry((end, start))
                    .or_default()
                    .insert(way.way_id);
            }

            neighbors_undirected.entry(start).or_default().insert(end);
            neighbors_undirected.entry(end).or_default().insert(start);
        }
    }

    if neighbors_undirected.is_empty() {
        None
    } else {
        Some(HighwayGraph {
            node_coords,
            neighbors_directed,
            neighbors_undirected,
            arc_way_ids,
        })
    }
}

fn compute_components(neighbors_undirected: &HashMap<i64, BTreeSet<i64>>) -> HashMap<i64, i32> {
    let mut component_by_node: HashMap<i64, i32> = HashMap::new();
    let mut component_idx: i32 = 0;

    let mut sorted_nodes: Vec<i64> = neighbors_undirected.keys().copied().collect();
    sorted_nodes.sort_unstable();

    for node_id in sorted_nodes {
        if component_by_node.contains_key(&node_id) {
            continue;
        }

        let mut queue = VecDeque::new();
        queue.push_back(node_id);
        component_by_node.insert(node_id, component_idx);

        while let Some(cur) = queue.pop_front() {
            if let Some(neighbors) = neighbors_undirected.get(&cur) {
                for &next in neighbors {
                    if let Entry::Vacant(entry) = component_by_node.entry(next) {
                        entry.insert(component_idx);
                        queue.push_back(next);
                    }
                }
            }
        }

        component_idx += 1;
    }

    component_by_node
}

fn identify_stop_nodes(
    graph: &HighwayGraph,
    all_exit_node_ids: &HashSet<i64>,
    route_signatures_by_way: Option<&RouteSignaturesByWay>,
) -> HashSet<i64> {
    let mut stop_nodes: HashSet<i64> = HashSet::new();
    let mut in_degree: HashMap<i64, usize> = HashMap::new();
    let mut out_degree: HashMap<i64, usize> = HashMap::new();
    let mut incoming_neighbors: HashMap<i64, BTreeSet<i64>> = HashMap::new();

    for (&node, targets) in &graph.neighbors_directed {
        *out_degree.entry(node).or_default() += targets.len();
        for &target in targets {
            *in_degree.entry(target).or_default() += 1;
            incoming_neighbors.entry(target).or_default().insert(node);
        }
    }

    for &node_id in graph.neighbors_undirected.keys() {
        let incoming = in_degree.get(&node_id).copied().unwrap_or(0);
        let outgoing = out_degree.get(&node_id).copied().unwrap_or(0);
        if !(incoming == 1 && outgoing == 1) {
            stop_nodes.insert(node_id);
            continue;
        }

        let Some(route_signatures_by_way) = route_signatures_by_way else {
            continue;
        };

        let incoming_signature = incoming_neighbors
            .get(&node_id)
            .and_then(|neighbors| neighbors.iter().next().copied())
            .map(|prev_node| {
                arc_route_signature(graph, (prev_node, node_id), route_signatures_by_way)
            });
        let outgoing_signature = graph
            .neighbors_directed
            .get(&node_id)
            .and_then(|neighbors| neighbors.iter().next().copied())
            .map(|next_node| {
                arc_route_signature(graph, (node_id, next_node), route_signatures_by_way)
            });

        if incoming_signature != outgoing_signature {
            stop_nodes.insert(node_id);
        }
    }

    for &node_id in all_exit_node_ids {
        if graph.neighbors_undirected.contains_key(&node_id) {
            stop_nodes.insert(node_id);
        }
    }

    stop_nodes
}

fn walk_compressed_edges(
    highway: &str,
    graph: &HighwayGraph,
    component_by_node: &HashMap<i64, i32>,
    stop_nodes: &HashSet<i64>,
    route_signatures_by_way: Option<&RouteSignaturesByWay>,
) -> Vec<CompressedEdge> {
    let mut edges = Vec::new();
    let mut visited_directed: HashSet<(i64, i64)> = HashSet::new();

    let mut sorted_stop_nodes: Vec<i64> = stop_nodes.iter().copied().collect();
    sorted_stop_nodes.sort_unstable();

    for start_node in sorted_stop_nodes {
        let Some(next_nodes) = graph.neighbors_directed.get(&start_node) else {
            continue;
        };

        for &first_hop in next_nodes {
            let first_edge = (start_node, first_hop);
            if visited_directed.contains(&first_edge) {
                continue;
            }

            let Some(&start_coord) = graph.node_coords.get(&start_node) else {
                continue;
            };
            let Some(&first_coord) = graph.node_coords.get(&first_hop) else {
                continue;
            };

            let mut polyline = vec![start_coord, first_coord];
            let mut length_m =
                haversine_distance(start_coord.0, start_coord.1, first_coord.0, first_coord.1);
            let mut source_way_ids: BTreeSet<i64> = graph
                .arc_way_ids
                .get(&first_edge)
                .cloned()
                .unwrap_or_default();
            let route_signature = route_signatures_by_way.map(|memberships| {
                arc_route_signature(graph, first_edge, memberships)
            });
            visited_directed.insert(first_edge);

            let mut prev = start_node;
            let mut cur = first_hop;

            while !stop_nodes.contains(&cur) {
                let Some(cur_neighbors) = graph.neighbors_directed.get(&cur) else {
                    break;
                };

                let mut next_iter = cur_neighbors.iter().copied().filter(|&n| n != prev);
                let Some(next) = next_iter.next() else {
                    break;
                };
                if next_iter.next().is_some() {
                    break;
                }

                let Some(&cur_coord) = graph.node_coords.get(&cur) else {
                    break;
                };
                let Some(&next_coord) = graph.node_coords.get(&next) else {
                    break;
                };

                let next_edge = (cur, next);
                if visited_directed.contains(&next_edge) {
                    break;
                }

                if let (Some(route_signatures_by_way), Some(route_signature)) =
                    (route_signatures_by_way, route_signature.as_ref())
                {
                    let next_signature =
                        arc_route_signature(graph, next_edge, route_signatures_by_way);
                    if next_signature != *route_signature {
                        break;
                    }
                }

                polyline.push(next_coord);
                length_m +=
                    haversine_distance(cur_coord.0, cur_coord.1, next_coord.0, next_coord.1);
                if let Some(way_ids) = graph.arc_way_ids.get(&next_edge) {
                    source_way_ids.extend(way_ids.iter().copied());
                }
                visited_directed.insert(next_edge);

                prev = cur;
                cur = next;
            }

            let end_node = cur;
            if end_node == start_node {
                continue;
            }

            let Some(&component) = component_by_node.get(&start_node) else {
                continue;
            };
            if component_by_node.get(&end_node) != Some(&component) {
                continue;
            }

            let (min_lat, max_lat, min_lon, max_lon) = bounds_for_polyline(&polyline);
            let id = format!("edge/{highway}/{start_node}/{end_node}");
            let polyline_json = serde_json::to_string(
                &polyline
                    .iter()
                    .map(|&(lat, lon)| [lat, lon])
                    .collect::<Vec<_>>(),
            )
            .unwrap_or_else(|_| "[]".to_string());
            let source_way_ids_json =
                serde_json::to_string(&source_way_ids.into_iter().collect::<Vec<_>>())
                    .unwrap_or_else(|_| "[]".to_string());
            let geom_wkt = polyline_to_linestring_wkt(&polyline);

            edges.push(CompressedEdge {
                id,
                highway: highway.to_string(),
                component,
                start_node,
                end_node,
                length_m: length_m.round().max(1.0) as i32,
                min_lat,
                max_lat,
                min_lon,
                max_lon,
                polyline_json,
                source_way_ids_json,
                geom_wkt,
                direction: None,
            });
        }
    }

    edges
}

fn arc_route_signature(
    graph: &HighwayGraph,
    arc: (i64, i64),
    route_signatures_by_way: &RouteSignaturesByWay,
) -> Vec<InterstateRouteSignature> {
    let mut signature: BTreeSet<InterstateRouteSignature> = BTreeSet::new();

    for way_id in graph.arc_way_ids.get(&arc).into_iter().flatten() {
        if let Some(route_signatures) = route_signatures_by_way.get(way_id) {
            signature.extend(route_signatures.iter().cloned());
        }
    }

    signature.into_iter().collect()
}

fn bounds_for_polyline(polyline: &[(f64, f64)]) -> (f64, f64, f64, f64) {
    let mut min_lat = f64::INFINITY;
    let mut max_lat = f64::NEG_INFINITY;
    let mut min_lon = f64::INFINITY;
    let mut max_lon = f64::NEG_INFINITY;

    for &(lat, lon) in polyline {
        min_lat = min_lat.min(lat);
        max_lat = max_lat.max(lat);
        min_lon = min_lon.min(lon);
        max_lon = max_lon.max(lon);
    }

    (min_lat, max_lat, min_lon, max_lon)
}

fn polyline_to_linestring_wkt(polyline: &[(f64, f64)]) -> String {
    let wkt_coords: Vec<String> = polyline
        .iter()
        .map(|&(lat, lon)| format!("{lon} {lat}"))
        .collect();
    format!("LINESTRING({})", wkt_coords.join(", "))
}

fn apply_component_directions(
    edges: &mut [CompressedEdge],
    component_directions: &HashMap<(String, i32), String>,
) {
    for edge in edges {
        let key = (edge.highway.clone(), edge.component);
        if let Some(direction) = component_directions.get(&key) {
            edge.direction = Some(direction.clone());
        }
    }
}

fn build_corridor_entries(
    highway: &str,
    component_by_node: &HashMap<i64, i32>,
    component_directions: &HashMap<(String, i32), String>,
    exit_id_by_node: &HashMap<i64, Vec<String>>,
) -> Vec<ExitCorridorEntry> {
    let mut entries = Vec::new();

    let mut node_ids: Vec<i64> = exit_id_by_node.keys().copied().collect();
    node_ids.sort_unstable();

    for node_id in node_ids {
        let Some(&component) = component_by_node.get(&node_id) else {
            continue;
        };

        let direction = component_directions
            .get(&(highway.to_string(), component))
            .cloned();

        let Some(exit_ids) = exit_id_by_node.get(&node_id) else {
            continue;
        };
        let mut sorted_exit_ids = exit_ids.clone();
        sorted_exit_ids.sort();

        for exit_id in sorted_exit_ids {
            entries.push(ExitCorridorEntry {
                exit_id,
                highway: highway.to_string(),
                component,
                node_id,
                direction: direction.clone(),
            });
        }
    }

    entries
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::interstate_relations::InterstateRouteSignature;

    fn sample_exit(id: &str, osm_id: i64) -> ParsedExit {
        ParsedExit {
            id: id.to_string(),
            osm_id,
        }
    }

    fn sample_highway(
        id: &str,
        refs: &[&str],
        nodes: &[i64],
        geometry: &[(f64, f64)],
    ) -> ParsedHighway {
        sample_highway_kind(id, "motorway", refs, nodes, geometry, true)
    }

    fn sample_highway_kind(
        id: &str,
        highway_type: &str,
        refs: &[&str],
        nodes: &[i64],
        geometry: &[(f64, f64)],
        is_oneway: bool,
    ) -> ParsedHighway {
        ParsedHighway {
            way_id: id
                .split('/')
                .nth(1)
                .and_then(|value| value.parse::<i64>().ok())
                .unwrap_or(0),
            refs: refs.iter().map(|value| (*value).to_string()).collect(),
            nodes: nodes.to_vec(),
            geometry: geometry.to_vec(),
            highway_type: highway_type.to_string(),
            is_oneway,
        }
    }

    fn sample_route_signature(
        root_relation_id: i64,
        direction: Option<&str>,
    ) -> InterstateRouteSignature {
        InterstateRouteSignature {
            root_relation_id,
            direction: direction.map(|value| value.to_string()),
        }
    }

    #[test]
    fn compresses_highway_and_assigns_corridor_entry() {
        let highways = vec![sample_highway(
            "way/1",
            &["I-95"],
            &[1, 2, 3],
            &[(39.0, -76.0), (39.001, -76.0), (39.002, -76.0)],
        )];
        let exits = vec![sample_exit("node/2", 2)];

        let (edges, corridor_entries) =
            compress_highway_graph(&highways, &exits, &HashMap::new());

        assert_eq!(
            edges.len(),
            2,
            "exit node should split a linear way into two edges"
        );
        assert!(edges.iter().all(|edge| edge.highway == "I-95"));
        assert!(corridor_entries
            .iter()
            .any(|entry| entry.exit_id == "node/2"
                && entry.highway == "I-95"
                && entry.node_id == 2));
    }

    #[test]
    fn component_assignment_is_deterministic_by_node_order() {
        let highways = vec![
            sample_highway(
                "way/10",
                &["I-95"],
                &[10, 11],
                &[(39.01, -76.0), (39.02, -76.0)],
            ),
            sample_highway(
                "way/1",
                &["I-95"],
                &[1, 2],
                &[(39.0, -76.0), (39.001, -76.0)],
            ),
        ];

        let (edges, _) = compress_highway_graph(&highways, &[], &HashMap::new());
        let edge_low = edges
            .iter()
            .find(|edge| edge.start_node == 1 && edge.end_node == 2)
            .expect("missing low-node edge");
        let edge_high = edges
            .iter()
            .find(|edge| edge.start_node == 10 && edge.end_node == 11)
            .expect("missing high-node edge");

        assert_eq!(edge_low.component, 0);
        assert_eq!(edge_high.component, 1);
    }

    #[test]
    fn ignores_non_interstate_refs() {
        let highways = vec![
            sample_highway(
                "way/1",
                &["US-101"],
                &[1, 2],
                &[(37.0, -122.0), (37.001, -122.0)],
            ),
            sample_highway(
                "way/2",
                &["I-280"],
                &[10, 11],
                &[(37.4, -122.1), (37.401, -122.1)],
            ),
        ];

        let (edges, corridor_entries) =
            compress_highway_graph(&highways, &[], &HashMap::new());

        assert_eq!(edges.len(), 1);
        assert_eq!(edges[0].highway, "I-280");
        assert!(corridor_entries.is_empty());
    }

    #[test]
    fn adopts_blank_ref_motorway_link_connector_paths() {
        let highways = vec![
            sample_highway(
                "way/1",
                &["I-95"],
                &[1, 2],
                &[(39.0, -76.0), (39.01, -76.0)],
            ),
            sample_highway_kind(
                "way/2",
                "motorway_link",
                &[],
                &[2, 3],
                &[(39.01, -76.0), (39.02, -75.99)],
                true,
            ),
            sample_highway(
                "way/3",
                &["I-95"],
                &[3, 4],
                &[(39.02, -75.99), (39.03, -75.98)],
            ),
        ];

        let (edges, _) = compress_highway_graph(&highways, &[], &HashMap::new());

        let i95_edges: Vec<&CompressedEdge> =
            edges.iter().filter(|edge| edge.highway == "I-95").collect();
        let components: HashSet<i32> = i95_edges.iter().map(|edge| edge.component).collect();
        assert_eq!(
            components.len(),
            1,
            "blank-ref connector should unify the highway graph"
        );
        assert!(i95_edges
            .iter()
            .any(|edge| edge.start_node == 1 && edge.end_node == 4));
    }

    #[test]
    fn splits_edges_when_route_membership_signature_changes_mid_chain() {
        let highways = vec![
            sample_highway(
                "way/1",
                &["I-19"],
                &[1, 2],
                &[(31.0, -110.0), (31.001, -110.0)],
            ),
            sample_highway(
                "way/2",
                &["I-19"],
                &[2, 3],
                &[(31.001, -110.0), (31.002, -110.0)],
            ),
            sample_highway(
                "way/3",
                &["I-19"],
                &[3, 4],
                &[(31.002, -110.0), (31.003, -110.0)],
            ),
        ];
        let route_signatures_by_highway_and_way = HashMap::from([(
            "I-19".to_string(),
            HashMap::from([
                (
                    1,
                    vec![sample_route_signature(2369468, Some("north"))],
                ),
                (
                    2,
                    vec![sample_route_signature(2369468, Some("north"))],
                ),
                (
                    3,
                    vec![sample_route_signature(2369468, Some("south"))],
                ),
            ]),
        )]);

        let (edges, _) = compress_highway_graph(
            &highways,
            &[],
            &route_signatures_by_highway_and_way,
        );

        assert_eq!(edges.len(), 2);
        assert!(edges.iter().any(|edge| edge.start_node == 1 && edge.end_node == 3));
        assert!(edges.iter().any(|edge| edge.start_node == 3 && edge.end_node == 4));
    }
}
