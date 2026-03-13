use std::collections::{hash_map::Entry, BTreeSet, HashMap, HashSet, VecDeque};

use openinterstate_core::geo::haversine_distance;
use openinterstate_core::highway_ref::is_interstate_highway_ref;

use crate::canonical_types::{ParsedExit, ParsedHighway};

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
}

/// Pure function: compress highway ways into directed edges.
pub(super) fn compress_highway_graph(
    highways: &[ParsedHighway],
    exits: &[ParsedExit],
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

        let component_by_node = compute_components(&graph.neighbors_undirected);
        let stop_nodes = identify_stop_nodes(
            &graph.neighbors_undirected,
            &graph.neighbors_directed,
            &all_exit_node_ids,
        );
        let mut edges = walk_compressed_edges(&highway, &graph, &component_by_node, &stop_nodes);

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
    let mut ways_by_highway: HashMap<String, Vec<&ParsedHighway>> = HashMap::new();
    let mut refless_motorways: Vec<&ParsedHighway> = Vec::new();

    for way in highways {
        // Skip motorway_link ways that don't carry a ref — these are exit ramps.
        // Links WITH a ref (e.g. "I 40") are mainline connectors at interchanges
        // where interstates merge/split.
        if way.highway_type == "motorway_link" && way.refs.is_empty() {
            continue;
        }

        if way.refs.is_empty() && way.highway_type == "motorway" {
            // Refless motorway ways (e.g. "Sam Cooper Boulevard" in Memphis is
            // physically I-40 but tagged without ref). Collect for node-based
            // assignment in a second pass onto an Interstate graph.
            refless_motorways.push(way);
            continue;
        }

        for reference in &way.refs {
            if is_interstate_highway_ref(reference) {
                ways_by_highway
                    .entry(reference.clone())
                    .or_default()
                    .push(way);
            }
        }
    }

    // Second pass: assign refless motorway ways to highways via shared OSM nodes.
    // Build node → set of highways index from the first pass.
    if !refless_motorways.is_empty() {
        let mut node_highways: HashMap<i64, HashSet<String>> = HashMap::new();
        for (highway, ways) in &ways_by_highway {
            for way in ways {
                for &node_id in &way.nodes {
                    node_highways
                        .entry(node_id)
                        .or_default()
                        .insert(highway.clone());
                }
            }
        }

        // Iterate to handle chains of refless ways (A→B→C where only A touches a ref'd way).
        let mut remaining = refless_motorways;
        let mut total_assigned = 0usize;
        loop {
            let mut unmatched: Vec<&ParsedHighway> = Vec::new();
            let mut assigned_this_round = 0usize;
            for way in remaining {
                let mut matched: HashSet<String> = HashSet::new();
                for &node_id in &way.nodes {
                    if let Some(hws) = node_highways.get(&node_id) {
                        matched.extend(hws.iter().cloned());
                    }
                }
                if matched.is_empty() {
                    unmatched.push(way);
                } else {
                    for highway in &matched {
                        ways_by_highway
                            .entry(highway.clone())
                            .or_default()
                            .push(way);
                    }
                    for &node_id in &way.nodes {
                        node_highways
                            .entry(node_id)
                            .or_default()
                            .extend(matched.iter().cloned());
                    }
                    assigned_this_round += 1;
                }
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

    ways_by_highway
}

fn build_highway_graph(highway_ways: &[&ParsedHighway]) -> Option<HighwayGraph> {
    let mut node_coords: HashMap<i64, (f64, f64)> = HashMap::new();
    let mut neighbors_directed: HashMap<i64, BTreeSet<i64>> = HashMap::new();
    let mut neighbors_undirected: HashMap<i64, BTreeSet<i64>> = HashMap::new();

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
            if !way.is_oneway {
                neighbors_directed.entry(end).or_default().insert(start);
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
    neighbors_undirected: &HashMap<i64, BTreeSet<i64>>,
    neighbors_directed: &HashMap<i64, BTreeSet<i64>>,
    all_exit_node_ids: &HashSet<i64>,
) -> HashSet<i64> {
    let mut stop_nodes: HashSet<i64> = HashSet::new();
    let mut in_degree: HashMap<i64, usize> = HashMap::new();
    let mut out_degree: HashMap<i64, usize> = HashMap::new();

    for (&node, targets) in neighbors_directed {
        *out_degree.entry(node).or_default() += targets.len();
        for &target in targets {
            *in_degree.entry(target).or_default() += 1;
        }
    }

    for &node_id in neighbors_undirected.keys() {
        let incoming = in_degree.get(&node_id).copied().unwrap_or(0);
        let outgoing = out_degree.get(&node_id).copied().unwrap_or(0);
        if !(incoming == 1 && outgoing == 1) {
            stop_nodes.insert(node_id);
        }
    }

    for &node_id in all_exit_node_ids {
        if neighbors_undirected.contains_key(&node_id) {
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

                polyline.push(next_coord);
                length_m +=
                    haversine_distance(cur_coord.0, cur_coord.1, next_coord.0, next_coord.1);
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
                geom_wkt,
                direction: None,
            });
        }
    }

    edges
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

    fn sample_exit(id: &str, osm_id: i64) -> ParsedExit {
        ParsedExit {
            id: id.to_string(),
            osm_id,
        }
    }

    fn sample_highway(
        _id: &str,
        refs: &[&str],
        nodes: &[i64],
        geometry: &[(f64, f64)],
    ) -> ParsedHighway {
        ParsedHighway {
            refs: refs.iter().map(|value| (*value).to_string()).collect(),
            nodes: nodes.to_vec(),
            geometry: geometry.to_vec(),
            highway_type: "motorway".to_string(),
            is_oneway: true,
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

        let (edges, corridor_entries) = compress_highway_graph(&highways, &exits);

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

        let (edges, _) = compress_highway_graph(&highways, &[]);
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

        let (edges, corridor_entries) = compress_highway_graph(&highways, &[]);

        assert_eq!(edges.len(), 1);
        assert_eq!(edges[0].highway, "I-280");
        assert!(corridor_entries.is_empty());
    }
}
