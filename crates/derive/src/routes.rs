use std::cmp::Ordering;
use std::collections::{HashMap, HashSet, VecDeque};

use openinterstate_core::geo::{bearing, haversine_distance, to_degrees, to_radians, EARTH_RADIUS};
use sha2::{Digest, Sha256};
use sqlx::PgPool;
use uuid::Uuid;

const MIN_LENGTH_M: f64 = 50_000.0;
const SPEED_MPS: f64 = 31.2928; // 70 mph
const INTERVAL_S: f64 = 5.0;
const STEP_M: f64 = SPEED_MPS * INTERVAL_S;
const LANE_OFFSET_M: f64 = 3.5;
const ANCHOR_STEP_POINTS: usize = 40;

#[derive(Debug, Clone)]
struct CorridorEdge {
    start_node: i64,
    end_node: i64,
    polyline: Vec<[f64; 2]>,
    length_m: f64,
}

#[derive(Debug, Clone)]
struct CorridorInfo {
    corridor_id: i32,
    highway: String,
    canonical_direction: String,
}

#[derive(Debug, Clone)]
struct RouteRow {
    id: String,
    highway: String,
    direction_code: String,
    direction_label: String,
    display_name: String,
    corridor_id: i32,
    variant_rank: i32,
    distance_m: f64,
    duration_s: f64,
    interval_s: f64,
    point_count: i32,
    start_lat: f64,
    start_lon: f64,
    end_lat: f64,
    end_lon: f64,
    min_lat: f64,
    max_lat: f64,
    min_lon: f64,
    max_lon: f64,
    waypoints_json: String,
    waypoints: Vec<[f64; 2]>,
}

#[derive(Debug, Clone)]
struct AnchorRow {
    route_id: String,
    anchor_index: i32,
    lat: f64,
    lon: f64,
}

pub async fn build_reference_routes(pool: &PgPool) -> anyhow::Result<()> {
    // Load interstate corridors
    let corridor_rows: Vec<(i32, String, Option<String>)> = sqlx::query_as(
        "SELECT corridor_id, highway, canonical_direction \
         FROM corridors WHERE highway LIKE 'I-%'",
    )
    .fetch_all(pool)
    .await?;

    if corridor_rows.is_empty() {
        tracing::warn!("No interstate corridors found; skipping reference route build");
        clear_tables(pool).await?;
        return Ok(());
    }

    let corridors: Vec<CorridorInfo> = corridor_rows
        .into_iter()
        .filter_map(|(id, highway, dir)| {
            Some(CorridorInfo {
                corridor_id: id,
                highway,
                canonical_direction: dir?,
            })
        })
        .collect();

    // Load edges for all interstate corridors
    let edge_rows: Vec<(i32, i64, i64, i32, String)> = sqlx::query_as(
        "SELECT corridor_id, start_node, end_node, length_m, polyline_json \
         FROM highway_edges \
         WHERE corridor_id IS NOT NULL AND highway LIKE 'I-%'",
    )
    .fetch_all(pool)
    .await?;

    let mut edges_by_corridor: HashMap<i32, Vec<CorridorEdge>> = HashMap::new();
    for (corridor_id, start_node, end_node, length_m, polyline_json) in edge_rows {
        let polyline = parse_polyline_json(&polyline_json);
        if polyline.len() < 2 {
            continue;
        }
        edges_by_corridor
            .entry(corridor_id)
            .or_default()
            .push(CorridorEdge {
                start_node,
                end_node,
                polyline,
                length_m: length_m as f64,
            });
    }

    let mut routes = build_corridor_routes(&corridors, &edges_by_corridor)?;
    collapse_same_highway_corridors(&mut routes)?;

    // Assign variant ranks
    let mut rank_map: HashMap<(String, String), i32> = HashMap::new();
    for route in &mut routes {
        let key = (route.highway.clone(), route.direction_code.clone());
        let rank = rank_map.entry(key).or_insert(0);
        *rank += 1;
        route.variant_rank = *rank;
    }

    let anchors = build_anchors(&routes);

    // Write to DB
    let mut tx = pool.begin().await?;
    sqlx::query("DELETE FROM reference_route_anchors")
        .execute(&mut *tx)
        .await?;
    sqlx::query("DELETE FROM reference_routes")
        .execute(&mut *tx)
        .await?;

    for route in &routes {
        sqlx::query(
            "INSERT INTO reference_routes \
                (id, highway, direction_code, direction_label, display_name, corridor_id, variant_rank, \
                 distance_m, duration_s, interval_s, point_count, \
                 start_lat, start_lon, end_lat, end_lon, \
                 min_lat, max_lat, min_lon, max_lon, waypoints_json) \
             VALUES \
                ($1::uuid, $2, $3, $4, $5, $6, $7, \
                 $8, $9, $10, $11, \
                 $12, $13, $14, $15, \
                 $16, $17, $18, $19, $20)",
        )
        .bind(&route.id)
        .bind(&route.highway)
        .bind(&route.direction_code)
        .bind(&route.direction_label)
        .bind(&route.display_name)
        .bind(route.corridor_id)
        .bind(route.variant_rank)
        .bind(route.distance_m)
        .bind(route.duration_s)
        .bind(route.interval_s)
        .bind(route.point_count)
        .bind(route.start_lat)
        .bind(route.start_lon)
        .bind(route.end_lat)
        .bind(route.end_lon)
        .bind(route.min_lat)
        .bind(route.max_lat)
        .bind(route.min_lon)
        .bind(route.max_lon)
        .bind(&route.waypoints_json)
        .execute(&mut *tx)
        .await?;
    }

    for anchor in &anchors {
        sqlx::query(
            "INSERT INTO reference_route_anchors (route_id, anchor_index, lat, lon) \
             VALUES ($1::uuid, $2, $3, $4)",
        )
        .bind(&anchor.route_id)
        .bind(anchor.anchor_index)
        .bind(anchor.lat)
        .bind(anchor.lon)
        .execute(&mut *tx)
        .await?;
    }

    tx.commit().await?;

    tracing::info!(
        "Built {} reference routes and {} anchor points",
        routes.len(),
        anchors.len()
    );

    Ok(())
}

async fn clear_tables(pool: &PgPool) -> anyhow::Result<()> {
    let mut tx = pool.begin().await?;
    sqlx::query("DELETE FROM reference_route_anchors")
        .execute(&mut *tx)
        .await?;
    sqlx::query("DELETE FROM reference_routes")
        .execute(&mut *tx)
        .await?;
    tx.commit().await?;
    Ok(())
}

// ============================================================================
// Route building from corridors
// ============================================================================

fn build_corridor_routes(
    corridors: &[CorridorInfo],
    edges_by_corridor: &HashMap<i32, Vec<CorridorEdge>>,
) -> anyhow::Result<Vec<RouteRow>> {
    let mut routes = Vec::new();

    for corridor in corridors {
        let edges = match edges_by_corridor.get(&corridor.corridor_id) {
            Some(e) if !e.is_empty() => e,
            _ => continue,
        };

        let total_length: f64 = edges.iter().map(|e| e.length_m).sum();
        if total_length < MIN_LENGTH_M {
            continue;
        }

        let direction_code = canonical_to_direction_code(&corridor.canonical_direction);

        let segments = walk_corridor_segments(edges, direction_code);
        if segments.is_empty() {
            continue;
        }

        // Resample + lane offset each segment independently, then concatenate.
        // This avoids interpolating across geographic gaps between disconnected
        // corridor components (which would create points over water/land).
        let mut waypoints: Vec<[f64; 2]> = Vec::new();
        for mut seg in segments {
            if seg.len() < 2 {
                continue;
            }
            // Orient segment in the labeled direction
            if !polyline_matches_direction(&seg, direction_code) {
                seg.reverse();
            }
            let sampled = resample(&seg, STEP_M);
            let offset = apply_lane_offset(&sampled, LANE_OFFSET_M);
            waypoints.extend(offset);
        }

        if waypoints.len() < 2 {
            continue;
        }

        let distance_m = *cumulative_distances(&waypoints).last().unwrap_or(&0.0);
        if distance_m < 1.0 {
            continue;
        }

        let bounds = bounds_for_points(&waypoints);
        let start = waypoints.first().copied().unwrap_or([0.0, 0.0]);
        let end = waypoints.last().copied().unwrap_or([0.0, 0.0]);
        let waypoints_json = serde_json::to_string(&waypoints)?;
        let label = direction_label(&direction_code).to_string();

        let route_id = deterministic_route_id(
            &corridor.highway,
            &direction_code,
            corridor.corridor_id,
            &waypoints_json,
        )
        .to_string();

        routes.push(RouteRow {
            id: route_id,
            highway: corridor.highway.clone(),
            direction_code: direction_code.to_string(),
            direction_label: label.clone(),
            display_name: format!("{} {}", corridor.highway, label),
            corridor_id: corridor.corridor_id,
            variant_rank: 0,
            distance_m,
            duration_s: waypoints.len() as f64 * INTERVAL_S,
            interval_s: INTERVAL_S,
            point_count: waypoints.len() as i32,
            start_lat: start[0],
            start_lon: start[1],
            end_lat: end[0],
            end_lon: end[1],
            min_lat: bounds.0,
            max_lat: bounds.1,
            min_lon: bounds.2,
            max_lon: bounds.3,
            waypoints_json,
            waypoints,
        });
    }

    routes.sort_by(|a, b| {
        let num_a = interstate_number(&a.highway);
        let num_b = interstate_number(&b.highway);
        num_a
            .cmp(&num_b)
            .then_with(|| a.highway.cmp(&b.highway))
            .then_with(|| a.direction_code.cmp(&b.direction_code))
            .then_with(|| {
                b.distance_m
                    .partial_cmp(&a.distance_m)
                    .unwrap_or(Ordering::Equal)
            })
    });

    Ok(routes)
}

/// Walk a corridor's edges to produce ordered polyline segments.
///
/// Returns one polyline per connected component, sorted along the travel
/// axis. Each segment is independently valid — no interpolation across gaps.
fn walk_corridor_segments(edges: &[CorridorEdge], direction_code: &str) -> Vec<Vec<[f64; 2]>> {
    if edges.is_empty() {
        return Vec::new();
    }

    // Build undirected adjacency and collect node coordinates
    let mut adjacency: HashMap<i64, Vec<(i64, usize)>> = HashMap::new();
    let mut node_coords: HashMap<i64, [f64; 2]> = HashMap::new();

    for (idx, edge) in edges.iter().enumerate() {
        adjacency
            .entry(edge.start_node)
            .or_default()
            .push((edge.end_node, idx));
        adjacency
            .entry(edge.end_node)
            .or_default()
            .push((edge.start_node, idx));
        node_coords
            .entry(edge.start_node)
            .or_insert(edge.polyline[0]);
        if let Some(last) = edge.polyline.last() {
            node_coords.entry(edge.end_node).or_insert(*last);
        }
    }

    if node_coords.is_empty() {
        return Vec::new();
    }

    // Find connected components via BFS on edges
    let components = connected_edge_components(edges);

    // For each component, BFS between its axis extremes to get a polyline
    let projection = |coord: &[f64; 2]| -> f64 {
        match direction_code {
            "EB" | "WB" => coord[1],
            _ => coord[0],
        }
    };

    let mut component_polylines: Vec<(f64, Vec<[f64; 2]>)> = Vec::new();

    for comp_edges in &components {
        if comp_edges.is_empty() {
            continue;
        }

        // Build local adjacency for this component
        let mut local_adj: HashMap<i64, Vec<(i64, usize)>> = HashMap::new();
        let mut local_coords: HashMap<i64, [f64; 2]> = HashMap::new();

        for (local_idx, &global_idx) in comp_edges.iter().enumerate() {
            let edge = &edges[global_idx];
            local_adj
                .entry(edge.start_node)
                .or_default()
                .push((edge.end_node, local_idx));
            local_adj
                .entry(edge.end_node)
                .or_default()
                .push((edge.start_node, local_idx));
            local_coords
                .entry(edge.start_node)
                .or_insert(edge.polyline[0]);
            if let Some(last) = edge.polyline.last() {
                local_coords.entry(edge.end_node).or_insert(*last);
            }
        }

        // Find axis-extreme nodes in this component
        let start_node = match local_coords.iter().min_by(|a, b| {
            projection(a.1)
                .partial_cmp(&projection(b.1))
                .unwrap_or(Ordering::Equal)
        }) {
            Some((node, _)) => *node,
            None => continue,
        };
        let end_node = match local_coords.iter().max_by(|a, b| {
            projection(a.1)
                .partial_cmp(&projection(b.1))
                .unwrap_or(Ordering::Equal)
        }) {
            Some((node, _)) => *node,
            None => continue,
        };

        if start_node == end_node {
            continue;
        }

        let parent = match bfs_shortest_path(start_node, end_node, &local_adj) {
            Some(p) => p,
            None => continue,
        };

        // Trace path
        let mut steps: Vec<(i64, i64, usize)> = Vec::new();
        let mut cur = end_node;
        while cur != start_node {
            let &(prev, local_idx) = match parent.get(&cur) {
                Some(p) => p,
                None => break,
            };
            steps.push((prev, cur, local_idx));
            cur = prev;
        }
        if cur != start_node {
            continue;
        }
        steps.reverse();

        // Assemble polyline from edge geometries
        let comp_edge_list: Vec<&CorridorEdge> = comp_edges.iter().map(|&i| &edges[i]).collect();
        let mut points: Vec<[f64; 2]> = Vec::new();
        for &(from, to, local_idx) in &steps {
            let edge = comp_edge_list[local_idx];
            let mut coords = edge.polyline.clone();

            if edge.start_node == from && edge.end_node == to {
                // Forward
            } else if edge.end_node == from && edge.start_node == to {
                coords.reverse();
            } else {
                continue;
            }

            let skip = if points.is_empty() { 0 } else { 1 };
            points.extend(&coords[skip..]);
        }

        if points.len() >= 2 {
            let sort_key = projection(&points[0]).min(projection(points.last().unwrap()));
            component_polylines.push((sort_key, points));
        }
    }

    if component_polylines.is_empty() {
        return Vec::new();
    }

    // Sort components along the travel axis
    component_polylines.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap_or(Ordering::Equal));

    // Return each component as a separate segment (no cross-gap interpolation)
    component_polylines
        .into_iter()
        .map(|(_, poly)| poly)
        .filter(|p| p.len() >= 2)
        .collect()
}

/// Find connected components in the edge graph (undirected).
/// Returns Vec<Vec<usize>> where each inner vec is a set of edge indices.
fn connected_edge_components(edges: &[CorridorEdge]) -> Vec<Vec<usize>> {
    let mut node_to_edges: HashMap<i64, Vec<usize>> = HashMap::new();
    for (idx, edge) in edges.iter().enumerate() {
        node_to_edges.entry(edge.start_node).or_default().push(idx);
        node_to_edges.entry(edge.end_node).or_default().push(idx);
    }

    let mut visited = vec![false; edges.len()];
    let mut components: Vec<Vec<usize>> = Vec::new();

    for seed in 0..edges.len() {
        if visited[seed] {
            continue;
        }

        let mut queue = VecDeque::new();
        let mut seen_nodes: HashSet<i64> = HashSet::new();
        let mut component: Vec<usize> = Vec::new();

        visited[seed] = true;
        component.push(seed);
        seen_nodes.insert(edges[seed].start_node);
        seen_nodes.insert(edges[seed].end_node);
        queue.push_back(edges[seed].start_node);
        queue.push_back(edges[seed].end_node);

        while let Some(node) = queue.pop_front() {
            if let Some(edge_idxs) = node_to_edges.get(&node) {
                for &idx in edge_idxs {
                    if !visited[idx] {
                        visited[idx] = true;
                        component.push(idx);
                        let edge = &edges[idx];
                        if seen_nodes.insert(edge.start_node) {
                            queue.push_back(edge.start_node);
                        }
                        if seen_nodes.insert(edge.end_node) {
                            queue.push_back(edge.end_node);
                        }
                    }
                }
            }
        }

        components.push(component);
    }

    components
}

fn bfs_shortest_path(
    start: i64,
    end: i64,
    adjacency: &HashMap<i64, Vec<(i64, usize)>>,
) -> Option<HashMap<i64, (i64, usize)>> {
    let mut queue = VecDeque::new();
    let mut visited: HashSet<i64> = HashSet::new();
    let mut parent: HashMap<i64, (i64, usize)> = HashMap::new();

    queue.push_back(start);
    visited.insert(start);

    while let Some(node) = queue.pop_front() {
        if node == end {
            return Some(parent);
        }
        if let Some(neighbors) = adjacency.get(&node) {
            for &(next, edge_idx) in neighbors {
                if visited.insert(next) {
                    parent.insert(next, (node, edge_idx));
                    queue.push_back(next);
                }
            }
        }
    }

    None
}

/// Merge corridors for the same (highway, direction_code) into a single route.
///
/// Geographically disconnected interstates (I-76 PA vs CO) will have separate
/// corridors. We merge them by sorting along the travel axis and concatenating.
fn collapse_same_highway_corridors(routes: &mut Vec<RouteRow>) -> anyhow::Result<()> {
    let mut grouped: HashMap<(String, String), Vec<RouteRow>> = HashMap::new();
    for route in std::mem::take(routes) {
        grouped
            .entry((route.highway.clone(), route.direction_code.clone()))
            .or_default()
            .push(route);
    }

    let mut collapsed: Vec<RouteRow> = Vec::new();
    for ((highway, direction_code), mut group) in grouped {
        if group.len() == 1 {
            collapsed.push(group.remove(0));
            continue;
        }

        // Sort corridors by geographic position along the travel axis
        let descending = matches!(direction_code.as_str(), "WB" | "SB");
        group.sort_by(|a, b| {
            let a_key = axis_sort_key(&direction_code, a);
            let b_key = axis_sort_key(&direction_code, b);
            if descending {
                b_key.partial_cmp(&a_key).unwrap_or(Ordering::Equal)
            } else {
                a_key.partial_cmp(&b_key).unwrap_or(Ordering::Equal)
            }
        });

        let mut merged_waypoints: Vec<[f64; 2]> = Vec::new();
        for route in &group {
            if route.waypoints.is_empty() {
                continue;
            }
            if merged_waypoints.is_empty() {
                merged_waypoints.extend(route.waypoints.iter().copied());
                continue;
            }
            // Skip first point if very close to last merged point (avoid duplicate)
            let append_from = match (
                merged_waypoints.last().copied(),
                route.waypoints.first().copied(),
            ) {
                (Some(last), Some(first)) => {
                    if haversine_distance(last[0], last[1], first[0], first[1]) < 35.0 {
                        1
                    } else {
                        0
                    }
                }
                _ => 0,
            };
            merged_waypoints.extend(route.waypoints.iter().skip(append_from).copied());
        }

        if merged_waypoints.len() < 2 {
            continue;
        }

        let distance_m = *cumulative_distances(&merged_waypoints)
            .last()
            .unwrap_or(&0.0);
        if distance_m < 1.0 {
            continue;
        }

        let bounds = bounds_for_points(&merged_waypoints);
        let start = merged_waypoints.first().copied().unwrap_or([0.0, 0.0]);
        let end = merged_waypoints.last().copied().unwrap_or([0.0, 0.0]);
        let waypoints_json = serde_json::to_string(&merged_waypoints)?;
        let label = direction_label(&direction_code).to_string();

        collapsed.push(RouteRow {
            id: deterministic_route_id(&highway, &direction_code, 0, &waypoints_json).to_string(),
            highway: highway.clone(),
            direction_code: direction_code.clone(),
            direction_label: label.clone(),
            display_name: format!("{} {}", highway, label),
            corridor_id: 0,
            variant_rank: 0,
            distance_m,
            duration_s: merged_waypoints.len() as f64 * INTERVAL_S,
            interval_s: INTERVAL_S,
            point_count: merged_waypoints.len() as i32,
            start_lat: start[0],
            start_lon: start[1],
            end_lat: end[0],
            end_lon: end[1],
            min_lat: bounds.0,
            max_lat: bounds.1,
            min_lon: bounds.2,
            max_lon: bounds.3,
            waypoints_json,
            waypoints: merged_waypoints,
        });
    }

    collapsed.sort_by(|a, b| {
        let num_a = interstate_number(&a.highway);
        let num_b = interstate_number(&b.highway);
        num_a
            .cmp(&num_b)
            .then_with(|| a.highway.cmp(&b.highway))
            .then_with(|| a.direction_code.cmp(&b.direction_code))
    });

    *routes = collapsed;
    Ok(())
}

// ============================================================================
// Helpers
// ============================================================================

fn canonical_to_direction_code(canonical: &str) -> &'static str {
    match canonical.to_ascii_lowercase().as_str() {
        "north" => "NB",
        "south" => "SB",
        "east" => "EB",
        "west" => "WB",
        _ => "NB",
    }
}

fn direction_label(direction_code: &str) -> &'static str {
    match direction_code {
        "NB" => "Northbound",
        "SB" => "Southbound",
        "EB" => "Eastbound",
        "WB" => "Westbound",
        _ => "Unknown",
    }
}

fn interstate_number(highway: &str) -> i32 {
    highway
        .strip_prefix("I-")
        .and_then(|v| v.parse::<i32>().ok())
        .unwrap_or(0)
}

fn axis_sort_key(direction_code: &str, route: &RouteRow) -> f64 {
    match direction_code {
        "WB" => route.start_lon.max(route.end_lon),
        "EB" => route.start_lon.min(route.end_lon),
        "SB" => route.start_lat.max(route.end_lat),
        _ => route.start_lat.min(route.end_lat), // NB
    }
}

/// Check whether the polyline's overall heading is consistent with the
/// direction code.  Uses the corridor's dominant axis: NB/SB check
/// latitude increase/decrease, EB/WB check longitude increase/decrease.
/// This handles diagonal interstates (e.g. I-85 at ~54°) correctly.
fn polyline_matches_direction(polyline: &[[f64; 2]], direction_code: &str) -> bool {
    if polyline.len() < 2 {
        return true;
    }
    let first = polyline[0];
    let last = polyline[polyline.len() - 1];

    match direction_code {
        "NB" => last[0] > first[0], // latitude increases going north
        "SB" => last[0] < first[0], // latitude decreases going south
        "EB" => last[1] > first[1], // longitude increases going east
        "WB" => last[1] < first[1], // longitude decreases going west
        _ => true,
    }
}

fn parse_polyline_json(raw: &str) -> Vec<[f64; 2]> {
    serde_json::from_str::<Vec<Vec<f64>>>(raw)
        .unwrap_or_default()
        .into_iter()
        .filter_map(|pair| {
            if pair.len() >= 2 {
                Some([pair[0], pair[1]])
            } else {
                None
            }
        })
        .collect()
}

fn cumulative_distances(points: &[[f64; 2]]) -> Vec<f64> {
    let mut dists = Vec::with_capacity(points.len());
    dists.push(0.0);
    for i in 1..points.len() {
        let d = haversine_distance(
            points[i - 1][0],
            points[i - 1][1],
            points[i][0],
            points[i][1],
        );
        dists.push(dists[i - 1] + d);
    }
    dists
}

fn resample(points: &[[f64; 2]], step_m: f64) -> Vec<[f64; 2]> {
    if points.len() < 2 {
        return points.to_vec();
    }

    let dists = cumulative_distances(points);
    let total = *dists.last().unwrap_or(&0.0);
    if total <= 0.0 {
        return points.to_vec();
    }

    let mut resampled = Vec::new();
    let mut seg_idx = 0usize;
    let mut d = 0.0;

    while d <= total {
        while seg_idx < dists.len().saturating_sub(2) && dists[seg_idx + 1] < d {
            seg_idx += 1;
        }

        let seg_start = dists[seg_idx];
        let seg_end = dists[seg_idx + 1];
        let seg_len = seg_end - seg_start;
        let t = if seg_len > 0.0 {
            (d - seg_start) / seg_len
        } else {
            0.0
        };

        let lat = points[seg_idx][0] + t * (points[seg_idx + 1][0] - points[seg_idx][0]);
        let lon = points[seg_idx][1] + t * (points[seg_idx + 1][1] - points[seg_idx][1]);
        resampled.push([lat, lon]);

        d += step_m;
    }

    if let Some(last) = points.last() {
        let needs_last = match resampled.last() {
            Some(cur) => haversine_distance(cur[0], cur[1], last[0], last[1]) > 10.0,
            None => true,
        };
        if needs_last {
            resampled.push(*last);
        }
    }

    resampled
}

fn apply_lane_offset(points: &[[f64; 2]], offset_m: f64) -> Vec<[f64; 2]> {
    let mut result = Vec::with_capacity(points.len());

    for i in 0..points.len() {
        let brg = if i < points.len() - 1 {
            bearing(
                points[i][0],
                points[i][1],
                points[i + 1][0],
                points[i + 1][1],
            )
        } else {
            bearing(
                points[i - 1][0],
                points[i - 1][1],
                points[i][0],
                points[i][1],
            )
        };

        let (lat, lon) = offset_point(points[i][0], points[i][1], brg, offset_m);
        result.push([lat, lon]);
    }

    result
}

fn offset_point(lat: f64, lon: f64, bearing_deg: f64, offset_m: f64) -> (f64, f64) {
    let perp_bearing = to_radians((bearing_deg + 90.0) % 360.0);
    let angular_dist = offset_m / EARTH_RADIUS;
    let lat1 = to_radians(lat);
    let lon1 = to_radians(lon);

    let lat2 = (lat1.sin() * angular_dist.cos()
        + lat1.cos() * angular_dist.sin() * perp_bearing.cos())
    .asin();
    let lon2 = lon1
        + (perp_bearing.sin() * angular_dist.sin() * lat1.cos())
            .atan2(angular_dist.cos() - lat1.sin() * lat2.sin());

    (to_degrees(lat2), to_degrees(lon2))
}

fn bounds_for_points(points: &[[f64; 2]]) -> (f64, f64, f64, f64) {
    let mut min_lat = f64::INFINITY;
    let mut max_lat = f64::NEG_INFINITY;
    let mut min_lon = f64::INFINITY;
    let mut max_lon = f64::NEG_INFINITY;

    for &[lat, lon] in points {
        min_lat = min_lat.min(lat);
        max_lat = max_lat.max(lat);
        min_lon = min_lon.min(lon);
        max_lon = max_lon.max(lon);
    }

    (min_lat, max_lat, min_lon, max_lon)
}

fn deterministic_route_id(
    highway: &str,
    direction_code: &str,
    corridor_id: i32,
    waypoints_json: &str,
) -> Uuid {
    let digest = Sha256::digest(waypoints_json.as_bytes());
    let key = format!("{}|{}|{}|{:x}", highway, direction_code, corridor_id, digest);
    let namespace = Uuid::from_u128(0x0f186e5eb6ea4dc9bd5f6b52471221d8);
    Uuid::new_v5(&namespace, key.as_bytes())
}

fn build_anchors(routes: &[RouteRow]) -> Vec<AnchorRow> {
    let mut anchors = Vec::new();

    for route in routes {
        if route.waypoints.is_empty() {
            continue;
        }

        let mut idx = 0usize;
        while idx < route.waypoints.len() {
            let p = route.waypoints[idx];
            anchors.push(AnchorRow {
                route_id: route.id.clone(),
                anchor_index: idx as i32,
                lat: p[0],
                lon: p[1],
            });
            idx += ANCHOR_STEP_POINTS;
        }

        let last_idx = route.waypoints.len() - 1;
        if anchors.last().map(|a| a.anchor_index as usize) != Some(last_idx) {
            let p = route.waypoints[last_idx];
            anchors.push(AnchorRow {
                route_id: route.id.clone(),
                anchor_index: last_idx as i32,
                lat: p[0],
                lon: p[1],
            });
        }
    }

    anchors
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn route_id_is_deterministic() {
        let id_a = deterministic_route_id("I-95", "NB", 1, "[[1,2],[3,4]]");
        let id_b = deterministic_route_id("I-95", "NB", 1, "[[1,2],[3,4]]");
        assert_eq!(id_a, id_b);
    }

    #[test]
    fn route_id_changes_for_direction() {
        let id_nb = deterministic_route_id("I-95", "NB", 1, "[[1,2],[3,4]]");
        let id_sb = deterministic_route_id("I-95", "SB", 1, "[[1,2],[3,4]]");
        assert_ne!(id_nb, id_sb);
    }

    #[test]
    fn direction_labels_are_user_facing() {
        assert_eq!(direction_label("NB"), "Northbound");
        assert_eq!(direction_label("SB"), "Southbound");
        assert_eq!(direction_label("EB"), "Eastbound");
        assert_eq!(direction_label("WB"), "Westbound");
    }

    #[test]
    fn canonical_to_direction_code_maps_correctly() {
        assert_eq!(canonical_to_direction_code("north"), "NB");
        assert_eq!(canonical_to_direction_code("South"), "SB");
        assert_eq!(canonical_to_direction_code("EAST"), "EB");
        assert_eq!(canonical_to_direction_code("west"), "WB");
    }

    #[test]
    fn walk_corridor_polyline_chains_edges() {
        let edges = vec![
            CorridorEdge {
                start_node: 1,
                end_node: 2,
                polyline: vec![[30.0, -87.0], [30.5, -87.0]],
                length_m: 50_000.0,
            },
            CorridorEdge {
                start_node: 2,
                end_node: 3,
                polyline: vec![[30.5, -87.0], [31.0, -87.0]],
                length_m: 50_000.0,
            },
        ];
        let segments = walk_corridor_segments(&edges, "NB");
        assert_eq!(segments.len(), 1);
        let poly = &segments[0];
        assert_eq!(poly.len(), 3);
        assert_eq!(poly[0], [30.0, -87.0]);
        assert_eq!(poly[2], [31.0, -87.0]);
    }

    #[test]
    fn walk_corridor_polyline_spans_axis_extremes() {
        let edges = vec![
            CorridorEdge {
                start_node: 1,
                end_node: 2,
                polyline: vec![[30.0, -87.0], [30.5, -87.0]],
                length_m: 50_000.0,
            },
            // Short spur off the main trunk
            CorridorEdge {
                start_node: 2,
                end_node: 4,
                polyline: vec![[30.5, -87.0], [30.6, -87.0]],
                length_m: 1_000.0,
            },
            // Main trunk continues
            CorridorEdge {
                start_node: 2,
                end_node: 3,
                polyline: vec![[30.5, -87.0], [31.0, -87.0]],
                length_m: 50_000.0,
            },
            CorridorEdge {
                start_node: 3,
                end_node: 5,
                polyline: vec![[31.0, -87.0], [31.5, -87.0]],
                length_m: 50_000.0,
            },
        ];
        // NB axis extremes: min lat 30.0 (node 1) → max lat 31.5 (node 5)
        let segments = walk_corridor_segments(&edges, "NB");
        assert_eq!(segments.len(), 1);
        let poly = &segments[0];
        assert_eq!(poly.first().unwrap()[0], 30.0);
        assert_eq!(poly.last().unwrap()[0], 31.5);
    }
}
