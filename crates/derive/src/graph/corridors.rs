//! Build corridor abstractions from the highway edge graph.
//!
//! A corridor represents one continuous travel direction on a highway,
//! merging fragmented graph components into a single ordered exit sequence.
//!
//! Merge pipeline per highway:
//! 1. Directional split — split mixed-direction components via directed BFS
//! 2. Absorb tiny post-split fragments (≤10 edges) back into parent
//! 3. Shared-node merge — union-find on components sharing graph nodes
//! 4. Motorway-link bridge merge — components connected via ref'd links (cosine > 0.5)
//! 5. Proximity merge — terminal nodes within 10km + bearing compatibility
//! 6. BBox overlap merge — same-direction components with overlapping bounding boxes
//! 7. Walk exits — collect exits by graph node membership, order by displacement projection
//! 8. Minor corridor absorption — corridors with <10% edges or ≤5 exits absorbed into primary

use std::collections::{BTreeMap, HashMap, HashSet};

use openinterstate_core::geo::haversine_distance;
use openinterstate_core::highway_ref::is_interstate_highway_ref;
use sqlx::PgPool;

// ============================================================================
// Public types
// ============================================================================

pub struct BuildCorridorsStats {
    pub corridors_created: usize,
    pub corridor_exits_created: usize,
    pub edges_updated: usize,
}

// ============================================================================
// Internal types
// ============================================================================

type EdgeDbRow = (
    String,         // id
    String,         // highway
    i32,            // component
    i64,            // start_node
    i64,            // end_node
    i32,            // length_m
    Option<String>, // direction
    f64,            // start_lat
    f64,            // start_lon
    f64,            // end_lat
    f64,            // end_lon
);

type ExitDbRow = (
    String,         // exit_id
    String,         // highway
    i32,            // graph_component
    i64,            // graph_node
    Option<String>, // ref
    Option<String>, // name
    f64,            // lat
    f64,            // lon
);

#[derive(Clone)]
struct EdgeData {
    id: String,
    highway: String,
    component: i32,
    start_node: i64,
    end_node: i64,
    length_m: i32,
    direction: Option<String>,
    start_lat: f64,
    start_lon: f64,
    end_lat: f64,
    end_lon: f64,
}

#[derive(Clone)]
struct ExitData {
    exit_id: String,
    graph_node: i64,
    ref_val: Option<String>,
    name: Option<String>,
    lat: f64,
    lon: f64,
}

struct CorridorResult {
    corridor_id: i32,
    highway: String,
    canonical_direction: Option<String>,
    exits: Vec<CorridorExitResult>,
    member_edge_ids: Vec<String>,
    /// Displacement vector (delta_lat, delta_lon) for projection-based exit sorting.
    displacement: (f64, f64),
    sample_points: Vec<(f64, f64)>,
}

struct CorridorExitResult {
    exit_id: String,
    ref_val: Option<String>,
    name: Option<String>,
    lat: f64,
    lon: f64,
}

const MINOR_CORRIDOR_ABSORPTION_MAX_GAP_M: f64 = 10_000.0;

// ============================================================================
// Union-Find
// ============================================================================

struct UnionFind {
    parent: Vec<usize>,
    rank: Vec<usize>,
}

impl UnionFind {
    fn new(n: usize) -> Self {
        Self {
            parent: (0..n).collect(),
            rank: vec![0; n],
        }
    }

    fn find(&mut self, mut x: usize) -> usize {
        while self.parent[x] != x {
            self.parent[x] = self.parent[self.parent[x]];
            x = self.parent[x];
        }
        x
    }

    fn union(&mut self, a: usize, b: usize) -> bool {
        let ra = self.find(a);
        let rb = self.find(b);
        if ra == rb {
            return false;
        }
        if self.rank[ra] < self.rank[rb] {
            self.parent[ra] = rb;
        } else if self.rank[ra] > self.rank[rb] {
            self.parent[rb] = ra;
        } else {
            self.parent[rb] = ra;
            self.rank[ra] += 1;
        }
        true
    }
}

// ============================================================================
// Main entry point
// ============================================================================

pub async fn build_corridors(pool: &PgPool) -> Result<BuildCorridorsStats, anyhow::Error> {
    tracing::info!("Loading highway edges for corridor building...");
    let raw_edges = load_edges(pool).await?;
    tracing::info!("Loaded {} edges", raw_edges.len());

    tracing::info!("Loading exit corridor data...");
    let raw_exits = load_exits(pool).await?;
    tracing::info!("Loaded {} exit-corridor entries", raw_exits.len());

    tracing::info!("Loading motorway_link bridges between components...");
    let link_bridges = load_motorway_link_bridges(pool).await?;
    tracing::info!(
        "Found {} motorway_link terminal bridge pairs",
        link_bridges.len()
    );

    tracing::info!("Loading ref'd motorway_link endpoints...");
    let refd_links = load_refd_link_endpoints(pool).await?;
    tracing::info!("Loaded {} ref'd link endpoints", refd_links.len());

    // Group by highway
    let mut edges_by_highway: BTreeMap<String, Vec<EdgeData>> = BTreeMap::new();
    for e in raw_edges {
        edges_by_highway
            .entry(e.highway.clone())
            .or_default()
            .push(e);
    }
    let exits_by_highway: BTreeMap<String, Vec<ExitData>> = raw_exits.into_iter().collect();

    // Group link bridges by highway
    let mut bridges_by_highway: HashMap<String, Vec<(i32, i32)>> = HashMap::new();
    for (highway, comp_a, comp_b) in &link_bridges {
        bridges_by_highway
            .entry(highway.clone())
            .or_default()
            .push((*comp_a, *comp_b));
    }

    let mut all_corridors: Vec<CorridorResult> = Vec::new();
    let mut next_corridor_id: i32 = 1;

    for (highway, hw_edges) in &edges_by_highway {
        let hw_exits = exits_by_highway
            .get(highway)
            .map(|v| v.as_slice())
            .unwrap_or(&[]);
        let mut hw_bridges: Vec<(i32, i32)> = bridges_by_highway
            .get(highway)
            .map(|v| v.clone())
            .unwrap_or_default();
        // Add ref'd link bridge pairs (non-terminal node connections)
        let refd_pairs = find_refd_link_bridge_pairs(highway, hw_edges, &refd_links);
        hw_bridges.extend(refd_pairs);
        let corridors = build_highway_corridors(
            highway,
            hw_edges,
            hw_exits,
            &hw_bridges,
            &mut next_corridor_id,
        );
        if !corridors.is_empty() {
            tracing::info!(
                "  {}: {} corridors, {} total corridor exits",
                highway,
                corridors.len(),
                corridors.iter().map(|c| c.exits.len()).sum::<usize>()
            );
        }
        all_corridors.extend(corridors);
    }

    let stats = write_corridors(pool, &all_corridors).await?;
    tracing::info!(
        "Corridor build complete: {} corridors, {} exits, {} edges updated",
        stats.corridors_created,
        stats.corridor_exits_created,
        stats.edges_updated
    );
    Ok(stats)
}

// ============================================================================
// DB loading
// ============================================================================

async fn load_edges(pool: &PgPool) -> Result<Vec<EdgeData>, anyhow::Error> {
    let rows: Vec<EdgeDbRow> = sqlx::query_as(
        "SELECT id, highway, component, start_node, end_node, length_m, direction, \
         ST_Y(ST_StartPoint(geom)) as start_lat, ST_X(ST_StartPoint(geom)) as start_lon, \
         ST_Y(ST_EndPoint(geom)) as end_lat, ST_X(ST_EndPoint(geom)) as end_lon \
         FROM highway_edges \
         WHERE highway LIKE 'I-%' \
         ORDER BY highway, component, id",
    )
    .fetch_all(pool)
    .await?;

    Ok(rows
        .into_iter()
        .map(|r| EdgeData {
            id: r.0,
            highway: r.1,
            component: r.2,
            start_node: r.3,
            end_node: r.4,
            length_m: r.5,
            direction: r.6,
            start_lat: r.7,
            start_lon: r.8,
            end_lat: r.9,
            end_lon: r.10,
        })
        .collect())
}

async fn load_exits(pool: &PgPool) -> Result<Vec<(String, Vec<ExitData>)>, anyhow::Error> {
    let rows: Vec<ExitDbRow> = sqlx::query_as(
        "SELECT ec.exit_id, ec.highway, ec.graph_component, ec.graph_node, \
         e.ref, e.name, ST_Y(e.geom) as lat, ST_X(e.geom) as lon \
         FROM exit_corridors ec \
         JOIN exits e ON e.id = ec.exit_id \
         WHERE ec.highway LIKE 'I-%' \
         ORDER BY ec.highway, ec.graph_component",
    )
    .fetch_all(pool)
    .await?;

    // Group by highway, each exit becomes ExitData
    let mut by_highway: BTreeMap<String, Vec<ExitData>> = BTreeMap::new();
    for r in rows {
        by_highway.entry(r.1.clone()).or_default().push(ExitData {
            exit_id: r.0,
            graph_node: r.3,
            ref_val: r.4,
            name: r.5,
            lat: r.6,
            lon: r.7,
        });
    }

    Ok(by_highway.into_iter().collect())
}

/// Find pairs of (highway, component_a, component_b) that are connected through
/// motorway_link ways carrying the same highway ref. These links are mainline
/// connectors at interchanges (not exit ramps) that were excluded from the edge
/// graph during import.
///
/// At interchange merges/splits, OSM uses motorway_link ways to connect the
/// separate carriageway to the concurrent segment. These links carry the
/// interstate ref (e.g. "I 40"), unlike exit ramps which typically have no ref.
/// We find terminal node pairs from different components that are:
/// - Within 2km of each other
/// - Both within 100m of a motorway_link way carrying the matching ref
async fn load_motorway_link_bridges(
    pool: &PgPool,
) -> Result<Vec<(String, i32, i32)>, anyhow::Error> {
    let rows: Vec<(String, i32, i32)> = sqlx::query_as(
        "WITH terminal_nodes AS ( \
           SELECT he.highway, he.component, \
                  he.start_node AS node, ST_StartPoint(he.geom) AS pt \
           FROM highway_edges he \
           WHERE he.highway LIKE 'I-%' \
             AND NOT EXISTS ( \
               SELECT 1 FROM highway_edges he2 \
               WHERE he2.highway = he.highway AND he2.component = he.component \
                 AND he2.end_node = he.start_node \
           ) \
           UNION ALL \
           SELECT he.highway, he.component, \
                  he.end_node AS node, ST_EndPoint(he.geom) AS pt \
           FROM highway_edges he \
           WHERE he.highway LIKE 'I-%' \
             AND NOT EXISTS ( \
             SELECT 1 FROM highway_edges he2 \
             WHERE he2.highway = he.highway AND he2.component = he.component \
               AND he2.start_node = he.end_node \
           ) \
         ), \
         terminals_with_link AS ( \
           SELECT DISTINCT t.highway, t.component, t.node, t.pt \
           FROM terminal_nodes t \
           WHERE EXISTS ( \
             SELECT 1 FROM osm2pgsql_v2_highways ml \
             WHERE ml.highway = 'motorway_link' \
               AND ml.ref IS NOT NULL \
               AND ml.ref LIKE '%' || REPLACE(t.highway, '-', ' ') || '%' \
               AND ST_DWithin(ml.geom::geography, t.pt::geography, 100) \
           ) \
         ) \
         SELECT DISTINCT t1.highway, \
                LEAST(t1.component, t2.component) AS comp_a, \
                GREATEST(t1.component, t2.component) AS comp_b \
         FROM terminals_with_link t1 \
         JOIN terminals_with_link t2 \
           ON t1.highway = t2.highway \
          AND t1.component < t2.component \
          AND ST_DWithin(t1.pt::geography, t2.pt::geography, 2000)",
    )
    .fetch_all(pool)
    .await?;

    Ok(rows)
}

/// Load ref'd motorway_link way endpoints from osm2pgsql_v2_highways.
/// Returns (normalized_highway_ref, first_node_id, last_node_id) for each link.
///
/// These are interchange connectors (not exit ramps) that bridge components
/// at non-terminal nodes. Unlike the terminal-based bridge query, this catches
/// links like the I-10 connector at the I-5 interchange in LA where both
/// endpoint nodes are mid-corridor rather than terminal.
async fn load_refd_link_endpoints(pool: &PgPool) -> Result<Vec<(String, i64, i64)>, anyhow::Error> {
    let rows: Vec<(Option<String>, Vec<i64>)> = sqlx::query_as(
        "SELECT ref, node_ids \
         FROM osm2pgsql_v2_highways \
         WHERE highway = 'motorway_link' \
           AND ref IS NOT NULL \
           AND ref ~* '(^|;)[[:space:]]*I[ -]?[0-9]' \
           AND array_length(node_ids, 1) >= 2",
    )
    .fetch_all(pool)
    .await?;

    let mut result = Vec::new();
    for (ref_raw, node_ids) in rows {
        let Some(ref_str) = ref_raw.as_deref() else {
            continue;
        };
        let first = node_ids[0];
        let last = node_ids[node_ids.len() - 1];
        // Each link may carry multiple refs (semicolon-separated)
        for part in ref_str.split(';') {
            if let Some(normalized) =
                openinterstate_core::highway_ref::normalize_highway_ref(part.trim())
            {
                if !is_interstate_highway_ref(&normalized) {
                    continue;
                }
                result.push((normalized, first, last));
            }
        }
    }
    Ok(result)
}

/// Given edges and ref'd link endpoints, find pairs of components that are
/// connected via a ref'd motorway_link (at ANY node, not just terminals).
fn find_refd_link_bridge_pairs(
    highway: &str,
    edges: &[EdgeData],
    refd_links: &[(String, i64, i64)],
) -> Vec<(i32, i32)> {
    // Build node → component(s) from edges
    let mut node_to_comps: HashMap<i64, HashSet<i32>> = HashMap::new();
    for e in edges {
        node_to_comps
            .entry(e.start_node)
            .or_default()
            .insert(e.component);
        node_to_comps
            .entry(e.end_node)
            .or_default()
            .insert(e.component);
    }

    let mut pairs: HashSet<(i32, i32)> = HashSet::new();
    for (ref_hw, first, last) in refd_links {
        if ref_hw != highway {
            continue;
        }
        let comps_first = node_to_comps.get(first);
        let comps_last = node_to_comps.get(last);
        if let (Some(cf), Some(cl)) = (comps_first, comps_last) {
            for &ca in cf {
                for &cb in cl {
                    if ca != cb {
                        let pair = if ca < cb { (ca, cb) } else { (cb, ca) };
                        pairs.insert(pair);
                    }
                }
            }
        }
    }
    pairs.into_iter().collect()
}

// ============================================================================
// Per-highway corridor building
// ============================================================================

/// Detect components with mixed travel directions (both NB and SB edges) and
/// split them into directional sub-components using directed BFS.
///
/// The graph compression stage uses undirected BFS for component assignment,
/// which merges opposing carriageways when they share nodes at interchanges.
/// For a highway like I-75, both the northbound and southbound carriageways
/// end up in the same component.
///
/// This function detects such mixed components (>30% minority direction) and
/// splits them by running directed BFS — edges only connect through the
/// directed adjacency (start_node → end_node), so opposing-direction edges
/// form separate connected components.
///
/// Returns cloned EdgeData with updated component IDs (synthetic negative IDs
/// for split sub-components to avoid collisions with real component IDs).
fn split_mixed_direction_components(highway: &str, edges: &[EdgeData]) -> Vec<EdgeData> {
    // Group edges by component
    let mut edges_by_comp: HashMap<i32, Vec<usize>> = HashMap::new();
    for (i, e) in edges.iter().enumerate() {
        edges_by_comp.entry(e.component).or_default().push(i);
    }

    // Determine the highway's primary axis using geographic span
    let (min_lat, max_lat, min_lon, max_lon) = edges.iter().fold(
        (
            f64::INFINITY,
            f64::NEG_INFINITY,
            f64::INFINITY,
            f64::NEG_INFINITY,
        ),
        |(mnla, mxla, mnlo, mxlo), e| {
            (
                mnla.min(e.start_lat).min(e.end_lat),
                mxla.max(e.start_lat).max(e.end_lat),
                mnlo.min(e.start_lon).min(e.end_lon),
                mxlo.max(e.start_lon).max(e.end_lon),
            )
        },
    );
    let lat_span = max_lat - min_lat;
    let lon_span = (max_lon - min_lon) * (((min_lat + max_lat) / 2.0).to_radians().cos());
    let is_ns_axis = lat_span >= lon_span;

    let mut result: Vec<EdgeData> = Vec::with_capacity(edges.len());
    let mut next_synthetic_id: i32 = -1;
    let mut total_splits = 0usize;

    for (&comp, edge_indices) in &edges_by_comp {
        // Count edges by primary axis direction
        let mut forward = 0usize; // north or east
        let mut reverse = 0usize; // south or west
        for &idx in edge_indices {
            let e = &edges[idx];
            let goes_forward = if is_ns_axis {
                e.end_lat > e.start_lat
            } else {
                e.end_lon > e.start_lon
            };
            if goes_forward {
                forward += 1;
            } else {
                reverse += 1;
            }
        }

        let total = forward + reverse;
        let minority_pct = forward.min(reverse) as f64 / total as f64;

        // Only split if both directions are significantly represented
        if minority_pct < 0.25 || total < 20 {
            // Keep component as-is
            for &idx in edge_indices {
                result.push(edges[idx].clone());
            }
            continue;
        }

        // Classify edges by direction, then BFS within each direction group.
        // This prevents shared interchange nodes from mixing carriageways.
        let mut forward_indices: Vec<usize> = Vec::new();
        let mut reverse_indices: Vec<usize> = Vec::new();
        for &idx in edge_indices {
            let e = &edges[idx];
            let goes_forward = if is_ns_axis {
                e.end_lat > e.start_lat
            } else {
                e.end_lon > e.start_lon
            };
            if goes_forward {
                forward_indices.push(idx);
            } else {
                reverse_indices.push(idx);
            }
        }

        let mut edge_to_sub: HashMap<usize, i32> = HashMap::new();

        // BFS within each direction group separately
        for dir_indices in [&forward_indices, &reverse_indices] {
            // Build adjacency only from edges in this direction group
            let dir_set: HashSet<usize> = dir_indices.iter().copied().collect();
            let mut fwd_adj: HashMap<i64, Vec<usize>> = HashMap::new();
            let mut rev_adj: HashMap<i64, Vec<usize>> = HashMap::new();
            for &idx in dir_indices {
                let e = &edges[idx];
                fwd_adj.entry(e.start_node).or_default().push(idx);
                rev_adj.entry(e.end_node).or_default().push(idx);
            }

            let mut visited: HashSet<usize> = HashSet::new();
            for &seed_idx in dir_indices {
                if visited.contains(&seed_idx) {
                    continue;
                }
                let sub_comp = next_synthetic_id;
                next_synthetic_id -= 1;

                let mut queue = std::collections::VecDeque::new();
                queue.push_back(seed_idx);
                visited.insert(seed_idx);
                edge_to_sub.insert(seed_idx, sub_comp);

                while let Some(cur_idx) = queue.pop_front() {
                    if let Some(next_edges) = fwd_adj.get(&edges[cur_idx].end_node) {
                        for &next_idx in next_edges {
                            if dir_set.contains(&next_idx) && visited.insert(next_idx) {
                                edge_to_sub.insert(next_idx, sub_comp);
                                queue.push_back(next_idx);
                            }
                        }
                    }
                    if let Some(prev_edges) = rev_adj.get(&edges[cur_idx].start_node) {
                        for &prev_idx in prev_edges {
                            if dir_set.contains(&prev_idx) && visited.insert(prev_idx) {
                                edge_to_sub.insert(prev_idx, sub_comp);
                                queue.push_back(prev_idx);
                            }
                        }
                    }
                }
            }
        }

        // Count resulting sub-components
        let sub_comps: HashSet<i32> = edge_to_sub.values().copied().collect();
        if sub_comps.len() > 1 {
            total_splits += 1;
            tracing::debug!(
                "  {}: split component {} ({} edges, {:.0}% minority) into {} sub-components",
                highway,
                comp,
                total,
                minority_pct * 100.0,
                sub_comps.len()
            );
        }

        for &idx in edge_indices {
            let mut edge = edges[idx].clone();
            if let Some(&sub) = edge_to_sub.get(&idx) {
                edge.component = sub;
            }
            result.push(edge);
        }
    }

    if total_splits > 0 {
        tracing::info!(
            "  {}: split {} mixed-direction components into directional sub-components",
            highway,
            total_splits
        );
    }

    result
}

fn build_highway_corridors(
    highway: &str,
    edges: &[EdgeData],
    exits: &[ExitData],
    link_bridges: &[(i32, i32)],
    next_id: &mut i32,
) -> Vec<CorridorResult> {
    if edges.is_empty() {
        return Vec::new();
    }

    // 0. Split mixed-direction components into directional sub-components.
    //    The graph compression uses undirected BFS, so opposing carriageways
    //    that share nodes at interchanges get merged into one component.
    //    We detect this and split them using directed BFS.
    //
    //    Build a mapping from original component IDs to post-split IDs so
    //    that the motorway-link bridge pairs (which use original IDs) can
    //    be translated to post-split IDs.
    let orig_comp_by_edge_id: HashMap<&str, i32> =
        edges.iter().map(|e| (e.id.as_str(), e.component)).collect();
    let split_edges = split_mixed_direction_components(highway, edges);
    let edges = &split_edges;

    let mut orig_to_split: HashMap<i32, HashSet<i32>> = HashMap::new();
    for e in edges {
        let orig = orig_comp_by_edge_id
            .get(e.id.as_str())
            .copied()
            .unwrap_or(e.component);
        orig_to_split.entry(orig).or_default().insert(e.component);
    }

    // 0b. Absorb tiny post-split fragments (≤10 edges) back into the largest
    //     sub-component from the same original component.  These are interchange
    //     artifacts from the directional split, not real corridors.
    let mut split_edges = split_edges;
    {
        // Count edges per post-split component
        let mut comp_edge_count: HashMap<i32, usize> = HashMap::new();
        for e in &split_edges {
            *comp_edge_count.entry(e.component).or_default() += 1;
        }

        let mut remap: HashMap<i32, i32> = HashMap::new();
        for (_orig, subs) in &orig_to_split {
            if subs.len() <= 1 {
                continue; // No split happened
            }
            // Find the largest sub-component
            let largest = subs
                .iter()
                .max_by_key(|&&c| comp_edge_count.get(&c).copied().unwrap_or(0))
                .copied()
                .unwrap();
            // Tiny sub-components get absorbed
            for &sub in subs {
                if sub != largest {
                    let count = comp_edge_count.get(&sub).copied().unwrap_or(0);
                    if count <= 10 {
                        remap.insert(sub, largest);
                    }
                }
            }
        }
        if !remap.is_empty() {
            let absorbed: usize = remap.values().count();
            tracing::info!(
                "  {}: absorbed {} tiny post-split fragments",
                highway,
                absorbed
            );
            for e in &mut split_edges {
                if let Some(&target) = remap.get(&e.component) {
                    e.component = target;
                }
            }
        }
    }
    let edges = &split_edges;

    // 1. Identify distinct components and build index
    let mut component_set: Vec<i32> = edges.iter().map(|e| e.component).collect();
    component_set.sort_unstable();
    component_set.dedup();

    if component_set.is_empty() {
        return Vec::new();
    }

    let comp_to_idx: HashMap<i32, usize> = component_set
        .iter()
        .enumerate()
        .map(|(i, &c)| (c, i))
        .collect();
    let n = component_set.len();

    // Build node coordinates map
    let mut node_coords: HashMap<i64, (f64, f64)> = HashMap::new();
    for e in edges {
        node_coords
            .entry(e.start_node)
            .or_insert((e.start_lat, e.start_lon));
        node_coords
            .entry(e.end_node)
            .or_insert((e.end_lat, e.end_lon));
    }

    // Group edges by component
    let mut edges_by_comp: HashMap<i32, Vec<&EdgeData>> = HashMap::new();
    for e in edges {
        edges_by_comp.entry(e.component).or_default().push(e);
    }

    // Compute displacement vector per component for direction compatibility checks
    let mut comp_displacement: HashMap<i32, (f64, f64)> = HashMap::new();
    for (&comp, comp_edges) in &edges_by_comp {
        let mut dlat = 0.0_f64;
        let mut dlon = 0.0_f64;
        for e in comp_edges {
            dlat += e.end_lat - e.start_lat;
            dlon += e.end_lon - e.start_lon;
        }
        comp_displacement.insert(comp, (dlat, dlon));
    }

    // Debug: show post-split component state
    for &comp in &component_set {
        let count = edges_by_comp.get(&comp).map(|v| v.len()).unwrap_or(0);
        let (dlat, dlon) = comp_displacement.get(&comp).copied().unwrap_or((0.0, 0.0));
        let mag = (dlat * dlat + dlon * dlon).sqrt();
        tracing::debug!(
            "  {}: comp {} → {} edges, displacement ({:.4}, {:.4}), mag {:.4}",
            highway,
            comp,
            count,
            dlat,
            dlon,
            mag,
        );
    }

    let mut uf = UnionFind::new(n);

    // Helper: compute aggregate displacement for a union-find group.
    let group_displacement = |uf: &mut UnionFind, comp_idx: usize| -> (f64, f64) {
        let root = uf.find(comp_idx);
        let mut dlat = 0.0_f64;
        let mut dlon = 0.0_f64;
        for (j, &c) in component_set.iter().enumerate() {
            if uf.find(j) == root {
                let (dl, dn) = comp_displacement.get(&c).copied().unwrap_or((0.0, 0.0));
                dlat += dl;
                dlon += dn;
            }
        }
        (dlat, dlon)
    };

    // 2. Union-find: merge components sharing nodes (only if same travel direction).
    //    Uses group-aggregate displacement so direction checks stay stable as
    //    groups accrete multiple fragments.
    let mut node_to_comps: HashMap<i64, Vec<i32>> = HashMap::new();
    for e in edges {
        node_to_comps
            .entry(e.start_node)
            .or_default()
            .push(e.component);
        node_to_comps
            .entry(e.end_node)
            .or_default()
            .push(e.component);
    }
    let mut shared_merges = 0usize;
    for comps in node_to_comps.values() {
        let unique: Vec<i32> = {
            let mut v = comps.clone();
            v.sort_unstable();
            v.dedup();
            v
        };
        for i in 1..unique.len() {
            let a = comp_to_idx[&unique[0]];
            let b = comp_to_idx[&unique[i]];
            // Use group-aggregate displacement for direction checks
            let (dlat_a, dlon_a) = group_displacement(&mut uf, a);
            let (dlat_b, dlon_b) = group_displacement(&mut uf, b);
            let dot = dlat_a * dlat_b + dlon_a * dlon_b;
            let mag_a = (dlat_a * dlat_a + dlon_a * dlon_a).sqrt();
            let mag_b = (dlat_b * dlat_b + dlon_b * dlon_b).sqrt();
            if mag_a < 0.1 || mag_b < 0.1 || dot < 0.0 {
                continue;
            }
            if uf.union(a, b) {
                tracing::debug!(
                    "  {}: shared-node merge comp {} ({} edges, mag {:.4}) + comp {} ({} edges, mag {:.4}), dot={:.4}",
                    highway, unique[0],
                    edges_by_comp.get(&unique[0]).map(|v| v.len()).unwrap_or(0),
                    mag_a, unique[i],
                    edges_by_comp.get(&unique[i]).map(|v| v.len()).unwrap_or(0),
                    mag_b, dot,
                );
                shared_merges += 1;
            }
        }
    }
    if shared_merges > 0 {
        tracing::debug!("  {}: {} shared-node merges total", highway, shared_merges);
    }

    // 3. Motorway-link bridge merge: components connected through motorway_link
    //    ways (only if same travel direction). Bridge pairs use original
    //    (pre-split) component IDs, so we translate them to post-split
    //    sub-component IDs via orig_to_split.
    let mut bridge_merges = 0usize;
    for &(orig_a, orig_b) in link_bridges {
        let subs_a = orig_to_split.get(&orig_a);
        let subs_b = orig_to_split.get(&orig_b);
        let (subs_a, subs_b) = match (subs_a, subs_b) {
            (Some(a), Some(b)) => (a, b),
            _ => continue,
        };
        for &sub_a in subs_a {
            for &sub_b in subs_b {
                if let (Some(&idx_a), Some(&idx_b)) =
                    (comp_to_idx.get(&sub_a), comp_to_idx.get(&sub_b))
                {
                    let (dlat_a, dlon_a) =
                        comp_displacement.get(&sub_a).copied().unwrap_or((0.0, 0.0));
                    let (dlat_b, dlon_b) =
                        comp_displacement.get(&sub_b).copied().unwrap_or((0.0, 0.0));
                    let dot = dlat_a * dlat_b + dlon_a * dlon_b;
                    let mag_a = (dlat_a * dlat_a + dlon_a * dlon_a).sqrt();
                    let mag_b = (dlat_b * dlat_b + dlon_b * dlon_b).sqrt();
                    // For bridge merges, require both components to have
                    // reliable direction vectors AND face the same direction.
                    // Cosine similarity > 0.5 (~60°) prevents weak fragments
                    // from bridging opposing carriageways (a near-zero vector
                    // has positive but tiny dot with ANY direction).
                    if mag_a < 0.01 || mag_b < 0.01 || dot < 0.0 {
                        continue;
                    }
                    let cosine = dot / (mag_a * mag_b);
                    if cosine < 0.5 {
                        continue;
                    }
                    if uf.union(idx_a, idx_b) {
                        tracing::debug!(
                            "  {}: bridge merge comp {} ({} edges, mag {:.4}) + comp {} ({} edges, mag {:.4}), dot={:.4}, bridge=({},{})",
                            highway, sub_a,
                            edges_by_comp.get(&sub_a).map(|v| v.len()).unwrap_or(0),
                            mag_a, sub_b,
                            edges_by_comp.get(&sub_b).map(|v| v.len()).unwrap_or(0),
                            mag_b, dot, orig_a, orig_b,
                        );
                        bridge_merges += 1;
                    }
                }
            }
        }
    }
    if bridge_merges > 0 {
        tracing::debug!(
            "  {}: {} motorway-link bridge merges total",
            highway,
            bridge_merges
        );
    }

    // 4. Proximity merge: find terminals, merge close ones with compatible bearing.
    //    10km threshold covers interchange areas, toll plazas, and short gaps
    //    where adjacent carriageway segments don't share graph nodes.
    //    Bearing check (60°) is applied for terminals >1km apart; for very
    //    close terminals (<1km), use displacement dot product instead (handles
    //    curved highways where overall bearing differs between segments).
    let proximity_merges = merge_components_by_proximity(
        &edges_by_comp,
        &component_set,
        &comp_to_idx,
        &node_coords,
        &mut uf,
        10_000.0,
        60.0,
    );
    if proximity_merges > 0 {
        tracing::debug!("  {}: {} proximity merges", highway, proximity_merges);
    }

    // 4b. Geographic overlap merge: merge same-direction components whose
    //     bounding boxes overlap along the highway axis. Two components on the
    //     same highway in the same direction that overlap geographically are the
    //     same carriageway — they just lack shared graph nodes.
    let mut overlap_merges = 0usize;
    {
        // Compute per-component bounding box and direction
        struct CompBBox {
            min_lat: f64,
            max_lat: f64,
            min_lon: f64,
            max_lon: f64,
        }
        let mut comp_bbox: HashMap<i32, CompBBox> = HashMap::new();
        for (&comp, comp_edges) in &edges_by_comp {
            let mut bb = CompBBox {
                min_lat: f64::INFINITY,
                max_lat: f64::NEG_INFINITY,
                min_lon: f64::INFINITY,
                max_lon: f64::NEG_INFINITY,
            };
            for e in comp_edges {
                bb.min_lat = bb.min_lat.min(e.start_lat).min(e.end_lat);
                bb.max_lat = bb.max_lat.max(e.start_lat).max(e.end_lat);
                bb.min_lon = bb.min_lon.min(e.start_lon).min(e.end_lon);
                bb.max_lon = bb.max_lon.max(e.start_lon).max(e.end_lon);
            }
            comp_bbox.insert(comp, bb);
        }

        for i in 0..component_set.len() {
            for j in (i + 1)..component_set.len() {
                let ci = component_set[i];
                let cj = component_set[j];
                let idx_i = comp_to_idx[&ci];
                let idx_j = comp_to_idx[&cj];
                if uf.find(idx_i) == uf.find(idx_j) {
                    continue;
                }
                // Direction compatibility (same as shared-node merge)
                let (dlat_a, dlon_a) = comp_displacement.get(&ci).copied().unwrap_or((0.0, 0.0));
                let (dlat_b, dlon_b) = comp_displacement.get(&cj).copied().unwrap_or((0.0, 0.0));
                let dot = dlat_a * dlat_b + dlon_a * dlon_b;
                let mag_a = (dlat_a * dlat_a + dlon_a * dlon_a).sqrt();
                let mag_b = (dlat_b * dlat_b + dlon_b * dlon_b).sqrt();
                // Both components need strong displacement, same direction, and
                // substantial size. The edge count check (≥50) prevents small
                // interchange fragments from bridging EB/WB carriageways.
                let count_a = edges_by_comp.get(&ci).map(|v| v.len()).unwrap_or(0);
                let count_b = edges_by_comp.get(&cj).map(|v| v.len()).unwrap_or(0);
                if mag_a < 1.0 || mag_b < 1.0 || dot < 0.0 || count_a < 50 || count_b < 50 {
                    continue;
                }
                // Bounding box overlap check
                let (Some(bb_a), Some(bb_b)) = (comp_bbox.get(&ci), comp_bbox.get(&cj)) else {
                    continue;
                };
                let lat_overlap = bb_a.min_lat <= bb_b.max_lat && bb_b.min_lat <= bb_a.max_lat;
                let lon_overlap = bb_a.min_lon <= bb_b.max_lon && bb_b.min_lon <= bb_a.max_lon;
                if lat_overlap && lon_overlap {
                    if uf.union(idx_i, idx_j) {
                        tracing::debug!(
                            "  {}: bbox-overlap merge comp {} ({} edges) + comp {} ({} edges)",
                            highway,
                            ci,
                            edges_by_comp.get(&ci).map(|v| v.len()).unwrap_or(0),
                            cj,
                            edges_by_comp.get(&cj).map(|v| v.len()).unwrap_or(0),
                        );
                        overlap_merges += 1;
                    }
                }
            }
        }
    }
    if overlap_merges > 0 {
        tracing::debug!("  {}: {} bbox-overlap merges", highway, overlap_merges);
    }

    // 5. Group by merged corridor
    let mut corridor_groups: BTreeMap<usize, Vec<i32>> = BTreeMap::new();
    for (i, &comp) in component_set.iter().enumerate() {
        let root = uf.find(i);
        corridor_groups.entry(root).or_default().push(comp);
    }

    // Sort groups by minimum component ID for deterministic ordering
    let mut sorted_groups: Vec<Vec<i32>> = corridor_groups.into_values().collect();
    sorted_groups.sort_by_key(|g| g.iter().copied().min().unwrap_or(0));

    // Exit lookup by graph_node
    let mut exit_by_node: HashMap<i64, Vec<&ExitData>> = HashMap::new();
    for exit in exits {
        exit_by_node.entry(exit.graph_node).or_default().push(exit);
    }

    // 6. Build each corridor
    let mut results = Vec::new();
    for group in sorted_groups {
        let group_edges: Vec<&EdgeData> = group
            .iter()
            .filter_map(|c| edges_by_comp.get(c))
            .flat_map(|v| v.iter().copied())
            .collect();

        if group_edges.is_empty() {
            continue;
        }

        // Count exits whose graph_node is in this corridor's edges
        let group_nodes: HashSet<i64> = group_edges
            .iter()
            .flat_map(|e| [e.start_node, e.end_node])
            .collect();
        let exits_in_nodes: usize = exit_by_node
            .iter()
            .filter(|(node, _)| group_nodes.contains(node))
            .map(|(_, v)| v.len())
            .sum();

        let ordered_exits = walk_corridor_exits(&group_edges, &exit_by_node, &node_coords);
        let direction = compute_corridor_direction(highway, &group_edges);

        if ordered_exits.len() != exits_in_nodes {
            tracing::warn!(
                "  {}: corridor {} walk found {} exits but {} exit nodes in edges (missed {})",
                highway,
                *next_id,
                ordered_exits.len(),
                exits_in_nodes,
                exits_in_nodes - ordered_exits.len(),
            );
        }

        let corridor_id = *next_id;
        *next_id += 1;

        let member_edge_ids: Vec<String> = group_edges.iter().map(|e| e.id.clone()).collect();
        let mut dlat = 0.0_f64;
        let mut dlon = 0.0_f64;
        for e in &group_edges {
            dlat += e.end_lat - e.start_lat;
            dlon += e.end_lon - e.start_lon;
        }
        let sample_points = corridor_sample_points(&ordered_exits, &group_edges);

        results.push(CorridorResult {
            corridor_id,
            highway: highway.to_string(),
            canonical_direction: direction,
            exits: ordered_exits,
            member_edge_ids,
            displacement: (dlat, dlon),
            sample_points,
        });
    }

    absorb_minor_corridors(highway, results)
}

// ============================================================================
// Proximity merging
// ============================================================================

fn absorb_minor_corridors(highway: &str, results: Vec<CorridorResult>) -> Vec<CorridorResult> {
    // Keep corridor ids sparse after absorption. Reusing ids here can make a
    // later highway overwrite an earlier corridor row and mix their edge sets.
    if results.len() <= 2 {
        return results;
    }

    let mut max_edges_by_dir: HashMap<Option<String>, usize> = HashMap::new();
    for corridor in &results {
        let entry = max_edges_by_dir
            .entry(corridor.canonical_direction.clone())
            .or_default();
        *entry = (*entry).max(corridor.member_edge_ids.len());
    }

    let mut keep = Vec::new();
    let mut orphans = Vec::new();
    let mut absorbed_count = 0usize;

    for corridor in results {
        let primary_edges = max_edges_by_dir
            .get(&corridor.canonical_direction)
            .copied()
            .unwrap_or(0);
        // A corridor is minor if it has <10% of the edges of the largest
        // same-direction corridor, OR if it has very few exits relative to
        // its edge count.
        let is_edge_minor = corridor.member_edge_ids.len() * 10 < primary_edges;
        let is_exit_minor =
            corridor.exits.len() <= 5 && primary_edges > corridor.member_edge_ids.len();
        let is_minor = is_edge_minor || is_exit_minor;
        if is_minor {
            absorbed_count += 1;
            orphans.push(corridor);
        } else {
            keep.push(corridor);
        }
    }

    if absorbed_count == 0 {
        return keep;
    }

    tracing::info!(
        "  {}: absorbing {} minor corridors into same-direction primaries",
        highway,
        absorbed_count
    );

    for orphan in orphans {
        let target = nearest_corridor_target(&orphan, &keep, MINOR_CORRIDOR_ABSORPTION_MAX_GAP_M);

        if let Some(target_idx) = target {
            merge_corridors(&mut keep[target_idx], orphan);
        } else {
            keep.push(orphan);
        }
    }

    keep.sort_by_key(|corridor| corridor.corridor_id);
    keep
}

fn corridor_sample_points(
    exits: &[CorridorExitResult],
    group_edges: &[&EdgeData],
) -> Vec<(f64, f64)> {
    if !exits.is_empty() {
        return exits.iter().map(|exit| (exit.lat, exit.lon)).collect();
    }

    let mut points = Vec::with_capacity(group_edges.len() * 2);
    for edge in group_edges {
        points.push((edge.start_lat, edge.start_lon));
        points.push((edge.end_lat, edge.end_lon));
    }
    points
}

fn nearest_corridor_target(
    orphan: &CorridorResult,
    keep: &[CorridorResult],
    max_gap_m: f64,
) -> Option<usize> {
    keep.iter()
        .enumerate()
        .filter(|(_, candidate)| candidate.canonical_direction == orphan.canonical_direction)
        .filter_map(|(idx, candidate)| {
            let gap_m = corridor_gap_m(orphan, candidate)?;
            (gap_m <= max_gap_m).then_some((idx, gap_m, candidate.member_edge_ids.len()))
        })
        .min_by(|a, b| {
            a.1.partial_cmp(&b.1)
                .unwrap_or(std::cmp::Ordering::Equal)
                .then_with(|| b.2.cmp(&a.2))
        })
        .map(|(idx, _, _)| idx)
        .or_else(|| {
            keep.iter()
                .enumerate()
                .filter_map(|(idx, candidate)| {
                    let gap_m = corridor_gap_m(orphan, candidate)?;
                    (gap_m <= max_gap_m).then_some((idx, gap_m, candidate.member_edge_ids.len()))
                })
                .min_by(|a, b| {
                    a.1.partial_cmp(&b.1)
                        .unwrap_or(std::cmp::Ordering::Equal)
                        .then_with(|| b.2.cmp(&a.2))
                })
                .map(|(idx, _, _)| idx)
        })
}

fn corridor_gap_m(a: &CorridorResult, b: &CorridorResult) -> Option<f64> {
    let mut best: Option<f64> = None;

    for &(alat, alon) in &a.sample_points {
        for &(blat, blon) in &b.sample_points {
            let gap_m = haversine_distance(alat, alon, blat, blon);
            best = Some(match best {
                Some(current) => current.min(gap_m),
                None => gap_m,
            });
        }
    }

    best
}

fn merge_corridors(target: &mut CorridorResult, source: CorridorResult) {
    target.member_edge_ids.extend(source.member_edge_ids);
    target.sample_points.extend(source.sample_points);

    // Binary-insert each absorbed exit into the already-sorted list at the
    // correct projection position.
    let (dlat, dlon) = target.displacement;
    let has_disp = dlat.abs() > 1e-6 || dlon.abs() > 1e-6;
    for exit in source.exits {
        let proj = if has_disp {
            exit.lat * dlat + exit.lon * dlon
        } else {
            exit.lat
        };
        let pos = target.exits.partition_point(|existing| {
            let existing_proj = if has_disp {
                existing.lat * dlat + existing.lon * dlon
            } else {
                existing.lat
            };
            existing_proj < proj
        });
        target.exits.insert(pos, exit);
    }
}

fn merge_components_by_proximity(
    edges_by_comp: &HashMap<i32, Vec<&EdgeData>>,
    component_set: &[i32],
    comp_to_idx: &HashMap<i32, usize>,
    node_coords: &HashMap<i64, (f64, f64)>,
    uf: &mut UnionFind,
    threshold_m: f64,
    max_bearing_diff_deg: f64,
) -> usize {
    // Compute displacement vectors and terminal nodes per component
    struct CompInfo {
        displacement: Option<(f64, f64)>, // (dlat, dlon)
        displacement_bearing: Option<f64>,
        terminals: Vec<(f64, f64)>, // (lat, lon) of source/sink nodes
    }

    let mut comp_info: HashMap<i32, CompInfo> = HashMap::new();

    for &comp in component_set {
        let Some(comp_edges) = edges_by_comp.get(&comp) else {
            continue;
        };

        // Displacement vector
        let mut delta_lat = 0.0_f64;
        let mut delta_lon = 0.0_f64;
        for e in comp_edges {
            delta_lat += e.end_lat - e.start_lat;
            delta_lon += e.end_lon - e.start_lon;
        }
        let (displacement, displacement_bearing) =
            if delta_lat.abs() > 1e-6 || delta_lon.abs() > 1e-6 {
                let bearing = delta_lon.atan2(delta_lat).to_degrees().rem_euclid(360.0);
                (Some((delta_lat, delta_lon)), Some(bearing))
            } else {
                (None, None)
            };

        // Terminal nodes: in-degree=0 (sources) or out-degree=0 (sinks)
        let mut in_deg: HashMap<i64, usize> = HashMap::new();
        let mut out_deg: HashMap<i64, usize> = HashMap::new();
        let mut all_nodes: HashSet<i64> = HashSet::new();
        for e in comp_edges {
            *out_deg.entry(e.start_node).or_default() += 1;
            *in_deg.entry(e.end_node).or_default() += 1;
            all_nodes.insert(e.start_node);
            all_nodes.insert(e.end_node);
        }

        let mut terminals = Vec::new();
        for &node in &all_nodes {
            let ind = in_deg.get(&node).copied().unwrap_or(0);
            let outd = out_deg.get(&node).copied().unwrap_or(0);
            if ind == 0 || outd == 0 {
                if let Some(&(lat, lon)) = node_coords.get(&node) {
                    terminals.push((lat, lon));
                }
            }
        }

        comp_info.insert(
            comp,
            CompInfo {
                displacement,
                displacement_bearing,
                terminals,
            },
        );
    }

    let mut merges = 0;

    // Check all pairs of components (that aren't already merged)
    for i in 0..component_set.len() {
        for j in (i + 1)..component_set.len() {
            let ci = component_set[i];
            let cj = component_set[j];

            let idx_i = comp_to_idx[&ci];
            let idx_j = comp_to_idx[&cj];

            // Already merged?
            if uf.find(idx_i) == uf.find(idx_j) {
                continue;
            }

            let Some(info_i) = comp_info.get(&ci) else {
                continue;
            };
            let Some(info_j) = comp_info.get(&cj) else {
                continue;
            };

            // Distance check: find closest terminal pair
            let mut min_dist = f64::INFINITY;
            for &(lat_i, lon_i) in &info_i.terminals {
                for &(lat_j, lon_j) in &info_j.terminals {
                    let dist = haversine_distance(lat_i, lon_i, lat_j, lon_j);
                    if dist < min_dist {
                        min_dist = dist;
                    }
                }
            }

            if min_dist > threshold_m {
                continue;
            }

            // Direction compatibility — two tiers:
            // Close terminals (< 1km): use displacement dot product (same as
            //   shared-node merge). Curved highways have large bearing diffs
            //   between segments, but dot product is positive for same-direction
            //   and negative for opposite-direction.
            // Distant terminals (1-15km): use stricter bearing angle check.
            if min_dist < 1_000.0 {
                let (dlat_a, dlon_a) = info_i.displacement.unwrap_or((0.0, 0.0));
                let (dlat_b, dlon_b) = info_j.displacement.unwrap_or((0.0, 0.0));
                let dot = dlat_a * dlat_b + dlon_a * dlon_b;
                let mag_a = (dlat_a * dlat_a + dlon_a * dlon_a).sqrt();
                let mag_b = (dlat_b * dlat_b + dlon_b * dlon_b).sqrt();
                if mag_a < 0.1 || mag_b < 0.1 || dot < 0.0 {
                    continue;
                }
            } else {
                let (dlat_a, dlon_a) = info_i.displacement.unwrap_or((0.0, 0.0));
                let (dlat_b, dlon_b) = info_j.displacement.unwrap_or((0.0, 0.0));
                let mag_a = (dlat_a * dlat_a + dlon_a * dlon_a).sqrt();
                let mag_b = (dlat_b * dlat_b + dlon_b * dlon_b).sqrt();
                // Both components need reliable displacement to trust bearing
                if mag_a < 0.1 || mag_b < 0.1 {
                    continue;
                }
                if let (Some(b_i), Some(b_j)) =
                    (info_i.displacement_bearing, info_j.displacement_bearing)
                {
                    let diff = (b_i - b_j).rem_euclid(360.0);
                    let angle_diff = diff.min(360.0 - diff);
                    if angle_diff > max_bearing_diff_deg {
                        continue;
                    }
                }
            }

            if uf.union(idx_i, idx_j) {
                let count_i = edges_by_comp.get(&ci).map(|v| v.len()).unwrap_or(0);
                let count_j = edges_by_comp.get(&cj).map(|v| v.len()).unwrap_or(0);
                let (dlat_i, dlon_i) = info_i.displacement.unwrap_or((0.0, 0.0));
                let (dlat_j, dlon_j) = info_j.displacement.unwrap_or((0.0, 0.0));
                let mag_i = (dlat_i * dlat_i + dlon_i * dlon_i).sqrt();
                let mag_j = (dlat_j * dlat_j + dlon_j * dlon_j).sqrt();
                tracing::debug!(
                    "  proximity merge comp {} ({} edges, mag {:.4}) + comp {} ({} edges, mag {:.4}), dist {:.0}m",
                    ci, count_i, mag_i, cj, count_j, mag_j, min_dist,
                );
                merges += 1;
            }
        }
    }

    merges
}

// ============================================================================
// Topological walk
// ============================================================================

fn walk_corridor_exits(
    edges: &[&EdgeData],
    exit_by_node: &HashMap<i64, Vec<&ExitData>>,
    _node_coords: &HashMap<i64, (f64, f64)>,
) -> Vec<CorridorExitResult> {
    if edges.is_empty() {
        return Vec::new();
    }

    // Collect all nodes in this corridor's edges
    let all_nodes: HashSet<i64> = edges
        .iter()
        .flat_map(|e| [e.start_node, e.end_node])
        .collect();

    // Collect all exits whose graph_node is in corridor nodes (deduplicated)
    let mut seen_exit_ids: HashSet<String> = HashSet::new();
    let mut exits: Vec<CorridorExitResult> = Vec::new();
    for (&node, node_exits) in exit_by_node {
        if !all_nodes.contains(&node) {
            continue;
        }
        for ex in node_exits {
            if seen_exit_ids.insert(ex.exit_id.clone()) {
                exits.push(CorridorExitResult {
                    exit_id: ex.exit_id.clone(),
                    ref_val: ex.ref_val.clone(),
                    name: ex.name.clone(),
                    lat: ex.lat,
                    lon: ex.lon,
                });
            }
        }
    }

    // Order by projection along the corridor's displacement vector
    let mut total_delta_lat = 0.0_f64;
    let mut total_delta_lon = 0.0_f64;
    for e in edges {
        total_delta_lat += e.end_lat - e.start_lat;
        total_delta_lon += e.end_lon - e.start_lon;
    }
    let has_displacement = total_delta_lat.abs() > 1e-6 || total_delta_lon.abs() > 1e-6;

    exits.sort_by(|a, b| {
        let proj_a = if has_displacement {
            a.lat * total_delta_lat + a.lon * total_delta_lon
        } else {
            a.lat
        };
        let proj_b = if has_displacement {
            b.lat * total_delta_lat + b.lon * total_delta_lon
        } else {
            b.lat
        };
        proj_a
            .partial_cmp(&proj_b)
            .unwrap_or(std::cmp::Ordering::Equal)
    });

    exits
}

// ============================================================================
// Direction computation
// ============================================================================

fn compute_corridor_direction(highway: &str, edges: &[&EdgeData]) -> Option<String> {
    if edges.is_empty() {
        return None;
    }

    // Sum displacement vectors weighted by length
    let mut delta_lat = 0.0_f64;
    let mut delta_lon = 0.0_f64;
    let mut _total_length = 0.0_f64;

    // Also count direction labels for near-zero displacement fallback
    let mut n_count = 0u32;
    let mut s_count = 0u32;
    let mut e_count = 0u32;
    let mut w_count = 0u32;

    for e in edges {
        delta_lat += e.end_lat - e.start_lat;
        delta_lon += e.end_lon - e.start_lon;
        _total_length += e.length_m as f64;

        match e.direction.as_deref() {
            Some("N" | "n" | "north") => n_count += 1,
            Some("S" | "s" | "south") => s_count += 1,
            Some("E" | "e" | "east") => e_count += 1,
            Some("W" | "w" | "west") => w_count += 1,
            _ => {}
        }
    }

    // Axis hint from interstate number parity or geographic span
    let axis_hint = compute_axis_hint(highway, edges);

    let has_displacement = delta_lat.abs().max(delta_lon.abs()) > 0.01;

    if has_displacement {
        match axis_hint.as_deref() {
            Some("ns") => {
                if delta_lat > 0.0 {
                    Some("north".to_string())
                } else {
                    Some("south".to_string())
                }
            }
            Some("ew") => {
                if delta_lon > 0.0 {
                    Some("east".to_string())
                } else {
                    Some("west".to_string())
                }
            }
            _ => {
                if delta_lat.abs() >= delta_lon.abs() {
                    if delta_lat > 0.0 {
                        Some("north".to_string())
                    } else {
                        Some("south".to_string())
                    }
                } else if delta_lon > 0.0 {
                    Some("east".to_string())
                } else {
                    Some("west".to_string())
                }
            }
        }
    } else {
        // Near-zero displacement: use edge direction label majority
        let max_count = n_count.max(s_count).max(e_count).max(w_count);
        if max_count == 0 {
            return None;
        }
        match axis_hint.as_deref() {
            Some("ns") => {
                if n_count >= s_count {
                    Some("north".to_string())
                } else {
                    Some("south".to_string())
                }
            }
            Some("ew") => {
                if e_count >= w_count {
                    Some("east".to_string())
                } else {
                    Some("west".to_string())
                }
            }
            _ => {
                if max_count == n_count {
                    Some("north".to_string())
                } else if max_count == s_count {
                    Some("south".to_string())
                } else if max_count == e_count {
                    Some("east".to_string())
                } else {
                    Some("west".to_string())
                }
            }
        }
    }
}

fn compute_axis_hint(highway: &str, edges: &[&EdgeData]) -> Option<String> {
    // Interstate parity rule: even = EW, odd = NS (for < 100)
    let upper = highway.to_ascii_uppercase();
    if let Some(num_str) = upper
        .strip_prefix("I-")
        .and_then(|rest| rest.split(|c: char| !c.is_ascii_digit()).next())
    {
        if let Ok(num) = num_str.parse::<u32>() {
            if num < 100 {
                return if num % 2 == 0 {
                    Some("ew".to_string())
                } else {
                    Some("ns".to_string())
                };
            }
        }
    }

    // Geographic span ratio
    let mut min_lat = f64::INFINITY;
    let mut max_lat = f64::NEG_INFINITY;
    let mut min_lon = f64::INFINITY;
    let mut max_lon = f64::NEG_INFINITY;
    for e in edges {
        min_lat = min_lat.min(e.start_lat).min(e.end_lat);
        max_lat = max_lat.max(e.start_lat).max(e.end_lat);
        min_lon = min_lon.min(e.start_lon).min(e.end_lon);
        max_lon = max_lon.max(e.start_lon).max(e.end_lon);
    }

    let mid_lat = (min_lat + max_lat) / 2.0;
    let lat_span_m = haversine_distance(
        min_lat,
        (min_lon + max_lon) / 2.0,
        max_lat,
        (min_lon + max_lon) / 2.0,
    );
    let lon_span_m = haversine_distance(mid_lat, min_lon, mid_lat, max_lon);

    if lat_span_m > lon_span_m * 1.5 {
        Some("ns".to_string())
    } else if lon_span_m > lat_span_m * 1.5 {
        Some("ew".to_string())
    } else {
        None
    }
}

// ============================================================================
// DB writing
// ============================================================================

async fn write_corridors(
    pool: &PgPool,
    corridors: &[CorridorResult],
) -> Result<BuildCorridorsStats, anyhow::Error> {
    let mut corridors_created = 0usize;
    let mut corridor_exits_created = 0usize;
    #[allow(unused_assignments)]
    let mut edges_updated = 0usize;

    // Phase 1: Insert corridors (small — ~9K rows)
    tracing::info!("  Writing {} corridor rows...", corridors.len());
    {
        let mut tx = pool.begin().await?;
        for corridor in corridors {
            sqlx::query(
                "INSERT INTO corridors (corridor_id, highway, canonical_direction) \
                 VALUES ($1, $2, $3) \
                 ON CONFLICT (corridor_id) DO UPDATE SET \
                   highway = EXCLUDED.highway, \
                   canonical_direction = EXCLUDED.canonical_direction",
            )
            .bind(corridor.corridor_id)
            .bind(&corridor.highway)
            .bind(&corridor.canonical_direction)
            .execute(&mut *tx)
            .await?;
            corridors_created += 1;
        }
        tx.commit().await?;
    }
    tracing::info!("  Committed {} corridors", corridors_created);

    // Phase 2: Insert corridor exits (~77K rows)
    tracing::info!("  Writing corridor exits...");
    {
        let mut tx = pool.begin().await?;
        for corridor in corridors {
            for (idx, exit) in corridor.exits.iter().enumerate() {
                sqlx::query(
                    "INSERT INTO corridor_exits \
                     (corridor_id, corridor_index, exit_id, ref, name, lat, lon) \
                     VALUES ($1, $2, $3, $4, $5, $6, $7) \
                     ON CONFLICT (corridor_id, corridor_index) DO UPDATE SET \
                       exit_id = EXCLUDED.exit_id, \
                       ref = EXCLUDED.ref, \
                       name = EXCLUDED.name, \
                       lat = EXCLUDED.lat, \
                       lon = EXCLUDED.lon",
                )
                .bind(corridor.corridor_id)
                .bind(idx as i32)
                .bind(&exit.exit_id)
                .bind(&exit.ref_val)
                .bind(&exit.name)
                .bind(exit.lat)
                .bind(exit.lon)
                .execute(&mut *tx)
                .await?;
                corridor_exits_created += 1;
            }
        }
        tx.commit().await?;
    }
    tracing::info!("  Committed {} corridor exits", corridor_exits_created);

    // Phase 3: Update highway_edges.corridor_id via staging table + bulk UPDATE
    tracing::info!("  Updating highway_edges corridor_id via bulk update...");
    sqlx::query("DROP TABLE IF EXISTS _corridor_edge_map")
        .execute(pool)
        .await?;
    sqlx::query(
        "CREATE UNLOGGED TABLE _corridor_edge_map (edge_id TEXT NOT NULL, corridor_id INTEGER NOT NULL, direction TEXT)",
    )
    .execute(pool)
    .await?;

    // Insert mappings in batches
    let batch_size = 10_000;
    let mut all_mappings: Vec<(&str, i32, Option<&str>)> = Vec::new();
    for corridor in corridors {
        let dir = corridor.canonical_direction.as_deref();
        for edge_id in &corridor.member_edge_ids {
            all_mappings.push((edge_id, corridor.corridor_id, dir));
        }
    }
    tracing::info!(
        "  Inserting {} edge mappings into temp table...",
        all_mappings.len()
    );
    for (batch_idx, chunk) in all_mappings.chunks(batch_size).enumerate() {
        let mut tx = pool.begin().await?;
        for &(edge_id, cid, dir) in chunk {
            sqlx::query(
                "INSERT INTO _corridor_edge_map (edge_id, corridor_id, direction) VALUES ($1, $2, $3)",
            )
            .bind(edge_id)
            .bind(cid)
            .bind(dir)
            .execute(&mut *tx)
            .await?;
        }
        tx.commit().await?;
        let done = ((batch_idx + 1) * batch_size).min(all_mappings.len());
        if done % 100_000 < batch_size || done == all_mappings.len() {
            tracing::info!("  temp table: {}/{}", done, all_mappings.len());
        }
    }

    // Index the staging table for fast join
    tracing::info!("  Creating index on staging table...");
    sqlx::query("CREATE INDEX ON _corridor_edge_map (edge_id)")
        .execute(pool)
        .await?;

    // Bulk UPDATE from staging table
    tracing::info!("  Running bulk UPDATE...");
    let result = sqlx::query(
        "UPDATE highway_edges he SET corridor_id = m.corridor_id, direction = m.direction \
         FROM _corridor_edge_map m WHERE he.id = m.edge_id",
    )
    .execute(pool)
    .await?;
    edges_updated = result.rows_affected() as usize;
    tracing::info!("  Bulk update complete: {} edges", edges_updated);

    sqlx::query("DROP TABLE IF EXISTS _corridor_edge_map")
        .execute(pool)
        .await?;

    Ok(BuildCorridorsStats {
        corridors_created,
        corridor_exits_created,
        edges_updated,
    })
}

#[cfg(test)]
mod tests {
    use super::{absorb_minor_corridors, CorridorExitResult, CorridorResult};

    fn corridor(
        corridor_id: i32,
        direction: &str,
        edge_count: usize,
        exit_count: usize,
        sample_points: &[(f64, f64)],
    ) -> CorridorResult {
        CorridorResult {
            corridor_id,
            highway: "I-TEST".to_string(),
            canonical_direction: Some(direction.to_string()),
            exits: (0..exit_count)
                .map(|idx| CorridorExitResult {
                    exit_id: format!("exit-{corridor_id}-{idx}"),
                    ref_val: None,
                    name: None,
                    lat: idx as f64,
                    lon: idx as f64,
                })
                .collect(),
            member_edge_ids: (0..edge_count)
                .map(|idx| format!("edge-{corridor_id}-{idx}"))
                .collect(),
            displacement: (1.0, 0.0),
            sample_points: sample_points.to_vec(),
        }
    }

    #[test]
    fn absorbed_corridors_do_not_force_dense_id_reuse() {
        let kept = absorb_minor_corridors(
            "I-TEST",
            vec![
                corridor(10, "north", 100, 20, &[(45.0, -122.0)]),
                corridor(11, "north", 2, 1, &[(45.001, -122.001)]),
                corridor(12, "north", 90, 18, &[(41.0, -75.0)]),
            ],
        );

        let ids: Vec<i32> = kept.iter().map(|corridor| corridor.corridor_id).collect();
        assert_eq!(ids, vec![10, 12]);
        assert!(kept.iter().any(|corridor| {
            corridor.corridor_id == 10
                && corridor
                    .member_edge_ids
                    .iter()
                    .any(|edge_id| edge_id == "edge-11-0")
        }));
    }

    #[test]
    fn absorbed_corridor_chooses_nearest_same_direction_target() {
        let kept = absorb_minor_corridors(
            "I-TEST",
            vec![
                corridor(10, "north", 100, 20, &[(45.0, -122.0)]),
                corridor(11, "north", 2, 1, &[(41.424, -75.611)]),
                corridor(12, "north", 90, 18, &[(41.423, -75.610)]),
            ],
        );

        assert!(kept.iter().any(|corridor| {
            corridor.corridor_id == 12
                && corridor
                    .member_edge_ids
                    .iter()
                    .any(|edge_id| edge_id == "edge-11-0")
        }));
        assert!(!kept.iter().any(|corridor| {
            corridor.corridor_id == 10
                && corridor
                    .member_edge_ids
                    .iter()
                    .any(|edge_id| edge_id == "edge-11-0")
        }));
    }

    #[test]
    fn distant_minor_corridor_is_left_separate() {
        let kept = absorb_minor_corridors(
            "I-TEST",
            vec![
                corridor(10, "north", 100, 20, &[(45.0, -122.0)]),
                corridor(11, "north", 2, 1, &[(30.0, -90.0)]),
                corridor(12, "north", 90, 18, &[(41.0, -75.0)]),
            ],
        );

        let ids: Vec<i32> = kept.iter().map(|corridor| corridor.corridor_id).collect();
        assert_eq!(ids, vec![10, 11, 12]);
    }
}
