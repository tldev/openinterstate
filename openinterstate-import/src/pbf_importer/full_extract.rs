use std::collections::{HashMap, HashSet};
use std::fs::File;
use std::path::{Path, PathBuf};

use anyhow::Context;
use osmpbfreader::{OsmObj, OsmPbfReader};
use openinterstate_core::highway_ref::normalize_highway_ref;

use crate::nsi::NsiBrandMatcher;
use crate::parser::{ParsedData, ParsedExit, ParsedHighway, ParsedPOI};

use super::helpers::{
    build_exit_anchor_links, build_exit_grid, build_node_to_refs_index, build_way_poi,
    categorize_poi, pick_primary_highway_from_tags, tags_to_map, way_near_any_exit,
};

const ACCESS_ROAD_EXIT_BUFFER_M: f64 = 3_000.0;

#[derive(Debug)]
pub(super) struct WaySeed {
    pub(super) id: i64,
    pub(super) tags: HashMap<String, String>,
    pub(super) nodes: Vec<i64>,
    pub(super) highway_type: Option<String>,
    pub(super) is_poi_candidate: bool,
}

struct WayScan {
    ways: Vec<WaySeed>,
    needed_nodes: HashSet<i64>,
    way_geom_node_ids: HashSet<i64>,
}

struct NodeScan {
    node_coords: HashMap<i64, (f64, f64)>,
    exits: Vec<ParsedExit>,
    node_pois: Vec<ParsedPOI>,
    exit_node_ids: HashSet<i64>,
}

#[derive(Default)]
struct AccessFilterStats {
    total: usize,
    kept: usize,
}

pub(crate) fn parse_pbf_extract(
    path: &Path,
    nsi: Option<&NsiBrandMatcher>,
) -> Result<ParsedData, anyhow::Error> {
    let way_scan = scan_way_seeds(path)?;
    let node_to_refs = build_node_to_refs_index(&way_scan.ways);
    let node_scan =
        scan_nodes_and_build_entities(path, &way_scan.needed_nodes, &node_to_refs, nsi)?;

    let (mut highways, mut way_pois, access_stats) = build_highways_and_way_pois(
        &way_scan.ways,
        &node_scan.node_coords,
        &node_scan.exits,
        nsi,
    );

    if access_stats.total > 0 {
        tracing::info!(
            "PBF access-way buffer filter: kept {}/{} ways within {:.1}km of exits",
            access_stats.kept,
            access_stats.total,
            ACCESS_ROAD_EXIT_BUFFER_M / 1_000.0
        );
    }

    if !node_scan.exit_node_ids.is_empty() {
        let extra = build_exit_anchor_links(
            &node_scan.exit_node_ids,
            &way_scan.way_geom_node_ids,
            &node_scan.node_coords,
        );
        highways.extend(extra);
    }

    let mut pois = node_scan.node_pois;
    pois.append(&mut way_pois);

    Ok(ParsedData {
        exits: node_scan.exits,
        pois,
        highways,
    })
}

pub(crate) fn list_pbf_files(dir: &str) -> Result<Vec<PathBuf>, anyhow::Error> {
    let mut files: Vec<PathBuf> = std::fs::read_dir(dir)?
        .filter_map(Result::ok)
        .map(|entry| entry.path())
        .filter(|path| path.extension().and_then(|ext| ext.to_str()) == Some("pbf"))
        .collect();
    files.sort();
    Ok(files)
}

fn scan_way_seeds(path: &Path) -> Result<WayScan, anyhow::Error> {
    let file = File::open(path).with_context(|| format!("open {}", path.display()))?;
    let mut pbf = OsmPbfReader::new(file);

    let mut ways = Vec::new();
    let mut needed_nodes: HashSet<i64> = HashSet::new();
    let mut way_geom_node_ids: HashSet<i64> = HashSet::new();

    for obj in pbf.iter() {
        let obj = obj.with_context(|| format!("reading {}", path.display()))?;
        if let OsmObj::Way(way) = obj {
            let tags = tags_to_map(&way.tags);
            let highway_type = tags.get("highway").cloned();
            let is_highway = matches!(
                highway_type.as_deref(),
                Some("motorway")
                    | Some("trunk")
                    | Some("motorway_link")
                    | Some("service")
                    | Some("primary")
                    | Some("primary_link")
                    | Some("secondary")
                    | Some("secondary_link")
                    | Some("tertiary")
                    | Some("tertiary_link")
                    | Some("residential")
                    | Some("unclassified")
            );
            let is_poi_candidate = categorize_poi(&tags).is_some();

            if is_highway || is_poi_candidate {
                let nodes: Vec<i64> = way.nodes.iter().map(|node| node.0).collect();
                for node_id in &nodes {
                    needed_nodes.insert(*node_id);
                    if is_highway {
                        way_geom_node_ids.insert(*node_id);
                    }
                }

                ways.push(WaySeed {
                    id: way.id.0,
                    tags,
                    nodes,
                    highway_type,
                    is_poi_candidate,
                });
            }
        }
    }

    Ok(WayScan {
        ways,
        needed_nodes,
        way_geom_node_ids,
    })
}

fn scan_nodes_and_build_entities(
    path: &Path,
    needed_nodes: &HashSet<i64>,
    node_to_refs: &HashMap<i64, Vec<String>>,
    nsi: Option<&NsiBrandMatcher>,
) -> Result<NodeScan, anyhow::Error> {
    let file = File::open(path).with_context(|| format!("reopen {}", path.display()))?;
    let mut pbf = OsmPbfReader::new(file);

    let mut node_coords: HashMap<i64, (f64, f64)> = HashMap::new();
    let mut exits = Vec::new();
    let mut node_pois = Vec::new();
    let mut exit_node_ids: HashSet<i64> = HashSet::new();

    for obj in pbf.iter() {
        let obj = obj.with_context(|| format!("second pass {}", path.display()))?;
        if let OsmObj::Node(node) = obj {
            let node_id = node.id.0;
            let tags = tags_to_map(&node.tags);
            let lat = node.lat();
            let lon = node.lon();

            if needed_nodes.contains(&node_id) {
                node_coords.insert(node_id, (lat, lon));
            }

            if tags.get("highway").map(String::as_str) == Some("motorway_junction") {
                let highway = pick_primary_highway_from_tags(&tags, node_to_refs.get(&node_id));
                exits.push(ParsedExit {
                    id: format!("node/{node_id}"),
                    osm_type: "node".to_string(),
                    osm_id: node_id,
                    lat,
                    lon,
                    state: None,
                    r#ref: tags
                        .get("ref")
                        .or_else(|| tags.get("junction:ref"))
                        .cloned(),
                    name: tags
                        .get("name")
                        .or_else(|| tags.get("exit:name"))
                        .or_else(|| tags.get("destination"))
                        .or_else(|| tags.get("destination:ref"))
                        .cloned(),
                    highway,
                    direction: tags.get("direction").cloned(),
                    tags_json: serde_json::to_string(&tags).ok(),
                });
                exit_node_ids.insert(node_id);
            }

            if let Some(category) = categorize_poi(&tags) {
                let raw_name = tags
                    .get("brand")
                    .or_else(|| tags.get("name"))
                    .or_else(|| tags.get("operator"))
                    .cloned()
                    .unwrap_or_else(|| "Unknown".to_string());
                let canonical = nsi
                    .and_then(|matcher| matcher.canonicalize(&raw_name, category))
                    .unwrap_or_else(|| raw_name.clone());
                let display_name =
                    openinterstate_core::brand_helpers::normalize_brand(&canonical).to_string();

                node_pois.push(ParsedPOI {
                    id: format!("node/{node_id}"),
                    osm_type: "node".to_string(),
                    osm_id: node_id,
                    lat,
                    lon,
                    state: None,
                    category: Some(category.to_string()),
                    name: Some(raw_name),
                    display_name: Some(display_name),
                    brand: tags.get("brand").cloned(),
                    tags_json: serde_json::to_string(&tags).ok(),
                });
            }
        }
    }

    Ok(NodeScan {
        node_coords,
        exits,
        node_pois,
        exit_node_ids,
    })
}

fn build_highways_and_way_pois(
    ways: &[WaySeed],
    node_coords: &HashMap<i64, (f64, f64)>,
    exits: &[ParsedExit],
    nsi: Option<&NsiBrandMatcher>,
) -> (Vec<ParsedHighway>, Vec<ParsedPOI>, AccessFilterStats) {
    let mut highways = Vec::new();
    let mut way_pois = Vec::new();
    let mut access_stats = AccessFilterStats::default();
    let exit_grid = build_exit_grid(exits);

    for way in ways {
        let coords: Vec<(f64, f64)> = way
            .nodes
            .iter()
            .filter_map(|node_id| node_coords.get(node_id).copied())
            .collect();
        if coords.len() < 2 || coords.len() != way.nodes.len() {
            continue;
        }

        if way.is_poi_candidate {
            if let Some(poi) = build_way_poi(way.id, &way.tags, &coords, nsi) {
                way_pois.push(poi);
            }
        }

        let Some(highway_type) = way.highway_type.as_deref() else {
            continue;
        };

        let refs_raw = way.tags.get("ref").cloned().unwrap_or_default();
        let refs = if highway_type == "motorway" || highway_type == "trunk" {
            refs_raw
                .split(';')
                .filter_map(|raw_ref| normalize_highway_ref(raw_ref.trim()))
                .collect::<Vec<_>>()
        } else {
            Vec::new()
        };

        // Refless motorway ways are kept — they connect ref'd segments via shared
        // OSM nodes (e.g. "Sam Cooper Boulevard" in Memphis is physically I-40 but
        // has no ref tag). The graph builder assigns them to highways by node overlap.

        let oneway_tag = way.tags.get("oneway").map_or("", String::as_str);
        let mut nodes = way.nodes.clone();
        let mut geometry = coords;
        let is_oneway = match oneway_tag {
            "no" => false,
            "-1" | "yes" | "1" => true,
            _ => highway_type == "motorway" || highway_type == "motorway_link",
        };
        if oneway_tag == "-1" {
            nodes.reverse();
            geometry.reverse();
        }

        let is_access_class = matches!(
            highway_type,
            "motorway_link"
                | "service"
                | "primary"
                | "primary_link"
                | "secondary"
                | "secondary_link"
                | "tertiary"
                | "tertiary_link"
                | "residential"
                | "unclassified"
        );
        let should_keep_for_access = if is_access_class {
            access_stats.total += 1;
            let keep = way_near_any_exit(&geometry, &exit_grid, ACCESS_ROAD_EXIT_BUFFER_M);
            if keep {
                access_stats.kept += 1;
            }
            keep
        } else {
            true
        };

        if should_keep_for_access {
            highways.push(ParsedHighway {
                id: format!("way/{}", way.id),
                refs,
                nodes,
                geometry,
                highway_type: highway_type.to_string(),
                is_oneway,
            });
        }
    }

    (highways, way_pois, access_stats)
}
