use std::collections::{HashMap, HashSet};
use std::fs::File;
use std::path::Path;

use anyhow::Context;
use osmpbfreader::{OsmObj, OsmPbfReader};

use crate::nsi::NsiBrandMatcher;
use crate::parser::ParsedPOI;

use super::helpers::{build_way_poi, categorize_poi, tags_to_map};

#[derive(Debug)]
struct WayPoiSeed {
    id: i64,
    tags: HashMap<String, String>,
    nodes: Vec<i64>,
}

/// Parse only POI ways from a PBF extract.
///
/// Useful for targeted backfills where exits/highways are already imported and
/// only missing way-derived POIs need to be added.
pub(crate) fn parse_pbf_extract_way_pois_only(
    path: &Path,
    nsi: Option<&NsiBrandMatcher>,
) -> Result<Vec<ParsedPOI>, anyhow::Error> {
    let (way_pois, needed_nodes) = collect_way_poi_seeds(path)?;
    if way_pois.is_empty() {
        return Ok(Vec::new());
    }

    let node_coords = collect_node_coords(path, &needed_nodes)?;
    let pois = build_way_pois(&way_pois, &node_coords, nsi);
    Ok(pois)
}

fn collect_way_poi_seeds(path: &Path) -> Result<(Vec<WayPoiSeed>, HashSet<i64>), anyhow::Error> {
    let file = File::open(path).with_context(|| format!("open {}", path.display()))?;
    let mut pbf = OsmPbfReader::new(file);

    let mut way_pois = Vec::new();
    let mut needed_nodes: HashSet<i64> = HashSet::new();

    for obj in pbf.iter() {
        let obj = obj.with_context(|| format!("reading {}", path.display()))?;
        if let OsmObj::Way(way) = obj {
            let tags = tags_to_map(&way.tags);
            if categorize_poi(&tags).is_none() {
                continue;
            }

            let nodes: Vec<i64> = way.nodes.iter().map(|node| node.0).collect();
            if nodes.len() < 2 {
                continue;
            }

            for node_id in &nodes {
                needed_nodes.insert(*node_id);
            }

            way_pois.push(WayPoiSeed {
                id: way.id.0,
                tags,
                nodes,
            });
        }
    }

    Ok((way_pois, needed_nodes))
}

fn collect_node_coords(
    path: &Path,
    needed_nodes: &HashSet<i64>,
) -> Result<HashMap<i64, (f64, f64)>, anyhow::Error> {
    let file = File::open(path).with_context(|| format!("reopen {}", path.display()))?;
    let mut pbf = OsmPbfReader::new(file);

    let mut node_coords: HashMap<i64, (f64, f64)> = HashMap::new();
    for obj in pbf.iter() {
        let obj = obj.with_context(|| format!("second pass {}", path.display()))?;
        if let OsmObj::Node(node) = obj {
            let node_id = node.id.0;
            if needed_nodes.contains(&node_id) {
                node_coords.insert(node_id, (node.lat(), node.lon()));
            }
        }
    }

    Ok(node_coords)
}

fn build_way_pois(
    way_pois: &[WayPoiSeed],
    node_coords: &HashMap<i64, (f64, f64)>,
    nsi: Option<&NsiBrandMatcher>,
) -> Vec<ParsedPOI> {
    let mut pois = Vec::new();

    for way in way_pois {
        let coords: Vec<(f64, f64)> = way
            .nodes
            .iter()
            .filter_map(|node_id| node_coords.get(node_id).copied())
            .collect();
        if coords.len() < 2 || coords.len() != way.nodes.len() {
            continue;
        }

        if let Some(poi) = build_way_poi(way.id, &way.tags, &coords, nsi) {
            pois.push(poi);
        }
    }

    pois
}
