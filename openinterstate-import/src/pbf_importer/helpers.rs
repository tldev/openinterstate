use std::collections::{HashMap, HashSet};

use osmpbfreader::Tags;
use openinterstate_core::geo::haversine_distance;
use openinterstate_core::highway_ref::normalize_highway_ref;

use crate::nsi::NsiBrandMatcher;
use crate::parser::{ParsedExit, ParsedHighway, ParsedPOI};

use super::full_extract::WaySeed;

const GRID_CELL_DEG: f64 = 0.02;

pub(super) fn tags_to_map(tags: &Tags) -> HashMap<String, String> {
    tags.iter()
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect()
}

pub(super) fn categorize_poi(tags: &HashMap<String, String>) -> Option<&'static str> {
    let amenity = tags.get("amenity").map(String::as_str).unwrap_or("");
    let tourism = tags.get("tourism").map(String::as_str).unwrap_or("");
    let highway = tags.get("highway").map(String::as_str).unwrap_or("");
    let shop = tags.get("shop").map(String::as_str).unwrap_or("");

    if amenity == "fuel" || shop == "gas" {
        return Some("gas");
    }
    if tourism == "hotel" || tourism == "motel" || tourism == "guest_house" {
        return Some("lodging");
    }
    if amenity == "restaurant"
        || amenity == "fast_food"
        || amenity == "cafe"
        || tags.contains_key("cuisine")
    {
        return Some("food");
    }
    if highway == "rest_area" || highway == "services" {
        return Some("restArea");
    }
    if amenity == "toilets" {
        return Some("restroom");
    }
    if amenity == "charging_station" {
        return Some("evCharging");
    }
    None
}

pub(super) fn build_way_poi(
    way_id: i64,
    tags: &HashMap<String, String>,
    coords: &[(f64, f64)],
    nsi: Option<&NsiBrandMatcher>,
) -> Option<ParsedPOI> {
    let category = categorize_poi(tags)?;
    if coords.is_empty() {
        return None;
    }

    let count = coords.len() as f64;
    let (sum_lat, sum_lon) = coords
        .iter()
        .fold((0.0_f64, 0.0_f64), |(a, b), (lat, lon)| (a + lat, b + lon));
    let lat = sum_lat / count;
    let lon = sum_lon / count;
    let raw_name = tags
        .get("brand")
        .or_else(|| tags.get("name"))
        .or_else(|| tags.get("operator"))
        .cloned()
        .unwrap_or_else(|| "Unknown".to_string());
    let canonical = nsi
        .and_then(|m| m.canonicalize(&raw_name, category))
        .unwrap_or_else(|| raw_name.clone());
    let display_name = openinterstate_core::brand_helpers::normalize_brand(&canonical).to_string();

    Some(ParsedPOI {
        id: format!("way/{way_id}"),
        osm_type: "way".into(),
        osm_id: way_id,
        lat,
        lon,
        state: None,
        category: Some(category.to_string()),
        name: Some(raw_name),
        display_name: Some(display_name),
        brand: tags.get("brand").cloned(),
        tags_json: serde_json::to_string(tags).ok(),
    })
}

pub(super) fn build_node_to_refs_index(ways: &[WaySeed]) -> HashMap<i64, Vec<String>> {
    let mut node_to_refs: HashMap<i64, Vec<String>> = HashMap::new();

    for way in ways {
        let is_mainline = matches!(
            way.highway_type.as_deref(),
            Some("motorway") | Some("trunk")
        );
        if !is_mainline {
            continue;
        }

        let refs_raw = way.tags.get("ref").map(String::as_str).unwrap_or("");
        if refs_raw.is_empty() {
            continue;
        }

        let refs: Vec<String> = refs_raw
            .split(';')
            .filter_map(|r| normalize_highway_ref(r.trim()))
            .collect();
        if refs.is_empty() {
            continue;
        }

        for node_id in &way.nodes {
            let entry = node_to_refs.entry(*node_id).or_default();
            for r in &refs {
                if !entry.contains(r) {
                    entry.push(r.clone());
                }
            }
        }
    }

    node_to_refs
}

pub(super) fn pick_primary_highway_from_tags(
    tags: &HashMap<String, String>,
    node_refs: Option<&Vec<String>>,
) -> Option<String> {
    if let Some(refs) = node_refs {
        for r in refs {
            if r.starts_with("I-") {
                return Some(r.clone());
            }
        }
        for r in refs {
            if r.starts_with("US-") {
                return Some(r.clone());
            }
        }
        if let Some(first) = refs.first() {
            return Some(first.clone());
        }
    }

    for key in ["highway:ref", "destination:ref", "destination:ref:to"] {
        if let Some(val) = tags.get(key) {
            if let Some(norm) = normalize_highway_ref(val) {
                return Some(norm);
            }
        }
    }
    None
}

pub(super) fn build_exit_anchor_links(
    exit_node_ids: &HashSet<i64>,
    highway_nodes: &HashSet<i64>,
    node_coords: &HashMap<i64, (f64, f64)>,
) -> Vec<ParsedHighway> {
    let mut result = Vec::new();
    for exit in exit_node_ids {
        if !node_coords.contains_key(exit) {
            continue;
        }
        if highway_nodes.contains(exit) {
            continue;
        }
        let Some(&(lat, lon)) = node_coords.get(exit) else {
            continue;
        };
        result.push(ParsedHighway {
            id: format!("way/anchor-{exit}"),
            refs: Vec::new(),
            nodes: vec![*exit, *exit],
            geometry: vec![(lat, lon), (lat, lon)],
            highway_type: "service".to_string(),
            is_oneway: true,
        });
    }
    result
}

#[derive(Clone, Copy)]
struct ExitPoint {
    lat: f64,
    lon: f64,
}

pub(super) fn build_exit_grid(exits: &[ParsedExit]) -> HashMap<(i32, i32), Vec<(f64, f64)>> {
    let mut grid: HashMap<(i32, i32), Vec<(f64, f64)>> = HashMap::new();
    for e in exits {
        let key = grid_key(e.lat, e.lon);
        grid.entry(key).or_default().push((e.lat, e.lon));
    }
    grid
}

fn grid_key(lat: f64, lon: f64) -> (i32, i32) {
    (
        (lat / GRID_CELL_DEG).floor() as i32,
        (lon / GRID_CELL_DEG).floor() as i32,
    )
}

pub(super) fn way_near_any_exit(
    geometry: &[(f64, f64)],
    exit_grid: &HashMap<(i32, i32), Vec<(f64, f64)>>,
    buffer_m: f64,
) -> bool {
    if geometry.is_empty() {
        return false;
    }

    let mut min_lat = f64::INFINITY;
    let mut max_lat = f64::NEG_INFINITY;
    let mut min_lon = f64::INFINITY;
    let mut max_lon = f64::NEG_INFINITY;
    for &(lat, lon) in geometry {
        min_lat = min_lat.min(lat);
        max_lat = max_lat.max(lat);
        min_lon = min_lon.min(lon);
        max_lon = max_lon.max(lon);
    }

    let lat_pad = buffer_m / 111_000.0;
    let mid_lat = ((min_lat + max_lat) / 2.0).to_radians();
    let lon_pad = buffer_m / (111_000.0 * mid_lat.cos().abs().max(0.1));

    let min_key = grid_key(min_lat - lat_pad, min_lon - lon_pad);
    let max_key = grid_key(max_lat + lat_pad, max_lon + lon_pad);

    let mut candidates: Vec<ExitPoint> = Vec::new();
    for gx in min_key.0..=max_key.0 {
        for gy in min_key.1..=max_key.1 {
            if let Some(points) = exit_grid.get(&(gx, gy)) {
                for (lat, lon) in points {
                    candidates.push(ExitPoint {
                        lat: *lat,
                        lon: *lon,
                    });
                }
            }
        }
    }
    if candidates.is_empty() {
        return false;
    }

    for &(lat, lon) in geometry {
        for ep in &candidates {
            if haversine_distance(lat, lon, ep.lat, ep.lon) <= buffer_m {
                return true;
            }
        }
    }
    false
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pick_primary_prefers_connected_mainline_refs() {
        let mut tags = HashMap::new();
        tags.insert("ref".to_string(), "26B".to_string());
        let node_refs = vec!["I-10".to_string(), "US-90".to_string()];

        let highway = pick_primary_highway_from_tags(&tags, Some(&node_refs));
        assert_eq!(highway, Some("I-10".to_string()));
    }

    #[test]
    fn pick_primary_uses_explicit_highway_ref_when_no_node_refs() {
        let mut tags = HashMap::new();
        tags.insert("highway:ref".to_string(), "I 65".to_string());

        let highway = pick_primary_highway_from_tags(&tags, None);
        assert_eq!(highway, Some("I-65".to_string()));
    }

    #[test]
    fn pick_primary_does_not_fallback_to_exit_number_ref() {
        let mut tags = HashMap::new();
        tags.insert("ref".to_string(), "26B".to_string());

        let highway = pick_primary_highway_from_tags(&tags, None);
        assert_eq!(highway, None);
    }
}
