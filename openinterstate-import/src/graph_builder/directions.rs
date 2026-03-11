use std::collections::HashMap;

use openinterstate_core::geo::bearing;

use super::compress::CompressedEdge;

/// Compute the cardinal direction (N/S/E/W) for each (highway, component) pair.
/// Uses length-weighted average bearing of all edge polylines in the component.
pub(super) fn compute_component_directions(
    edges: &[CompressedEdge],
    highway: &str,
) -> HashMap<(String, i32), String> {
    let mut sin_sum: HashMap<i32, f64> = HashMap::new();
    let mut cos_sum: HashMap<i32, f64> = HashMap::new();

    for edge in edges {
        if edge.highway != highway {
            continue;
        }

        let poly: Vec<(f64, f64)> = serde_json::from_str::<Vec<[f64; 2]>>(&edge.polyline_json)
            .unwrap_or_default()
            .into_iter()
            .map(|p| (p[0], p[1]))
            .collect();

        if poly.len() < 2 {
            continue;
        }

        let first = poly[0];
        let last = poly[poly.len() - 1];
        let b = bearing(first.0, first.1, last.0, last.1);
        let weight = edge.length_m as f64;

        let rad = b.to_radians();
        *sin_sum.entry(edge.component).or_default() += rad.sin() * weight;
        *cos_sum.entry(edge.component).or_default() += rad.cos() * weight;
    }

    let mut result = HashMap::new();
    for (&comp, sin_val) in &sin_sum {
        let cos_val = cos_sum.get(&comp).copied().unwrap_or(0.0);
        let avg_bearing = sin_val.atan2(cos_val).to_degrees().rem_euclid(360.0);
        let cardinal = bearing_to_cardinal(avg_bearing);
        result.insert((highway.to_string(), comp), cardinal.to_string());
    }

    result
}

fn bearing_to_cardinal(bearing_deg: f64) -> &'static str {
    let b = bearing_deg.rem_euclid(360.0);
    if !(45.0..315.0).contains(&b) {
        "N"
    } else if (45.0..135.0).contains(&b) {
        "E"
    } else if (135.0..225.0).contains(&b) {
        "S"
    } else {
        "W"
    }
}
