use std::cmp::Ordering;
use std::collections::HashMap;
use std::path::Path;

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
const DISCONNECTED_MICRO_SEGMENT_MAX_LENGTH_M: f64 = 2_000.0;
const DISCONNECTED_MICRO_SEGMENT_MAX_SHARE: f64 = 0.05;

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

pub async fn build_reference_routes(
    pool: &PgPool,
    _interstate_relation_cache: &Path,
) -> anyhow::Result<()> {
    let corridor_rows: Vec<(i32, String, Option<String>, String)> = sqlx::query_as(
        "SELECT corridor_id, highway, canonical_direction, geometry_json \
         FROM corridors \
         WHERE highway LIKE 'I-%'",
    )
    .fetch_all(pool)
    .await?;

    if corridor_rows.is_empty() {
        tracing::warn!("No interstate corridors found; skipping reference route build");
        clear_tables(pool).await?;
        return Ok(());
    }

    let corridor_geometry_rows: Vec<(CorridorInfo, String)> = corridor_rows
        .into_iter()
        .filter_map(|(id, highway, dir, geometry_json)| {
            Some((
                CorridorInfo {
                    corridor_id: id,
                    highway,
                    canonical_direction: dir?,
                },
                geometry_json,
            ))
        })
        .collect();
    let mut routes = build_geometry_backed_routes(&corridor_geometry_rows)?;

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

fn build_geometry_backed_routes(
    corridors: &[(CorridorInfo, String)],
) -> anyhow::Result<Vec<RouteRow>> {
    let mut routes = Vec::new();

    for (corridor, geometry_json) in corridors {
        let direction_code = canonical_to_direction_code(&corridor.canonical_direction);
        let mut route_segments = parse_geometry_json_segments(geometry_json);
        route_segments.retain(|segment| segment.len() >= 2);
        if route_segments.is_empty() {
            continue;
        }

        let mut sampled_segments = Vec::new();
        for mut segment in route_segments {
            if !polyline_matches_direction(&segment, direction_code) {
                segment.reverse();
            }
            let sampled = resample(&segment, STEP_M);
            let offset = apply_lane_offset(&sampled, LANE_OFFSET_M);
            if offset.len() >= 2 {
                sampled_segments.push(offset);
            }
        }
        if sampled_segments.is_empty() {
            continue;
        }
        sampled_segments = prune_geometry_micro_segments(sampled_segments);
        if sampled_segments.is_empty() {
            continue;
        }

        let distance_m: f64 = sampled_segments
            .iter()
            .map(|segment| segment_length_m(segment))
            .sum();
        if distance_m < MIN_LENGTH_M {
            continue;
        }

        let waypoints: Vec<[f64; 2]> = sampled_segments
            .iter()
            .flat_map(|segment| segment.iter().copied())
            .collect();
        let bounds = bounds_for_points(&waypoints);
        let start = waypoints.first().copied().unwrap_or([0.0, 0.0]);
        let end = waypoints.last().copied().unwrap_or([0.0, 0.0]);
        let waypoints_json = serialize_route_waypoints(&sampled_segments)?;
        let label = direction_label(direction_code).to_string();

        let route_id = deterministic_route_id(
            &corridor.highway,
            direction_code,
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

fn parse_geometry_json_segments(raw: &str) -> Vec<Vec<[f64; 2]>> {
    let Ok(value) = serde_json::from_str::<serde_json::Value>(raw) else {
        return Vec::new();
    };
    let Some(geometry_type) = value.get("type").and_then(|value| value.as_str()) else {
        return Vec::new();
    };

    match geometry_type {
        "LineString" => value
            .get("coordinates")
            .and_then(parse_linestring_coords)
            .map(|segment| vec![segment])
            .unwrap_or_default(),
        "MultiLineString" => value
            .get("coordinates")
            .and_then(|coords| coords.as_array())
            .map(|segments| {
                segments
                    .iter()
                    .filter_map(parse_linestring_coords)
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default(),
        _ => Vec::new(),
    }
}

fn parse_linestring_coords(value: &serde_json::Value) -> Option<Vec<[f64; 2]>> {
    let coords = value.as_array()?;
    let segment: Vec<[f64; 2]> = coords
        .iter()
        .filter_map(|pair| {
            let pair = pair.as_array()?;
            let lon = pair.first()?.as_f64()?;
            let lat = pair.get(1)?.as_f64()?;
            Some([lat, lon])
        })
        .collect();
    if segment.len() >= 2 {
        Some(segment)
    } else {
        None
    }
}

fn segment_length_m(segment: &[[f64; 2]]) -> f64 {
    segment
        .windows(2)
        .map(|pair| haversine_distance(pair[0][0], pair[0][1], pair[1][0], pair[1][1]))
        .sum()
}

fn prune_geometry_micro_segments(segments: Vec<Vec<[f64; 2]>>) -> Vec<Vec<[f64; 2]>> {
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

fn serialize_route_waypoints(segments: &[Vec<[f64; 2]>]) -> anyhow::Result<String> {
    match segments {
        [] => Ok("[]".to_string()),
        [segment] => Ok(serde_json::to_string(segment)?),
        _ => Ok(serde_json::to_string(segments)?),
    }
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
    let key = format!(
        "{}|{}|{}|{:x}",
        highway, direction_code, corridor_id, digest
    );
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
