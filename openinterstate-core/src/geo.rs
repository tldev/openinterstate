/// Earth radius in meters (mean).
pub const EARTH_RADIUS: f64 = 6_371_000.0;

#[inline]
pub fn to_radians(deg: f64) -> f64 {
    deg * std::f64::consts::PI / 180.0
}

#[inline]
pub fn to_degrees(rad: f64) -> f64 {
    rad * 180.0 / std::f64::consts::PI
}

/// Haversine distance in meters between two lat/lon points.
pub fn haversine_distance(lat1: f64, lon1: f64, lat2: f64, lon2: f64) -> f64 {
    let d_lat = to_radians(lat2 - lat1);
    let d_lon = to_radians(lon2 - lon1);
    let a = (d_lat / 2.0).sin().powi(2)
        + to_radians(lat1).cos() * to_radians(lat2).cos() * (d_lon / 2.0).sin().powi(2);
    EARTH_RADIUS * 2.0 * a.sqrt().atan2((1.0 - a).sqrt())
}

/// Initial bearing in degrees [0, 360) from point 1 to point 2.
pub fn bearing(lat1: f64, lon1: f64, lat2: f64, lon2: f64) -> f64 {
    let d_lon = to_radians(lon2 - lon1);
    let y = d_lon.sin() * to_radians(lat2).cos();
    let x = to_radians(lat1).cos() * to_radians(lat2).sin()
        - to_radians(lat1).sin() * to_radians(lat2).cos() * d_lon.cos();
    (to_degrees(y.atan2(x)) + 360.0) % 360.0
}

/// Normalize angle to [-180, 180].
pub fn normalize_angle(angle: f64) -> f64 {
    let mut a = angle % 360.0;
    if a > 180.0 {
        a -= 360.0;
    }
    if a < -180.0 {
        a += 360.0;
    }
    a
}

/// Absolute difference between two angles, in [0, 180].
pub fn absolute_angle_difference(a: f64, b: f64) -> f64 {
    normalize_angle(a - b).abs()
}

/// Project a point forward along a bearing by a given distance.
pub fn project_point(lat: f64, lon: f64, bearing_deg: f64, distance_meters: f64) -> (f64, f64) {
    let brng = to_radians(bearing_deg);
    let d = distance_meters / EARTH_RADIUS;
    let lat1 = to_radians(lat);
    let lon1 = to_radians(lon);

    let lat2 = (lat1.sin() * d.cos() + lat1.cos() * d.sin() * brng.cos()).asin();
    let lon2 = lon1 + (brng.sin() * d.sin() * lat1.cos()).atan2(d.cos() - lat1.sin() * lat2.sin());

    (to_degrees(lat2), to_degrees(lon2))
}

/// Signed cross-track distance in meters from a point to the great-circle path (start→end).
pub fn cross_track_distance_meters(
    point_lat: f64,
    point_lon: f64,
    start_lat: f64,
    start_lon: f64,
    end_lat: f64,
    end_lon: f64,
) -> f64 {
    let dist_ap = haversine_distance(start_lat, start_lon, point_lat, point_lon) / EARTH_RADIUS;
    let bearing_ap = to_radians(bearing(start_lat, start_lon, point_lat, point_lon));
    let bearing_ab = to_radians(bearing(start_lat, start_lon, end_lat, end_lon));
    (dist_ap.sin() * (bearing_ap - bearing_ab).sin()).asin() * EARTH_RADIUS
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_haversine_known_distance() {
        // NYC to LA: ~3944 km
        let d = haversine_distance(40.7128, -74.0060, 34.0522, -118.2437);
        assert!((d - 3_944_000.0).abs() < 10_000.0);
    }

    #[test]
    fn test_haversine_same_point() {
        let d = haversine_distance(35.0, -80.0, 35.0, -80.0);
        assert!(d.abs() < 0.001);
    }

    #[test]
    fn test_bearing_north() {
        let b = bearing(35.0, -80.0, 36.0, -80.0);
        assert!((b - 0.0).abs() < 1.0);
    }

    #[test]
    fn test_bearing_east() {
        let b = bearing(35.0, -80.0, 35.0, -79.0);
        assert!((b - 90.0).abs() < 1.5);
    }

    #[test]
    fn test_normalize_angle() {
        assert!((normalize_angle(270.0) - (-90.0)).abs() < 0.001);
        assert!((normalize_angle(-270.0) - 90.0).abs() < 0.001);
        assert!((normalize_angle(0.0)).abs() < 0.001);
        assert!((normalize_angle(180.0) - 180.0).abs() < 0.001);
    }

    #[test]
    fn test_project_point_north() {
        let (lat, lon) = project_point(35.0, -80.0, 0.0, 1000.0);
        assert!(lat > 35.0);
        assert!((lon - (-80.0)).abs() < 0.01);
    }

    #[test]
    fn test_cross_track_on_path() {
        // Point on the path should have ~0 cross-track distance
        let ct = cross_track_distance_meters(35.5, -80.0, 35.0, -80.0, 36.0, -80.0);
        assert!(ct.abs() < 100.0);
    }
}
