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
}
