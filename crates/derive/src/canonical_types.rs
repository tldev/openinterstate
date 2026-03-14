#[derive(Debug, Clone)]
pub struct ParsedExit {
    pub id: String,
    pub osm_id: i64,
}

#[derive(Debug, Clone)]
pub struct ParsedHighway {
    pub way_id: i64,
    pub refs: Vec<String>,
    pub nodes: Vec<i64>,
    pub geometry: Vec<(f64, f64)>,
    pub highway_type: String,
    pub is_oneway: bool,
}
