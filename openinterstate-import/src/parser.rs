#[derive(Debug, Clone)]
pub struct ParsedExit {
    pub id: String,
    pub osm_type: String,
    pub osm_id: i64,
    pub lat: f64,
    pub lon: f64,
    pub state: Option<String>,
    pub r#ref: Option<String>,
    pub name: Option<String>,
    pub highway: Option<String>,
    pub direction: Option<String>,
    pub tags_json: Option<String>,
}

#[derive(Debug, Clone)]
pub struct ParsedPOI {
    pub id: String,
    pub osm_type: String,
    pub osm_id: i64,
    pub lat: f64,
    pub lon: f64,
    pub state: Option<String>,
    pub category: Option<String>,
    pub name: Option<String>,
    pub display_name: Option<String>,
    pub brand: Option<String>,
    pub tags_json: Option<String>,
}

#[derive(Debug, Clone)]
pub struct ParsedHighway {
    pub id: String,
    pub refs: Vec<String>,
    pub nodes: Vec<i64>,
    pub geometry: Vec<(f64, f64)>,
    pub highway_type: String,
    pub is_oneway: bool,
}

#[derive(Debug, Clone)]
pub struct ParsedData {
    pub exits: Vec<ParsedExit>,
    pub pois: Vec<ParsedPOI>,
    pub highways: Vec<ParsedHighway>,
}
