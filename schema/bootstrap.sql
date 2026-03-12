-- OpenInterstate bootstrap schema for fresh Postgres initialization.
-- This repo does not carry an in-place upgrade path; a fresh clone should
-- initialize directly into the current standalone schema.

CREATE TABLE IF NOT EXISTS exits (
    id TEXT PRIMARY KEY,
    osm_type TEXT NOT NULL,
    osm_id BIGINT NOT NULL,
    state TEXT,
    ref TEXT,
    name TEXT,
    highway TEXT,
    direction TEXT,
    geom GEOMETRY(Point, 4326) NOT NULL,
    tags_json JSONB
);
CREATE INDEX IF NOT EXISTS exits_geom_idx ON exits USING GIST (geom);
CREATE INDEX IF NOT EXISTS exits_highway_idx ON exits (highway);

CREATE TABLE IF NOT EXISTS pois (
    id TEXT PRIMARY KEY,
    osm_type TEXT NOT NULL,
    osm_id BIGINT NOT NULL,
    state TEXT,
    category TEXT,
    name TEXT,
    display_name TEXT,
    brand TEXT,
    geom GEOMETRY(Point, 4326) NOT NULL,
    tags_json JSONB
);
CREATE INDEX IF NOT EXISTS pois_geom_idx ON pois USING GIST (geom);
CREATE INDEX IF NOT EXISTS pois_category_idx ON pois (category);

CREATE TABLE IF NOT EXISTS highway_edges (
    id TEXT PRIMARY KEY,
    highway TEXT NOT NULL,
    component INTEGER NOT NULL,
    start_node BIGINT NOT NULL,
    end_node BIGINT NOT NULL,
    length_m INTEGER NOT NULL,
    geom GEOMETRY(LineString, 4326) NOT NULL,
    min_lat DOUBLE PRECISION NOT NULL,
    max_lat DOUBLE PRECISION NOT NULL,
    min_lon DOUBLE PRECISION NOT NULL,
    max_lon DOUBLE PRECISION NOT NULL,
    polyline_json TEXT NOT NULL,
    direction TEXT,
    corridor_id INTEGER
);
CREATE INDEX IF NOT EXISTS highway_edges_geom_idx ON highway_edges USING GIST (geom);
CREATE INDEX IF NOT EXISTS highway_edges_corridor_idx ON highway_edges (highway, component);
CREATE INDEX IF NOT EXISTS highway_edges_start_node_idx ON highway_edges (start_node);
CREATE INDEX IF NOT EXISTS highway_edges_end_node_idx ON highway_edges (end_node);
CREATE INDEX IF NOT EXISTS highway_edges_corridor_id_idx ON highway_edges (corridor_id);

CREATE TABLE IF NOT EXISTS corridors (
    corridor_id INTEGER PRIMARY KEY,
    highway TEXT NOT NULL,
    canonical_direction TEXT
);
CREATE INDEX IF NOT EXISTS corridors_highway_idx ON corridors (highway);

CREATE TABLE IF NOT EXISTS corridor_exits (
    corridor_id INTEGER NOT NULL,
    corridor_index INTEGER NOT NULL,
    exit_id TEXT NOT NULL,
    ref TEXT,
    name TEXT,
    lat DOUBLE PRECISION NOT NULL,
    lon DOUBLE PRECISION NOT NULL,
    PRIMARY KEY (corridor_id, corridor_index)
);
CREATE INDEX IF NOT EXISTS corridor_exits_exit_idx ON corridor_exits (exit_id);

CREATE TABLE IF NOT EXISTS exit_corridors (
    exit_id TEXT NOT NULL,
    highway TEXT NOT NULL,
    graph_component INTEGER NOT NULL,
    graph_node BIGINT NOT NULL,
    direction TEXT,
    PRIMARY KEY (exit_id, highway)
);
CREATE INDEX IF NOT EXISTS exit_corridors_corridor
    ON exit_corridors (highway, graph_component);

CREATE TABLE IF NOT EXISTS exit_poi_candidates (
    exit_id TEXT NOT NULL,
    poi_id TEXT NOT NULL,
    category TEXT NOT NULL,
    distance_m INTEGER NOT NULL,
    rank INTEGER NOT NULL,
    PRIMARY KEY (exit_id, poi_id)
);
CREATE INDEX IF NOT EXISTS exit_poi_candidates_exit_idx
    ON exit_poi_candidates (exit_id, category, rank);

CREATE TABLE IF NOT EXISTS canonical_exits (
    id TEXT PRIMARY KEY,
    primary_exit_id TEXT NOT NULL,
    highway TEXT NOT NULL,
    direction TEXT NOT NULL,
    ref TEXT,
    name TEXT,
    geom GEOMETRY(Point, 4326) NOT NULL,
    sequence_index INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS canonical_exits_corridor_seq_idx
    ON canonical_exits (highway, direction, sequence_index);
CREATE INDEX IF NOT EXISTS canonical_exits_geom_idx
    ON canonical_exits USING GIST (geom);

CREATE TABLE IF NOT EXISTS canonical_exit_aliases (
    canonical_id TEXT NOT NULL,
    exit_id TEXT NOT NULL,
    PRIMARY KEY (canonical_id, exit_id)
);
CREATE INDEX IF NOT EXISTS canonical_exit_aliases_exit_idx
    ON canonical_exit_aliases (exit_id);

CREATE TABLE IF NOT EXISTS reference_routes (
    id UUID PRIMARY KEY,
    highway TEXT NOT NULL,
    direction_code TEXT NOT NULL,
    direction_label TEXT NOT NULL,
    display_name TEXT NOT NULL,
    corridor_id INTEGER NOT NULL,
    variant_rank INTEGER NOT NULL,
    distance_m DOUBLE PRECISION NOT NULL,
    duration_s DOUBLE PRECISION NOT NULL,
    interval_s DOUBLE PRECISION NOT NULL,
    point_count INTEGER NOT NULL,
    start_lat DOUBLE PRECISION NOT NULL,
    start_lon DOUBLE PRECISION NOT NULL,
    end_lat DOUBLE PRECISION NOT NULL,
    end_lon DOUBLE PRECISION NOT NULL,
    min_lat DOUBLE PRECISION NOT NULL,
    max_lat DOUBLE PRECISION NOT NULL,
    min_lon DOUBLE PRECISION NOT NULL,
    max_lon DOUBLE PRECISION NOT NULL,
    waypoints_json TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS reference_routes_highway_direction_idx
    ON reference_routes (highway, direction_code, variant_rank);

CREATE TABLE IF NOT EXISTS reference_route_anchors (
    route_id UUID NOT NULL REFERENCES reference_routes(id) ON DELETE CASCADE,
    anchor_index INTEGER NOT NULL,
    lat DOUBLE PRECISION NOT NULL,
    lon DOUBLE PRECISION NOT NULL,
    PRIMARY KEY (route_id, anchor_index)
);
CREATE INDEX IF NOT EXISTS reference_route_anchors_lat_lon_idx
    ON reference_route_anchors (lat, lon);

CREATE TABLE IF NOT EXISTS exit_poi_reachability (
    exit_id TEXT NOT NULL,
    poi_id TEXT NOT NULL,
    route_distance_m INTEGER,
    route_duration_s INTEGER,
    reachable BOOLEAN NOT NULL DEFAULT FALSE,
    reachability_score DOUBLE PRECISION,
    reachability_confidence DOUBLE PRECISION,
    provider TEXT NOT NULL DEFAULT 'osrm',
    provider_dataset_version TEXT,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (exit_id, poi_id)
);
CREATE INDEX IF NOT EXISTS exit_poi_reachability_exit_idx
    ON exit_poi_reachability (exit_id);

CREATE TABLE IF NOT EXISTS osrm_snap_hints (
    source_scope TEXT NOT NULL,
    endpoint_kind TEXT NOT NULL,
    endpoint_id TEXT NOT NULL,
    dataset_key TEXT NOT NULL,
    input_lon DOUBLE PRECISION NOT NULL,
    input_lat DOUBLE PRECISION NOT NULL,
    snapped_lon DOUBLE PRECISION NOT NULL,
    snapped_lat DOUBLE PRECISION NOT NULL,
    hint TEXT NOT NULL,
    snapped_distance_m DOUBLE PRECISION,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (source_scope, endpoint_kind, endpoint_id, dataset_key)
);
CREATE INDEX IF NOT EXISTS osrm_snap_hints_lookup_idx
    ON osrm_snap_hints (source_scope, endpoint_kind, dataset_key);
