-- Deterministic projection for OpenInterstate product tables.
--
-- Canonical sources:
--   osm2pgsql_v2_exits_nodes
--   osm2pgsql_v2_poi_nodes
--   osm2pgsql_v2_poi_ways
--   osm2pgsql_v2_highways

BEGIN;

CREATE EXTENSION IF NOT EXISTS postgis;

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
    source_way_ids_json TEXT NOT NULL DEFAULT '[]',
    direction TEXT
);
CREATE INDEX IF NOT EXISTS highway_edges_geom_idx ON highway_edges USING GIST (geom);
CREATE INDEX IF NOT EXISTS highway_edges_corridor_idx ON highway_edges (highway, component);
CREATE INDEX IF NOT EXISTS highway_edges_start_node_idx ON highway_edges (start_node);
CREATE INDEX IF NOT EXISTS highway_edges_end_node_idx ON highway_edges (end_node);
ALTER TABLE highway_edges
    ADD COLUMN IF NOT EXISTS source_way_ids_json TEXT NOT NULL DEFAULT '[]';

-- Corridor tables (populated by openinterstate-derive after this SQL)
ALTER TABLE highway_edges ADD COLUMN IF NOT EXISTS corridor_id INTEGER;
CREATE INDEX IF NOT EXISTS highway_edges_corridor_id_idx ON highway_edges (corridor_id);

CREATE TABLE IF NOT EXISTS corridors (
    corridor_id INTEGER PRIMARY KEY,
    highway TEXT NOT NULL,
    canonical_direction TEXT,
    root_relation_id BIGINT,
    geometry_json TEXT NOT NULL DEFAULT '[]',
    source_way_ids_json TEXT NOT NULL DEFAULT '[]'
);
CREATE INDEX IF NOT EXISTS corridors_highway_idx ON corridors (highway);
ALTER TABLE corridors
    ADD COLUMN IF NOT EXISTS root_relation_id BIGINT;
ALTER TABLE corridors
    ADD COLUMN IF NOT EXISTS geometry_json TEXT NOT NULL DEFAULT '[]';
ALTER TABLE corridors
    ADD COLUMN IF NOT EXISTS source_way_ids_json TEXT NOT NULL DEFAULT '[]';

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
CREATE INDEX IF NOT EXISTS exit_corridors_corridor ON exit_corridors (highway, graph_component);

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

DO $$
BEGIN
    EXECUTE 'DROP TABLE IF EXISTS ' || 'test' || '_drive_route_anchors';
    EXECUTE 'DROP TABLE IF EXISTS ' || 'test' || '_drive_routes';
END $$;

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

DO $$
BEGIN
    IF to_regclass('public.osm2pgsql_v2_exits_nodes') IS NULL
       OR to_regclass('public.osm2pgsql_v2_poi_nodes') IS NULL
       OR to_regclass('public.osm2pgsql_v2_poi_ways') IS NULL
       OR to_regclass('public.osm2pgsql_v2_highways') IS NULL THEN
        RAISE EXCEPTION
            'Missing canonical osm2pgsql tables. Run bin/openinterstate import first.';
    END IF;
END $$;

-- Preserve reachability history/cache rows across derive runs.
-- highway_edges and exit_corridors are rebuilt by --build-graph-only (runs after this SQL).
TRUNCATE
    exits,
    pois,
    exit_poi_candidates,
    canonical_exits,
    canonical_exit_aliases,
    corridors,
    corridor_exits,
    reference_route_anchors,
    reference_routes;

WITH rest_anchor_exits AS (
    SELECT DISTINCT en.node_id
    FROM osm2pgsql_v2_exits_nodes en
    JOIN (
        SELECT pn.geom
        FROM osm2pgsql_v2_poi_nodes pn
        WHERE pn.category = 'restArea'
           OR LOWER(TRIM(COALESCE(NULLIF(pn.display_name, ''), NULLIF(pn.name, ''), ''))) LIKE '%welcome center%'
        UNION ALL
        SELECT ST_PointOnSurface(pw.geom)::geometry(Point, 4326) AS geom
        FROM osm2pgsql_v2_poi_ways pw
        WHERE pw.category = 'restArea'
           OR LOWER(TRIM(COALESCE(NULLIF(pw.display_name, ''), NULLIF(pw.name, ''), ''))) LIKE '%welcome center%'
    ) rp
      ON rp.geom && ST_Expand(en.geom, 0.02)
     AND ST_DWithin(rp.geom::geography, en.geom::geography, 1200.0)
)
INSERT INTO exits (id, osm_type, osm_id, state, ref, name, highway, direction, geom, tags_json)
SELECT
    'node/' || en.node_id::text AS id,
    'node' AS osm_type,
    en.node_id,
    NULL::text AS state,
    COALESCE(
        NULLIF(TRIM(en.ref), ''),
        NULLIF(TRIM(en.tags ->> 'ref'), ''),
        NULLIF(TRIM(en.tags ->> 'junction:ref'), '')
    ) AS ref,
    COALESCE(NULLIF(en.name, ''), NULLIF(en.tags ->> 'exit:name', ''), NULLIF(en.tags ->> 'name', ''), NULLIF(en.tags ->> 'destination', '')) AS name,
    hw.highway_pick,
    NULL::text AS direction,
    en.geom,
    en.tags AS tags_json
FROM osm2pgsql_v2_exits_nodes en
LEFT JOIN LATERAL (
    SELECT
        COALESCE(
            NULLIF(trim(h.ref), ''),
            NULLIF(trim(h.tags ->> 'ref'), ''),
            h.highway
        ) AS highway_pick
    FROM osm2pgsql_v2_highways h
    WHERE h.highway IN ('motorway', 'trunk', 'motorway_link', 'trunk_link')
      AND ST_DWithin(h.geom, en.geom, 0.001)
    ORDER BY
        CASE WHEN h.highway IN ('motorway', 'trunk') THEN 0 ELSE 1 END,
        h.geom <-> en.geom
) hw ON TRUE
WHERE (
        COALESCE(
            NULLIF(TRIM(en.ref), ''),
            NULLIF(TRIM(en.tags ->> 'ref'), ''),
            NULLIF(TRIM(en.tags ->> 'junction:ref'), '')
        ) ~ '[0-9]'
        OR en.node_id IN (SELECT node_id FROM rest_anchor_exits)
      )
ON CONFLICT (id) DO NOTHING;

INSERT INTO pois (id, osm_type, osm_id, state, category, name, display_name, brand, geom, tags_json)
SELECT
    src.id,
    src.osm_type,
    src.osm_id,
    NULL::text AS state,
    src.category,
    CASE
        WHEN src.category = 'restArea' AND (
            COALESCE(NULLIF(src.name, ''), '') = ''
            OR (COALESCE(src.tags_json->>'name', '') = ''
                AND COALESCE(src.tags_json->>'brand', '') = '')
        )
        THEN 'Rest Area'
        ELSE src.name
    END AS name,
    CASE
        WHEN src.category = 'restArea' AND (
            COALESCE(src.tags_json->>'name', '') = ''
            AND COALESCE(src.tags_json->>'brand', '') = '')
        THEN 'Rest Area'
        ELSE COALESCE(
            NULLIF(src.display_name, ''),
            NULLIF(src.name, ''),
            CASE WHEN src.category = 'restArea' THEN 'Rest Area' END
        )
    END AS display_name,
    src.brand,
    src.geom,
    src.tags_json
FROM (
    SELECT
        'node/' || n.node_id::text AS id,
        'node'::text AS osm_type,
        n.node_id AS osm_id,
        n.category,
        n.name,
        n.display_name,
        n.brand,
        n.geom,
        n.tags AS tags_json
    FROM osm2pgsql_v2_poi_nodes n
    UNION ALL
    SELECT
        'way/' || w.osm_id::text AS id,
        'way'::text AS osm_type,
        w.osm_id AS osm_id,
        w.category,
        w.name,
        w.display_name,
        w.brand,
        ST_PointOnSurface(w.geom)::geometry(Point, 4326) AS geom,
        w.tags AS tags_json
    FROM osm2pgsql_v2_poi_ways w
) src;


-- Refresh planner statistics after bulk-loading exits and pois so the
-- spatial join below uses accurate row estimates and index selectivity.
ANALYZE exits;
ANALYZE pois;

INSERT INTO exit_poi_candidates (exit_id, poi_id, category, distance_m, rank)
SELECT exit_id, poi_id, category, distance_m, rank
FROM (
    SELECT
        e.id AS exit_id,
        p.id AS poi_id,
        p.category,
        ROUND(ST_Distance(e.geom::geography, p.geom::geography))::integer AS distance_m,
        ROW_NUMBER() OVER (
            PARTITION BY e.id, p.category
            ORDER BY ST_Distance(e.geom, p.geom)
        ) AS rank
    FROM exits e
    JOIN pois p
      ON p.geom && ST_Expand(e.geom, 0.012)
     AND ST_DWithin(e.geom::geography, p.geom::geography, 800.0)
    WHERE p.category IS NOT NULL
      AND p.category <> 'restroom'
      AND (p.category = 'restArea' OR LOWER(TRIM(COALESCE(NULLIF(p.display_name, ''), NULLIF(p.name, ''), 'Unknown'))) <> 'unknown')
      AND NOT EXISTS (
          SELECT 1
          FROM exit_poi_reachability prior
          WHERE prior.exit_id = e.id
            AND prior.poi_id = p.id
            AND prior.reachable = FALSE
      )
) ranked
WHERE rank <= 12;

-- Drop direction-labeled rest areas that explicitly conflict with exit travel
-- direction (for example, "eastbound" at a westbound exit).
WITH directional_labels AS (
    SELECT
        c.exit_id,
        c.poi_id,
        CASE
            WHEN LOWER(TRIM(COALESCE(ec.direction, e.direction, ''))) IN ('west', 'w', 'wb') THEN 'west'
            WHEN LOWER(TRIM(COALESCE(ec.direction, e.direction, ''))) IN ('east', 'e', 'eb') THEN 'east'
            WHEN LOWER(TRIM(COALESCE(ec.direction, e.direction, ''))) IN ('north', 'n', 'nb') THEN 'north'
            WHEN LOWER(TRIM(COALESCE(ec.direction, e.direction, ''))) IN ('south', 's', 'sb') THEN 'south'
            ELSE NULL
        END AS exit_dir,
        LOWER(TRIM(COALESCE(NULLIF(p.display_name, ''), NULLIF(p.name, ''), NULLIF(p.tags_json ->> 'name', ''), ''))) AS poi_label
    FROM exit_poi_candidates c
    JOIN pois p ON p.id = c.poi_id
    JOIN exits e ON e.id = c.exit_id
    LEFT JOIN exit_corridors ec ON ec.exit_id = c.exit_id
    WHERE c.category = 'restArea'
),
parsed AS (
    SELECT
        dl.exit_id,
        dl.poi_id,
        dl.exit_dir,
        (dl.poi_label ~ '(^|[^a-z])(eastbound|east[[:space:]]*bound|eb)([^a-z]|$)') AS has_east,
        (dl.poi_label ~ '(^|[^a-z])(westbound|west[[:space:]]*bound|wb)([^a-z]|$)') AS has_west,
        (dl.poi_label ~ '(^|[^a-z])(northbound|north[[:space:]]*bound|nb)([^a-z]|$)') AS has_north,
        (dl.poi_label ~ '(^|[^a-z])(southbound|south[[:space:]]*bound|sb)([^a-z]|$)') AS has_south
    FROM directional_labels dl
),
mismatched AS (
    SELECT
        p.exit_id,
        p.poi_id
    FROM parsed p
    WHERE p.exit_dir IS NOT NULL
      AND (
          (
              p.exit_dir IN ('east', 'west')
              AND (CASE WHEN p.has_east THEN 1 ELSE 0 END + CASE WHEN p.has_west THEN 1 ELSE 0 END) = 1
              AND (
                  (p.has_east AND p.exit_dir <> 'east')
                  OR
                  (p.has_west AND p.exit_dir <> 'west')
              )
          )
          OR
          (
              p.exit_dir IN ('north', 'south')
              AND (CASE WHEN p.has_north THEN 1 ELSE 0 END + CASE WHEN p.has_south THEN 1 ELSE 0 END) = 1
              AND (
                  (p.has_north AND p.exit_dir <> 'north')
                  OR
                  (p.has_south AND p.exit_dir <> 'south')
              )
          )
      )
)
DELETE FROM exit_poi_candidates c
USING mismatched m
WHERE c.exit_id = m.exit_id
  AND c.poi_id = m.poi_id;

-- highway_edges, exit_corridors, corridors, and corridor_exits are rebuilt
-- by openinterstate-derive steps that run after this SQL in the pipeline:
--   1. --build-graph-only    -> highway_edges + exit_corridors
--   2. --build-corridors-only -> corridors + corridor_exits + highway_edges.corridor_id

COMMIT;
