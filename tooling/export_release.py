#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import hashlib
import json
import math
import re
from dataclasses import dataclass
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any
from zipfile import ZIP_DEFLATED, ZipFile

import psycopg


# Keep the public release surface focused on official Interstate route numbers.
# In addition to plain numeric routes (I-95), include the current signed
# letter-suffixed branches that are part of the Interstate system.
INTERSTATE_FILTER = r"^(?:I-?[0-9]+|I-?35[EW]|I-?69[CEW])$"
INTERSTATE_NAME_RE = re.compile(INTERSTATE_FILTER)
SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
ROUTE_GAP_BREAK_METERS = 10_000


@dataclass(frozen=True)
class ExportSpec:
    name: str
    filename: str
    query: str
    columns: list[str] | None = None


def is_release_interstate_name(highway: str) -> bool:
    return bool(INTERSTATE_NAME_RE.fullmatch(highway.strip().upper()))


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Export OpenInterstate v1 release artifacts.")
    parser.add_argument("--database-url", required=True)
    parser.add_argument("--release-id", required=True)
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--state-dir")
    parser.add_argument("--source-pbf-file")
    parser.add_argument("--source-pbf-metadata-file")
    parser.add_argument("--import-pbf-file")
    parser.add_argument("--source-url")
    args = parser.parse_args(argv)
    has_source_file = bool(args.source_pbf_file)
    has_source_metadata = bool(args.source_pbf_metadata_file)
    if has_source_file == has_source_metadata:
        parser.error("exactly one of --source-pbf-file or --source-pbf-metadata-file is required")
    if has_source_metadata and not args.import_pbf_file:
        parser.error("--import-pbf-file is required when --source-pbf-metadata-file is used")
    return args


def ensure_dirs(output_dir: Path) -> tuple[Path, Path, Path]:
    csv_dir = output_dir / "csv"
    gpx_dir = output_dir / "gpx"
    examples_dir = output_dir / "examples"
    for path in (output_dir, csv_dir, gpx_dir, examples_dir):
        path.mkdir(parents=True, exist_ok=True)
    return csv_dir, gpx_dir, examples_dir


def normalize_value(value: Any) -> Any:
    if isinstance(value, datetime):
        return value.astimezone(timezone.utc).isoformat()
    if isinstance(value, memoryview):
        return value.tobytes().hex()
    return value


def fetch_rows(conn: psycopg.Connection, query: str) -> tuple[list[str], list[dict[str, Any]]]:
    with conn.cursor() as cur:
        cur.execute(query)
        columns = [desc.name for desc in cur.description]
        rows = []
        for record in cur.fetchall():
            rows.append({col: normalize_value(val) for col, val in zip(columns, record, strict=True)})
        return columns, rows


def write_csv(rows: list[dict[str, Any]], fieldnames: list[str], path: Path) -> None:
    with path.open("w", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def sha256_file(path: Path, hash_cache: dict[tuple[str, int, int], str] | None = None) -> str:
    stat = path.stat()
    cache_key = (str(path.resolve()), stat.st_size, stat.st_mtime_ns)
    if hash_cache is not None and cache_key in hash_cache:
        return hash_cache[cache_key]

    digest = hashlib.sha256()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(1024 * 1024), b""):
            digest.update(chunk)
    sha = digest.hexdigest()
    if hash_cache is not None:
        hash_cache[cache_key] = sha
    return sha


def metadata_cache_path(state_dir: Path, source_path: Path) -> Path:
    cache_key = hashlib.sha256(str(source_path.resolve()).encode("utf-8")).hexdigest()
    return state_dir / "file-metadata" / f"{cache_key}.json"


def build_source_file_metadata(
    path: Path,
    state_dir: Path | None = None,
    hash_cache: dict[tuple[str, int, int], str] | None = None,
) -> dict[str, Any]:
    stat = path.stat()
    modified_at = datetime.fromtimestamp(stat.st_mtime, tz=timezone.utc).isoformat()

    if state_dir is not None:
        cache_path = metadata_cache_path(state_dir, path)
        if cache_path.exists():
            try:
                cached = json.loads(cache_path.read_text(encoding="utf-8"))
            except json.JSONDecodeError:
                cached = None
            if (
                isinstance(cached, dict)
                and cached.get("path") == str(path.resolve())
                and cached.get("size_bytes") == stat.st_size
                and cached.get("modified_at") == modified_at
                and isinstance(cached.get("sha256"), str)
            ):
                return cached

    metadata = {
        "path": str(path.resolve()),
        "filename": path.name,
        "size_bytes": stat.st_size,
        "modified_at": modified_at,
        "sha256": sha256_file(path, hash_cache),
    }
    if state_dir is not None:
        cache_path.parent.mkdir(parents=True, exist_ok=True)
        cache_path.write_text(json.dumps(metadata, indent=2), encoding="utf-8")
    return metadata


def validate_source_file_metadata(raw: Any, label: str) -> dict[str, Any]:
    if not isinstance(raw, dict):
        raise ValueError(f"{label} metadata must be a JSON object")

    path = raw.get("path")
    filename = raw.get("filename")
    size_bytes = raw.get("size_bytes")
    modified_at = raw.get("modified_at")
    sha256 = raw.get("sha256")

    if not isinstance(path, str) or not path.strip():
        raise ValueError(f"{label} metadata must include a non-empty path")
    if not isinstance(filename, str) or not filename.strip():
        raise ValueError(f"{label} metadata must include a non-empty filename")
    if not isinstance(size_bytes, int) or size_bytes < 0:
        raise ValueError(f"{label} metadata must include a non-negative integer size_bytes")
    if not isinstance(modified_at, str) or not modified_at.strip():
        raise ValueError(f"{label} metadata must include a non-empty modified_at")
    if not isinstance(sha256, str) or not SHA256_RE.fullmatch(sha256):
        raise ValueError(f"{label} metadata must include a lowercase 64-character sha256")

    return {
        "path": path,
        "filename": filename,
        "size_bytes": size_bytes,
        "modified_at": modified_at,
        "sha256": sha256,
    }


def load_source_file_metadata(path: Path, label: str) -> dict[str, Any]:
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError as exc:
        raise ValueError(f"{label} metadata file not found: {path}") from exc
    except json.JSONDecodeError as exc:
        raise ValueError(f"{label} metadata file is not valid JSON: {path}") from exc
    return validate_source_file_metadata(raw, label)


def write_checksums(
    files: list[Path],
    output_path: Path,
    hash_cache: dict[tuple[str, int, int], str] | None = None,
) -> None:
    with output_path.open("w", encoding="utf-8", newline="") as fh:
        writer = csv.writer(fh, delimiter=" ")
        for file_path in files:
            writer.writerow([sha256_file(file_path, hash_cache), file_path.relative_to(output_path.parent).as_posix()])


def distance_meters(a: list[float], b: list[float]) -> float:
    lat1, lon1 = a
    lat2, lon2 = b
    lat1_rad = math.radians(lat1)
    lat2_rad = math.radians(lat2)
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    hav = (
        math.sin(dlat / 2) ** 2
        + math.cos(lat1_rad) * math.cos(lat2_rad) * math.sin(dlon / 2) ** 2
    )
    return 2 * 6_371_000 * math.asin(min(1.0, math.sqrt(hav)))


def split_route_waypoints(
    waypoints: list[list[float]], gap_meters: float = ROUTE_GAP_BREAK_METERS
) -> list[list[list[float]]]:
    if len(waypoints) < 2:
        return []

    segments: list[list[list[float]]] = []
    current = [waypoints[0]]

    for point in waypoints[1:]:
        if distance_meters(current[-1], point) > gap_meters:
            if len(current) >= 2:
                segments.append(current)
            current = [point]
            continue
        current.append(point)

    if len(current) >= 2:
        segments.append(current)

    return segments


def route_geometry_geojson(route: dict[str, Any]) -> dict[str, Any]:
    waypoints = json.loads(route["waypoints_json"])
    segments = split_route_waypoints(waypoints)
    if not segments and len(waypoints) >= 2:
        segments = [waypoints]

    if len(segments) <= 1:
        coords = [[pair[1], pair[0]] for pair in (segments[0] if segments else [])]
        return {"type": "LineString", "coordinates": coords}

    return {
        "type": "MultiLineString",
        "coordinates": [
            [[pair[1], pair[0]] for pair in segment]
            for segment in segments
        ],
    }


def route_waypoints_to_gpx(route: dict[str, Any]) -> str:
    waypoints = json.loads(route["waypoints_json"])
    segments = split_route_waypoints(waypoints)
    if not segments and len(waypoints) >= 2:
        segments = [waypoints]

    track_segments = []
    for segment_idx, segment in enumerate(segments):
        track_segments.append("    <trkseg>")
        for point_idx, pair in enumerate(segment):
            lat, lon = pair
            track_segments.append(
                f'      <trkpt lat="{lat}" lon="{lon}"><name>{route["reference_route_id"]}-{segment_idx}-{point_idx}</name></trkpt>'
            )
        track_segments.append("    </trkseg>")

    display_name = route["display_name"] or route["reference_route_id"]
    return "\n".join(
        [
            '<?xml version="1.0" encoding="UTF-8"?>',
            '<gpx version="1.1" creator="OpenInterstate" xmlns="http://www.topografix.com/GPX/1/1">',
            f"  <trk><name>{xml_escape(display_name)}</name>",
            *track_segments,
            "  </trk>",
            "</gpx>",
        ]
    )


def xml_escape(value: str) -> str:
    return (
        value.replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
        .replace('"', "&quot;")
        .replace("'", "&apos;")
    )


def write_reference_route_zip(routes: list[dict[str, Any]], output_zip: Path) -> None:
    with ZipFile(output_zip, "w", compression=ZIP_DEFLATED) as zf:
        for route in routes:
            route_id = route["reference_route_id"]
            zf.writestr(f"{route_id}.gpx", route_waypoints_to_gpx(route))


def write_example_geojson(rows: list[dict[str, Any]], output_path: Path) -> None:
    features = []
    for row in rows[:10]:
        geometry = json.loads(row["geometry_geojson"]) if row.get("geometry_geojson") else None
        properties = {k: v for k, v in row.items() if k != "geometry_geojson"}
        features.append({"type": "Feature", "geometry": geometry, "properties": properties})
    payload = {"type": "FeatureCollection", "features": features}
    output_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def build_manifest(
    release_id: str,
    output_dir: Path,
    files: list[Path],
    row_counts: dict[str, int],
    source_lineage: dict[str, Any],
    hash_cache: dict[tuple[str, int, int], str] | None = None,
) -> dict[str, Any]:
    return {
        "release_id": release_id,
        "release_date": str(date.today()),
        "schema_version": "v1",
        "source_lineage": source_lineage,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "files": [
            {
                "path": file_path.relative_to(output_dir).as_posix(),
                "format": file_path.suffix.lstrip("."),
                "sha256": sha256_file(file_path, hash_cache),
                "size_bytes": file_path.stat().st_size,
            }
            for file_path in files
        ],
        "table_row_counts": row_counts,
        "attribution": "Contains OpenStreetMap-derived data. Use with required OSM attribution and release-level licensing notes.",
    }


def main() -> None:
    args = parse_args()
    output_dir = Path(args.output_dir).resolve()
    state_dir = Path(args.state_dir).resolve() if args.state_dir else None
    csv_dir, gpx_dir, examples_dir = ensure_dirs(output_dir)
    source_pbf_path = Path(args.source_pbf_file).resolve() if args.source_pbf_file else None
    import_pbf_path = Path(args.import_pbf_file).resolve() if args.import_pbf_file else source_pbf_path
    hash_cache: dict[tuple[str, int, int], str] = {}

    if source_pbf_path is not None:
        source_pbf_metadata = build_source_file_metadata(source_pbf_path, state_dir, hash_cache)
    else:
        source_pbf_metadata = load_source_file_metadata(Path(args.source_pbf_metadata_file).resolve(), "source_pbf")
    assert import_pbf_path is not None
    import_pbf_metadata = build_source_file_metadata(import_pbf_path, state_dir, hash_cache)

    source_lineage = {
        "source_url": args.source_url,
        "source_pbf": source_pbf_metadata,
        "import_pbf": import_pbf_metadata,
        "derivation": [
            "osm2pgsql flex import via schema/osm2pgsql/openinterstate.lua",
            "schema/derive.sql",
            "openinterstate-derive graph, corridor, and reference-route builders",
        ],
    }

    specs = [
        ExportSpec(
            name="corridors",
            filename="corridors.csv",
            query=f"""
                SELECT
                  c.corridor_id,
                  c.highway AS interstate_name,
                  c.canonical_direction AS direction_code,
                  initcap(c.canonical_direction) AS direction_label,
                  ST_AsGeoJSON(ST_LineMerge(ST_Collect(he.geom))) AS geometry_geojson,
                  COUNT(he.id) AS edge_count
                FROM corridors c
                JOIN highway_edges he ON he.corridor_id = c.corridor_id
                WHERE c.highway ~ '{INTERSTATE_FILTER}'
                GROUP BY c.corridor_id, c.highway, c.canonical_direction
                ORDER BY c.highway, c.canonical_direction, c.corridor_id
            """,
        ),
        ExportSpec(
            name="corridor_edges",
            filename="corridor_edges.csv",
            query=f"""
                SELECT
                  he.id AS edge_id,
                  he.corridor_id,
                  c.highway AS interstate_name,
                  he.direction AS direction_code,
                  he.length_m,
                  ST_AsGeoJSON(he.geom) AS geometry_geojson
                FROM highway_edges he
                JOIN corridors c ON c.corridor_id = he.corridor_id
                WHERE c.highway ~ '{INTERSTATE_FILTER}'
                ORDER BY he.corridor_id, he.id
            """,
        ),
        ExportSpec(
            name="corridor_exits",
            filename="corridor_exits.csv",
            query=f"""
                SELECT
                  ce.exit_id,
                  ce.corridor_id,
                  c.highway AS interstate_name,
                  c.canonical_direction AS direction_code,
                  ce.corridor_index AS sequence_index,
                  ce.ref AS exit_number,
                  ce.name AS exit_name,
                  ce.lat,
                  ce.lon,
                  json_build_object('type', 'Point', 'coordinates', json_build_array(ce.lon, ce.lat))::text AS geometry_geojson
                FROM corridor_exits ce
                JOIN corridors c USING (corridor_id)
                WHERE c.highway ~ '{INTERSTATE_FILTER}'
                ORDER BY c.highway, c.canonical_direction, ce.corridor_index
            """,
        ),
        ExportSpec(
            name="exit_aliases",
            filename="exit_aliases.csv",
            query="""
                SELECT
                  canonical_id AS canonical_exit_id,
                  exit_id AS source_exit_id
                FROM canonical_exit_aliases
                ORDER BY canonical_id, exit_id
            """,
            columns=["canonical_exit_id", "source_exit_id"],
        ),
        ExportSpec(
            name="places",
            filename="places.csv",
            query=f"""
                SELECT DISTINCT
                  p.id AS place_id,
                  p.category,
                  p.name,
                  p.display_name,
                  p.brand,
                  ST_AsGeoJSON(p.geom) AS geometry_geojson
                FROM pois p
                JOIN exit_poi_candidates epc ON epc.poi_id = p.id
                JOIN corridor_exits ce ON ce.exit_id = epc.exit_id
                JOIN corridors c USING (corridor_id)
                WHERE c.highway ~ '{INTERSTATE_FILTER}'
                ORDER BY p.id
            """,
        ),
        ExportSpec(
            name="exit_place_links",
            filename="exit_place_links.csv",
            query=f"""
                SELECT
                  epc.exit_id,
                  epc.poi_id AS place_id,
                  epc.category,
                  epc.distance_m,
                  epc.rank
                FROM exit_poi_candidates epc
                JOIN corridor_exits ce ON ce.exit_id = epc.exit_id
                JOIN corridors c USING (corridor_id)
                WHERE c.highway ~ '{INTERSTATE_FILTER}'
                ORDER BY epc.exit_id, epc.poi_id
            """,
        ),
        ExportSpec(
            name="exit_place_scores",
            filename="exit_place_scores.csv",
            query=f"""
                SELECT
                  epr.exit_id,
                  epr.poi_id AS place_id,
                  epr.route_distance_m,
                  epr.route_duration_s,
                  epr.reachable,
                  epr.reachability_score,
                  epr.reachability_confidence,
                  epr.provider,
                  epr.provider_dataset_version,
                  epr.updated_at
                FROM exit_poi_reachability epr
                JOIN corridor_exits ce ON ce.exit_id = epr.exit_id
                JOIN corridors c USING (corridor_id)
                WHERE c.highway ~ '{INTERSTATE_FILTER}'
                ORDER BY epr.exit_id, epr.poi_id
            """,
        ),
        ExportSpec(
            name="reference_routes",
            filename="reference_routes.csv",
            query=f"""
                SELECT
                  id::text AS reference_route_id,
                  highway AS interstate_name,
                  direction_code,
                  direction_label,
                  display_name,
                  distance_m,
                  duration_s,
                  point_count,
                  waypoints_json
                FROM reference_routes
                WHERE highway ~ '{INTERSTATE_FILTER}'
                ORDER BY highway, direction_code, display_name
            """,
        ),
    ]

    row_counts: dict[str, int] = {}
    written_files: list[Path] = []

    with psycopg.connect(args.database_url) as conn:
        for spec in specs:
            columns, rows = fetch_rows(conn, spec.query)
            row_counts[spec.name] = len(rows)
            fieldnames = spec.columns or columns

            if spec.name == "reference_routes":
                for row in rows:
                    row["geometry_geojson"] = json.dumps(route_geometry_geojson(row))
                parquet_rows = [
                    {key: value for key, value in row.items() if key != "waypoints_json"} for row in rows
                ]
                write_csv(
                    parquet_rows,
                    [name for name in fieldnames if name != "waypoints_json"] + ["geometry_geojson"],
                    csv_dir / spec.filename,
                )
                write_reference_route_zip(rows, gpx_dir / "reference_routes.gpx.zip")
                written_files.append(csv_dir / spec.filename)
                written_files.append(gpx_dir / "reference_routes.gpx.zip")
                continue

            write_csv(rows, fieldnames, csv_dir / spec.filename)
            written_files.append(csv_dir / spec.filename)

            if spec.name == "corridors":
                write_example_geojson(rows, examples_dir / "sample_corridors.geojson")
                written_files.append(examples_dir / "sample_corridors.geojson")

        lineage_path = output_dir / "source_lineage.json"
        lineage_path.write_text(json.dumps(source_lineage, indent=2), encoding="utf-8")
        written_files.append(lineage_path)

        manifest = build_manifest(args.release_id, output_dir, written_files, row_counts, source_lineage, hash_cache)
        manifest_path = output_dir / "manifest.json"
        manifest_path.write_text(json.dumps(manifest, indent=2), encoding="utf-8")
        written_files.append(manifest_path)

        checksums_path = output_dir / "checksums.txt"
        write_checksums(written_files, checksums_path, hash_cache)


if __name__ == "__main__":
    main()
