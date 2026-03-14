import importlib.util
import json
import sys
import tempfile
import types
import unittest
from pathlib import Path


MODULE_PATH = Path(__file__).resolve().parents[1] / "export_release.py"
SPEC = importlib.util.spec_from_file_location("export_release", MODULE_PATH)
assert SPEC and SPEC.loader
MODULE = importlib.util.module_from_spec(SPEC)
PSYCOPG_STUB = types.ModuleType("psycopg")
PSYCOPG_STUB.Connection = object
sys.modules["psycopg"] = PSYCOPG_STUB
sys.modules[SPEC.name] = MODULE
SPEC.loader.exec_module(MODULE)


class ReleaseInterstateNameTests(unittest.TestCase):
    def test_plain_numeric_interstates_are_included(self) -> None:
        self.assertTrue(MODULE.is_release_interstate_name("I-95"))
        self.assertTrue(MODULE.is_release_interstate_name("I95"))

    def test_official_lettered_branches_are_included(self) -> None:
        for highway in ("I-35E", "I-35W", "I-69C", "I-69E", "I-69W"):
            with self.subTest(highway=highway):
                self.assertTrue(MODULE.is_release_interstate_name(highway))

    def test_non_route_labels_are_excluded(self) -> None:
        for highway in ("I-405 Express Toll Lanes", "I-80U", "I-80E", "I-80W", "I-480N"):
            with self.subTest(highway=highway):
                self.assertFalse(MODULE.is_release_interstate_name(highway))


class SourceFileMetadataTests(unittest.TestCase):
    def test_parse_args_accepts_source_metadata_with_import_file(self) -> None:
        args = MODULE.parse_args(
            [
                "--database-url",
                "postgres://db",
                "--release-id",
                "release-2026-03-12",
                "--output-dir",
                "/tmp/release",
                "--source-pbf-metadata-file",
                "/tmp/source.json",
                "--import-pbf-file",
                "/tmp/import.osm.pbf",
            ]
        )
        self.assertEqual(args.source_pbf_metadata_file, "/tmp/source.json")
        self.assertEqual(args.import_pbf_file, "/tmp/import.osm.pbf")

    def test_parse_args_rejects_missing_source_locator(self) -> None:
        with self.assertRaises(SystemExit):
            MODULE.parse_args(
                [
                    "--database-url",
                    "postgres://db",
                    "--release-id",
                    "release-2026-03-12",
                    "--output-dir",
                    "/tmp/release",
                ]
            )

    def test_parse_args_rejects_metadata_without_import_file(self) -> None:
        with self.assertRaises(SystemExit):
            MODULE.parse_args(
                [
                    "--database-url",
                    "postgres://db",
                    "--release-id",
                    "release-2026-03-12",
                    "--output-dir",
                    "/tmp/release",
                    "--source-pbf-metadata-file",
                    "/tmp/source.json",
                ]
            )

    def test_load_source_file_metadata_accepts_streamed_source_locator(self) -> None:
        metadata = {
            "path": "https://download.geofabrik.de/north-america/us-latest.osm.pbf",
            "filename": "us-latest.osm.pbf",
            "size_bytes": 123,
            "modified_at": "2026-03-12T00:00:00+00:00",
            "sha256": "a" * 64,
        }
        with tempfile.TemporaryDirectory() as tmpdir:
            metadata_path = Path(tmpdir) / "source.json"
            metadata_path.write_text(json.dumps(metadata), encoding="utf-8")
            loaded = MODULE.load_source_file_metadata(metadata_path, "source_pbf")
        self.assertEqual(loaded, metadata)

    def test_load_source_file_metadata_rejects_bad_sha256(self) -> None:
        metadata = {
            "path": "streamed://us-latest.osm.pbf",
            "filename": "us-latest.osm.pbf",
            "size_bytes": 123,
            "modified_at": "2026-03-12T00:00:00+00:00",
            "sha256": "xyz",
        }
        with tempfile.TemporaryDirectory() as tmpdir:
            metadata_path = Path(tmpdir) / "source.json"
            metadata_path.write_text(json.dumps(metadata), encoding="utf-8")
            with self.assertRaisesRegex(ValueError, "sha256"):
                MODULE.load_source_file_metadata(metadata_path, "source_pbf")


class RouteGeometryTests(unittest.TestCase):
    def test_route_geometry_splits_large_gaps_into_multilinestring(self) -> None:
        route = {
            "waypoints_json": json.dumps(
                [
                    [32.0, -117.0],
                    [32.01, -117.01],
                    [40.0, -75.0],
                    [40.01, -75.01],
                ]
            )
        }

        geometry = MODULE.route_geometry_geojson(route)

        self.assertEqual(geometry["type"], "MultiLineString")
        self.assertEqual(len(geometry["coordinates"]), 2)

    def test_route_geometry_honors_explicit_segmented_waypoints(self) -> None:
        route = {
            "waypoints_json": json.dumps(
                [
                    [[32.0, -117.0], [32.01, -117.01]],
                    [[40.0, -75.0], [40.01, -75.01]],
                ]
            )
        }

        geometry = MODULE.route_geometry_geojson(route)

        self.assertEqual(geometry["type"], "MultiLineString")
        self.assertEqual(len(geometry["coordinates"]), 2)

    def test_route_waypoints_to_gpx_emits_multiple_track_segments(self) -> None:
        route = {
            "reference_route_id": "route-1",
            "display_name": "I-TEST Northbound",
            "waypoints_json": json.dumps(
                [
                    [32.0, -117.0],
                    [32.01, -117.01],
                    [40.0, -75.0],
                    [40.01, -75.01],
                ]
            ),
        }

        gpx = MODULE.route_waypoints_to_gpx(route)

        self.assertEqual(gpx.count("<trkseg>"), 2)
        self.assertIn("route-1-0-0", gpx)
        self.assertIn("route-1-1-1", gpx)

    def test_route_waypoints_to_gpx_preserves_explicit_segments(self) -> None:
        route = {
            "reference_route_id": "route-2",
            "display_name": "I-TEST Southbound",
            "waypoints_json": json.dumps(
                [
                    [[32.0, -117.0], [32.01, -117.01]],
                    [[40.0, -75.0], [40.01, -75.01]],
                ]
            ),
        }

        gpx = MODULE.route_waypoints_to_gpx(route)

        self.assertEqual(gpx.count("<trkseg>"), 2)
        self.assertIn("route-2-0-0", gpx)
        self.assertIn("route-2-1-1", gpx)


if __name__ == "__main__":
    unittest.main()
