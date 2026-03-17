import sys
import types
import unittest
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

sys.modules.setdefault("psycopg", types.SimpleNamespace(Connection=object))

from tooling.export_release import build_export_specs  # noqa: E402


class ExportReleaseTests(unittest.TestCase):
    def test_corridor_edges_direction_code_uses_corridor_canonical_direction(self) -> None:
        specs = {spec.name: spec for spec in build_export_specs()}

        corridor_edges_query = specs["corridor_edges"].query

        self.assertIn("c.canonical_direction AS direction_code", corridor_edges_query)
        self.assertNotIn("he.direction AS direction_code", corridor_edges_query)


if __name__ == "__main__":
    unittest.main()
