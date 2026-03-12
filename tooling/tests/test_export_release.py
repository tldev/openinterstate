import importlib.util
import sys
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


if __name__ == "__main__":
    unittest.main()
