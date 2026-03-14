import importlib.util
import sys
import unittest
from pathlib import Path


MODULE_PATH = Path(__file__).resolve().parents[1] / "extract_interstate_relations.py"
SPEC = importlib.util.spec_from_file_location("extract_interstate_relations", MODULE_PATH)
assert SPEC and SPEC.loader
MODULE = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = MODULE
SPEC.loader.exec_module(MODULE)


class ExtractInterstateRelationsTests(unittest.TestCase):
    def test_parse_relation_line_accepts_interstate_route(self) -> None:
        relation = MODULE.parse_relation_line(
            "r331325 v18 dV c0 t2021-10-01T02:32:58Z i0 u "
            "Tname=I%20%95%20%(super),network=US:I,ref=95,route=road,type=route "
            "Mr317707@,r338257@south,r338258@north"
        )

        self.assertIsNotNone(relation)
        assert relation is not None
        self.assertEqual(relation.relation_id, 331325)
        self.assertEqual(relation.ref, "I-95")
        self.assertEqual(relation.members[0].member_type, "r")
        self.assertEqual(relation.members[1].role, "south")

    def test_parse_relation_line_rejects_non_interstate_network(self) -> None:
        relation = MODULE.parse_relation_line(
            "r23147 v393 dV c0 t2026-03-05T11:41:12Z i0 u "
            "Tnetwork=US:US,ref=64,route=road,type=route Mw1@forward"
        )

        self.assertIsNone(relation)

    def test_flatten_relation_memberships_inherits_direction_from_children(self) -> None:
        relations = {
            1: MODULE.InterstateRelation(
                relation_id=1,
                ref="I-95",
                direction=None,
                members=[
                    MODULE.RelationMember("r", 2, "north"),
                    MODULE.RelationMember("r", 3, "south"),
                ],
            ),
            2: MODULE.InterstateRelation(
                relation_id=2,
                ref="I-95",
                direction="north",
                members=[MODULE.RelationMember("w", 101, "")],
            ),
            3: MODULE.InterstateRelation(
                relation_id=3,
                ref="I-95",
                direction="south",
                members=[MODULE.RelationMember("w", 202, "")],
            ),
        }

        rows = MODULE.flatten_relation_memberships(relations)

        self.assertEqual(
            rows,
            [
                (101, "I-95", 1, 2, "north", "", 0),
                (202, "I-95", 1, 3, "south", "", 0),
            ],
        )

    def test_flatten_relation_memberships_keeps_relation_root_for_direct_way_members(self) -> None:
        relations = {
            10: MODULE.InterstateRelation(
                relation_id=10,
                ref="I-10",
                direction="west",
                members=[MODULE.RelationMember("w", 999, "")],
            )
        }

        rows = MODULE.flatten_relation_memberships(relations)

        self.assertEqual(rows, [(999, "I-10", 10, 10, "west", "", 0)])


if __name__ == "__main__":
    unittest.main()
