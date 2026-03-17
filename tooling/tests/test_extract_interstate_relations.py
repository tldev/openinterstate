import io
import sys
import unittest
from contextlib import redirect_stderr
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from tooling.extract_interstate_relations import (  # noqa: E402
    InterstateRelation,
    RelationMember,
    flatten_relation_memberships,
    normalize_direction,
)


def way_member(way_id: int, role: str = "") -> RelationMember:
    return RelationMember(member_type="w", member_id=way_id, role=role)


def relation_member(relation_id: int, role: str = "") -> RelationMember:
    return RelationMember(member_type="r", member_id=relation_id, role=role)


def relation(
    relation_id: int,
    ref: str,
    *,
    direction: str | None = None,
    name: str | None = None,
    description: str | None = None,
    members: list[RelationMember] | None = None,
) -> InterstateRelation:
    return InterstateRelation(
        relation_id=relation_id,
        ref=ref,
        direction=direction,
        name=name,
        description=description,
        members=members or [],
    )


def flatten_rows(
    relations: dict[int, InterstateRelation],
) -> tuple[dict[int, tuple[int, str, int, int, str, str, int]], str]:
    stderr = io.StringIO()
    with redirect_stderr(stderr):
        rows = flatten_relation_memberships(relations)
    return {row[0]: row for row in rows}, stderr.getvalue()


class ExtractInterstateRelationsTests(unittest.TestCase):
    def test_normalize_direction_accepts_rich_cardinal_variants(self) -> None:
        self.assertEqual(normalize_direction("south (local)"), "south")
        self.assertEqual(normalize_direction("north (thru)"), "north")
        self.assertEqual(normalize_direction("South"), "south")
        self.assertEqual(normalize_direction("eastbound"), "east")
        self.assertEqual(normalize_direction("westbound"), "west")
        self.assertIsNone(normalize_direction("forward"))

    def test_rule1_infers_direction_from_title_when_root_axis_matches(self) -> None:
        rows_by_way, _ = flatten_rows(
            {
                100: relation(
                    100,
                    "I-30",
                    members=[
                        relation_member(101),
                        relation_member(102),
                        relation_member(103),
                    ],
                ),
                101: relation(
                    101,
                    "I-30",
                    name="I 30 (AR) (East)",
                    members=[way_member(1), way_member(2, "forward")],
                ),
                102: relation(102, "I-30", direction="east", members=[way_member(3)]),
                103: relation(103, "I-30", direction="west", members=[way_member(4)]),
            }
        )

        self.assertEqual(rows_by_way[1][4], "east")
        self.assertEqual(rows_by_way[2][4], "east")

    def test_rule1_does_not_infer_wrong_axis_title_direction(self) -> None:
        rows_by_way, warnings = flatten_rows(
            {
                100: relation(
                    100,
                    "I-30",
                    members=[
                        relation_member(101),
                        relation_member(102),
                        relation_member(103),
                    ],
                ),
                101: relation(
                    101,
                    "I-30",
                    name="I 30 Spur (North)",
                    members=[way_member(1)],
                ),
                102: relation(102, "I-30", direction="east", members=[way_member(3)]),
                103: relation(103, "I-30", direction="west", members=[way_member(4)]),
            }
        )

        self.assertEqual(rows_by_way[1][4], "")
        self.assertIn("warning: unresolved directional Interstate relation root I-30", warnings)

    def test_rule2_propagates_seeded_direction_to_forward_and_blank_members(self) -> None:
        rows_by_way, _ = flatten_rows(
            {
                200: relation(
                    200,
                    "I-435",
                    members=[
                        relation_member(201),
                        relation_member(202),
                        relation_member(203),
                    ],
                ),
                201: relation(
                    201,
                    "I-435",
                    description="I 435 (KS/MO) (clockwise)",
                    members=[
                        way_member(10, "south"),
                        way_member(11, "forward"),
                        way_member(12),
                    ],
                ),
                202: relation(202, "I-435", direction="north", members=[way_member(20)]),
                203: relation(203, "I-435", direction="south", members=[way_member(21)]),
            }
        )

        self.assertEqual(rows_by_way[10][4], "south")
        self.assertEqual(rows_by_way[11][4], "south")
        self.assertEqual(rows_by_way[12][4], "south")

    def test_rule2_leaves_all_forward_leaf_unresolved_without_seed(self) -> None:
        rows_by_way, warnings = flatten_rows(
            {
                300: relation(
                    300,
                    "I-41",
                    members=[
                        relation_member(301),
                        relation_member(302),
                        relation_member(303),
                    ],
                ),
                301: relation(
                    301,
                    "I-41",
                    description="I 41 (WI)",
                    members=[way_member(30, "forward"), way_member(31, "forward")],
                ),
                302: relation(302, "I-41", direction="north", members=[way_member(32)]),
                303: relation(303, "I-41", direction="south", members=[way_member(33)]),
            }
        )

        self.assertEqual(rows_by_way[30][4], "")
        self.assertEqual(rows_by_way[31][4], "")
        self.assertIn("root=300", warnings)
        self.assertIn("leafs=301", warnings)
        self.assertIn("blank_members=2", warnings)

    def test_rule2_does_not_propagate_when_leaf_has_conflicting_seeds(self) -> None:
        rows_by_way, warnings = flatten_rows(
            {
                400: relation(
                    400,
                    "I-95",
                    members=[
                        relation_member(401),
                        relation_member(402),
                        relation_member(403),
                    ],
                ),
                401: relation(
                    401,
                    "I-95",
                    members=[
                        way_member(40, "north"),
                        way_member(41, "south"),
                        way_member(42, "forward"),
                    ],
                ),
                402: relation(402, "I-95", direction="north", members=[way_member(43)]),
                403: relation(403, "I-95", direction="south", members=[way_member(44)]),
            }
        )

        self.assertEqual(rows_by_way[40][4], "north")
        self.assertEqual(rows_by_way[41][4], "south")
        self.assertEqual(rows_by_way[42][4], "")
        self.assertIn("root=400", warnings)

    def test_relation_direction_overrides_conflicting_member_roles(self) -> None:
        rows_by_way, _ = flatten_rows(
            {
                500: relation(
                    500,
                    "I-12",
                    direction="east",
                    members=[
                        way_member(50, "west"),
                        way_member(51, "east"),
                        way_member(52, "forward"),
                        way_member(53),
                    ],
                ),
            }
        )

        self.assertEqual(rows_by_way[50][4], "east")
        self.assertEqual(rows_by_way[51][4], "east")
        self.assertEqual(rows_by_way[52][4], "east")
        self.assertEqual(rows_by_way[53][4], "east")

    def test_collapses_subsumed_same_ref_root_memberships(self) -> None:
        relations = {
            100: relation(
                100,
                "I-10",
                members=[relation_member(101), relation_member(102)],
            ),
            200: relation(
                200,
                "I-10",
                members=[
                    relation_member(101),
                    relation_member(102),
                    relation_member(103),
                ],
            ),
            101: relation(101, "I-10", direction="east", members=[way_member(1)]),
            102: relation(102, "I-10", direction="west", members=[way_member(2)]),
            103: relation(103, "I-10", direction="east", members=[way_member(3)]),
        }

        stderr = io.StringIO()
        with redirect_stderr(stderr):
            rows = flatten_relation_memberships(relations)

        self.assertEqual({row[0]: row[2] for row in rows}, {1: 200, 2: 200, 3: 200})
        self.assertEqual({row[2] for row in rows}, {200})

    def test_keeps_same_ref_roots_separate_when_neither_is_subset(self) -> None:
        relations = {
            300: relation(
                300,
                "I-10",
                members=[relation_member(301), relation_member(302)],
            ),
            400: relation(
                400,
                "I-10",
                members=[relation_member(302), relation_member(303)],
            ),
            301: relation(301, "I-10", direction="east", members=[way_member(10)]),
            302: relation(302, "I-10", direction="west", members=[way_member(11)]),
            303: relation(303, "I-10", direction="east", members=[way_member(12)]),
        }

        stderr = io.StringIO()
        with redirect_stderr(stderr):
            rows = flatten_relation_memberships(relations)

        root_ids_by_way: dict[int, set[int]] = {}
        for way_id, _ref, root_relation_id, *_rest in rows:
            root_ids_by_way.setdefault(way_id, set()).add(root_relation_id)

        self.assertEqual(root_ids_by_way[10], {300})
        self.assertEqual(root_ids_by_way[11], {300, 400})
        self.assertEqual(root_ids_by_way[12], {400})


if __name__ == "__main__":
    unittest.main()
