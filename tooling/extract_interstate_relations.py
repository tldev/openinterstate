#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
from collections import defaultdict
import re
import subprocess
import sys
import urllib.parse
from dataclasses import dataclass
from pathlib import Path


INTERSTATE_NETWORK = "US:I"
INTERSTATE_REF_RE = re.compile(r"^I?[\s-]*(\d+[A-Z]?)$")
CARDINAL_DIRECTIONS = {"north", "south", "east", "west"}
BOUND_CARDINAL_RE = re.compile(
    r"^(north|south|east|west)(?:bound)?(?:\b|[^a-z])",
    re.IGNORECASE,
)
FLOW_ROLE_RE = re.compile(r"^(forward|backward)(?:\b|[^a-z])", re.IGNORECASE)
TITLE_CARDINAL_RE = re.compile(r"\b(north|south|east|west)\b", re.IGNORECASE)
NORTH_SOUTH_AXIS = frozenset({"north", "south"})
EAST_WEST_AXIS = frozenset({"east", "west"})


@dataclass(frozen=True)
class RelationMember:
    member_type: str
    member_id: int
    role: str


@dataclass
class InterstateRelation:
    relation_id: int
    ref: str
    direction: str | None
    name: str | None
    description: str | None
    members: list[RelationMember]


@dataclass(frozen=True)
class RootMembership:
    relation_id: int
    way_id: int
    direction: str


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Extract cached Interstate route relation memberships from a source PBF."
    )
    parser.add_argument("--source-pbf", required=True)
    parser.add_argument("--output", required=True)
    return parser.parse_args(argv)


def normalize_interstate_ref(raw: str | None) -> str | None:
    if raw is None:
        return None
    match = INTERSTATE_REF_RE.fullmatch(raw.strip().upper())
    if not match:
        return None
    return f"I-{match.group(1)}"


def normalize_direction(raw: str | None) -> str | None:
    if raw is None:
        return None
    value = urllib.parse.unquote(raw).strip().lower()
    if not value:
        return None
    aliases = {
        "north": "north",
        "northbound": "north",
        "n": "north",
        "south": "south",
        "southbound": "south",
        "s": "south",
        "east": "east",
        "eastbound": "east",
        "e": "east",
        "west": "west",
        "westbound": "west",
        "w": "west",
    }
    if value in aliases:
        return aliases[value]
    match = BOUND_CARDINAL_RE.match(value)
    if match:
        return match.group(1).lower()
    return None


def normalize_flow_role(raw: str | None) -> str | None:
    if raw is None:
        return None
    value = urllib.parse.unquote(raw).strip().lower()
    if not value:
        return None
    match = FLOW_ROLE_RE.match(value)
    if match:
        return match.group(1).lower()
    return None


def parse_tags(raw: str) -> dict[str, str]:
    if not raw:
        return {}

    tags: dict[str, str] = {}
    for entry in raw.split(","):
        key, sep, value = entry.partition("=")
        if not sep:
            continue
        tags[key] = urllib.parse.unquote(value)
    return tags


def parse_members(raw: str) -> list[RelationMember]:
    if not raw:
        return []

    members: list[RelationMember] = []
    for entry in raw.split(","):
        if not entry:
            continue

        member_type = entry[0]
        if member_type not in {"w", "r"}:
            continue

        body = entry[1:]
        member_id_raw, _, role = body.partition("@")
        if not member_id_raw.isdigit():
            continue

        members.append(
            RelationMember(
                member_type=member_type,
                member_id=int(member_id_raw),
                role=urllib.parse.unquote(role),
            )
        )
    return members


def parse_relation_line(line: str) -> InterstateRelation | None:
    if not line.startswith("r"):
        return None

    prefix, _, remainder = line.partition(" T")
    if not remainder:
        return None

    relation_token = prefix.split(" ", 1)[0]
    if not relation_token[1:].isdigit():
        return None
    relation_id = int(relation_token[1:])

    tags_raw, _, members_raw = remainder.partition(" M")
    tags = parse_tags(tags_raw)
    if tags.get("network") != INTERSTATE_NETWORK or tags.get("route") != "road":
        return None

    ref = normalize_interstate_ref(tags.get("ref"))
    if ref is None:
        return None

    return InterstateRelation(
        relation_id=relation_id,
        ref=ref,
        direction=normalize_direction(tags.get("direction")),
        name=tags.get("name"),
        description=tags.get("description"),
        members=parse_members(members_raw),
    )


def load_interstate_relations(source_pbf: Path) -> dict[int, InterstateRelation]:
    relations: dict[int, InterstateRelation] = {}
    process = subprocess.Popen(
        ["osmium", "cat", "-t", "relation", str(source_pbf), "-f", "opl"],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    assert process.stdout is not None

    try:
        for line in process.stdout:
            relation = parse_relation_line(line.rstrip("\n"))
            if relation is None:
                continue
            relations[relation.relation_id] = relation
    finally:
        process.stdout.close()

    stderr = ""
    if process.stderr is not None:
        stderr = process.stderr.read()
        process.stderr.close()

    return_code = process.wait()
    if return_code != 0:
        raise RuntimeError(f"osmium cat failed with exit code {return_code}: {stderr.strip()}")

    return relations


def referenced_relation_ids(relations: dict[int, InterstateRelation]) -> set[int]:
    referenced: set[int] = set()
    for relation in relations.values():
        for member in relation.members:
            if member.member_type == "r" and member.member_id in relations:
                referenced.add(member.member_id)
    return referenced


def effective_direction(
    relation_direction: str | None,
    leaf_default_direction: str | None,
    member_role: str,
    inherited_direction: str | None,
) -> str | None:
    if relation_direction is not None:
        return relation_direction
    role_direction = normalize_direction(member_role)
    if role_direction is not None:
        return role_direction
    flow_role = normalize_flow_role(member_role)
    role_value = urllib.parse.unquote(member_role).strip()
    if leaf_default_direction is not None and (
        not role_value or flow_role == "forward"
    ):
        return leaf_default_direction
    return inherited_direction


def matching_axis(directions: set[str]) -> frozenset[str] | None:
    if NORTH_SOUTH_AXIS.issubset(directions) and directions.issubset(NORTH_SOUTH_AXIS):
        return NORTH_SOUTH_AXIS
    if EAST_WEST_AXIS.issubset(directions) and directions.issubset(EAST_WEST_AXIS):
        return EAST_WEST_AXIS
    return None


def root_relation_ids(relations: dict[int, InterstateRelation]) -> list[int]:
    return sorted(set(relations) - referenced_relation_ids(relations))


def same_ref_membership_signature(
    relations: dict[int, InterstateRelation],
    root_relation_id: int,
) -> frozenset[tuple[int, str]]:
    subtree_relation_ids: set[int] = set()

    def collect_subtree(relation_id: int, stack: set[int]) -> None:
        if relation_id in stack or relation_id in subtree_relation_ids:
            return

        subtree_relation_ids.add(relation_id)
        relation = relations[relation_id]
        next_stack = set(stack)
        next_stack.add(relation_id)
        for member in relation.members:
            if member.member_type != "r" or member.member_id not in relations:
                continue
            child = relations[member.member_id]
            if child.ref != relation.ref:
                continue
            collect_subtree(child.relation_id, next_stack)

    collect_subtree(root_relation_id, set())
    subtree_relations = {
        relation_id: relations[relation_id] for relation_id in subtree_relation_ids
    }
    relation_to_root = {
        relation_id: root_relation_id for relation_id in subtree_relation_ids
    }
    root_axes = infer_root_axes(subtree_relations, relation_to_root)
    leaf_default_directions = infer_leaf_default_directions(
        subtree_relations, relation_to_root, root_axes
    )
    memberships: set[tuple[int, str]] = set()

    def visit(
        relation_id: int,
        inherited_direction: str | None,
        stack: set[int],
    ) -> None:
        if relation_id in stack:
            return

        relation = subtree_relations[relation_id]
        next_stack = set(stack)
        next_stack.add(relation_id)
        for member in relation.members:
            direction = effective_direction(
                relation.direction,
                leaf_default_directions.get(relation_id),
                member.role,
                inherited_direction,
            )
            if member.member_type == "w":
                memberships.add((member.member_id, direction or ""))
                continue

            if member.member_type == "r" and member.member_id in subtree_relations:
                child = subtree_relations[member.member_id]
                if child.ref != relation.ref:
                    continue
                visit(member.member_id, direction, next_stack)

    visit(root_relation_id, relations[root_relation_id].direction, set())
    return frozenset(memberships)


def collapse_subsumed_root_aliases(
    relations: dict[int, InterstateRelation],
    roots: list[int],
) -> dict[int, int]:
    aliases = {root_relation_id: root_relation_id for root_relation_id in roots}
    signatures = {
        root_relation_id: same_ref_membership_signature(relations, root_relation_id)
        for root_relation_id in roots
    }
    roots_by_ref: dict[str, list[int]] = defaultdict(list)
    for root_relation_id in roots:
        roots_by_ref[relations[root_relation_id].ref].append(root_relation_id)

    def directed_only(sig: frozenset[tuple[int, str]]) -> frozenset[tuple[int, str]]:
        has_directed = any(d for _, d in sig)
        if not has_directed:
            return sig
        return frozenset((w, d) for w, d in sig if d)

    for root_ids in roots_by_ref.values():
        for root_relation_id in sorted(
            root_ids,
            key=lambda candidate: (len(signatures[candidate]), candidate),
        ):
            signature = directed_only(signatures[root_relation_id])
            candidates: list[int] = []
            for candidate_root_id in root_ids:
                if candidate_root_id == root_relation_id:
                    continue

                candidate_signature = directed_only(signatures[candidate_root_id])
                if not signature.issubset(candidate_signature):
                    continue

                if (
                    signature == candidate_signature
                    and candidate_root_id > root_relation_id
                ):
                    continue

                candidates.append(candidate_root_id)

            if candidates:
                aliases[root_relation_id] = min(
                    candidates,
                    key=lambda candidate: (len(signatures[candidate]), candidate),
                )

    def canonical_root(root_relation_id: int) -> int:
        alias = aliases[root_relation_id]
        if alias == root_relation_id:
            return alias
        resolved = canonical_root(alias)
        aliases[root_relation_id] = resolved
        return resolved

    for root_relation_id in roots:
        aliases[root_relation_id] = canonical_root(root_relation_id)

    return aliases


def assign_roots(relations: dict[int, InterstateRelation]) -> dict[int, int]:
    relation_to_root: dict[int, int] = {}
    roots = root_relation_ids(relations)
    root_aliases = collapse_subsumed_root_aliases(relations, roots)

    def visit(relation_id: int, root_relation_id: int, stack: set[int]) -> None:
        if relation_id in stack:
            return
        relation_to_root.setdefault(relation_id, root_relation_id)
        relation = relations[relation_id]
        next_stack = set(stack)
        next_stack.add(relation_id)
        for member in relation.members:
            if member.member_type != "r" or member.member_id not in relations:
                continue
            child = relations[member.member_id]
            if child.ref != relation.ref:
                continue
            visit(child.relation_id, root_relation_id, next_stack)

    for root_relation_id in sorted(
        roots,
        key=lambda candidate: (root_aliases[candidate] != candidate, candidate),
    ):
        visit(root_relation_id, root_aliases[root_relation_id], set())

    for relation_id in sorted(relations):
        if relation_id not in relation_to_root:
            visit(relation_id, relation_id, set())

    return relation_to_root


def infer_root_axes(
    relations: dict[int, InterstateRelation],
    relation_to_root: dict[int, int],
) -> dict[int, frozenset[str]]:
    directions_by_root: dict[int, set[str]] = defaultdict(set)
    for relation_id, relation in relations.items():
        root_relation_id = relation_to_root.get(relation_id, relation_id)
        if relation.direction is not None:
            directions_by_root[root_relation_id].add(relation.direction)
        for member in relation.members:
            if member.member_type != "w":
                continue
            direction = normalize_direction(member.role)
            if direction is not None:
                directions_by_root[root_relation_id].add(direction)

    axes: dict[int, frozenset[str]] = {}
    for root_relation_id, directions in directions_by_root.items():
        axis = matching_axis(directions)
        if axis is not None:
            axes[root_relation_id] = axis
    return axes


def extract_title_direction(
    relation: InterstateRelation,
    root_axis: frozenset[str] | None,
) -> str | None:
    if root_axis is None:
        return None
    text = " ".join(
        value for value in (relation.name, relation.description) if value is not None
    )
    if not text:
        return None
    matches = {match.group(1).lower() for match in TITLE_CARDINAL_RE.finditer(text)}
    if len(matches) != 1:
        return None
    direction = next(iter(matches))
    if direction in root_axis:
        return direction
    return None


def infer_leaf_default_directions(
    relations: dict[int, InterstateRelation],
    relation_to_root: dict[int, int],
    root_axes: dict[int, frozenset[str]],
) -> dict[int, str]:
    inferred: dict[int, str] = {}
    for relation_id, relation in relations.items():
        if relation.direction is not None:
            continue

        root_relation_id = relation_to_root.get(relation_id, relation_id)
        root_axis = root_axes.get(root_relation_id)
        if root_axis is None:
            continue

        title_direction = extract_title_direction(relation, root_axis)
        if title_direction is not None:
            inferred[relation_id] = title_direction
            continue

        seed_directions: set[str] = set()
        invalid_member_roles = False
        for member in relation.members:
            if member.member_type != "w":
                continue

            member_direction = normalize_direction(member.role)
            if member_direction is not None:
                if member_direction not in root_axis:
                    invalid_member_roles = True
                    break
                seed_directions.add(member_direction)
                continue

            flow_role = normalize_flow_role(member.role)
            role_value = urllib.parse.unquote(member.role).strip()
            if role_value and flow_role not in {None, "forward"}:
                invalid_member_roles = True
                break

        if invalid_member_roles or len(seed_directions) != 1:
            continue

        inferred[relation_id] = next(iter(seed_directions))

    return inferred


def flatten_relation_memberships(
    relations: dict[int, InterstateRelation],
) -> list[tuple[int, str, int, int, str, str, int]]:
    rows: list[tuple[int, str, int, int, str, str, int]] = []
    relation_to_root = assign_roots(relations)
    root_axes = infer_root_axes(relations, relation_to_root)
    leaf_default_directions = infer_leaf_default_directions(
        relations, relation_to_root, root_axes
    )
    roots = root_relation_ids(relations)
    root_aliases = collapse_subsumed_root_aliases(relations, roots)
    canonical_roots = sorted(
        root_relation_id
        for root_relation_id in roots
        if root_aliases[root_relation_id] == root_relation_id
    )
    visited_relations: set[int] = set()
    sequence_by_group: dict[tuple[int, str], int] = defaultdict(int)
    unresolved_by_root: dict[tuple[str, int], dict[str, object]] = defaultdict(
        lambda: {"leaf_relation_ids": set(), "blank_members": 0}
    )

    def visit(
        relation_id: int,
        root_relation_id: int,
        inherited_direction: str | None,
        stack: set[int],
    ) -> None:
        if relation_id in stack:
            return

        relation = relations[relation_id]
        visited_relations.add(relation_id)
        stack = set(stack)
        stack.add(relation_id)

        for member in relation.members:
            direction = effective_direction(
                relation.direction,
                leaf_default_directions.get(relation_id),
                member.role,
                inherited_direction,
            )
            if member.member_type == "w":
                direction_key = direction or ""
                sequence_index = sequence_by_group[(root_relation_id, direction_key)]
                sequence_by_group[(root_relation_id, direction_key)] += 1
                if not direction_key and root_relation_id in root_axes:
                    unresolved = unresolved_by_root[(relation.ref, root_relation_id)]
                    leaf_relation_ids = unresolved["leaf_relation_ids"]
                    assert isinstance(leaf_relation_ids, set)
                    leaf_relation_ids.add(relation_id)
                    blank_members = unresolved["blank_members"]
                    assert isinstance(blank_members, int)
                    unresolved["blank_members"] = blank_members + 1
                rows.append(
                    (
                        member.member_id,
                        relation.ref,
                        root_relation_id,
                        relation_id,
                        direction_key,
                        member.role,
                        sequence_index,
                    )
                )
                continue

            if member.member_type == "r" and member.member_id in relations:
                child = relations[member.member_id]
                if child.ref != relation.ref:
                    continue
                visit(member.member_id, root_relation_id, direction, stack)

    for root_relation_id in canonical_roots:
        visit(root_relation_id, root_relation_id, relations[root_relation_id].direction, set())

    for relation_id in sorted(relations):
        if relation_id not in visited_relations:
            root_relation_id = relation_to_root.get(relation_id, relation_id)
            if root_relation_id != relation_id:
                continue
            visit(relation_id, relation_id, relations[relation_id].direction, set())

    for (highway, root_relation_id), unresolved in sorted(unresolved_by_root.items()):
        leaf_relation_ids = unresolved["leaf_relation_ids"]
        blank_members = unresolved["blank_members"]
        assert isinstance(leaf_relation_ids, set)
        assert isinstance(blank_members, int)
        leafs_text = ",".join(str(leaf_id) for leaf_id in sorted(leaf_relation_ids))
        print(
            "warning: unresolved directional Interstate relation root "
            f"{highway} root={root_relation_id} "
            f"leafs={leafs_text or '(none)'} blank_members={blank_members}",
            file=sys.stderr,
        )

    return rows


def write_rows(rows: list[tuple[int, str, int, int, str, str, int]], output: Path) -> None:
    output.parent.mkdir(parents=True, exist_ok=True)
    with output.open("w", encoding="utf-8", newline="") as fh:
        writer = csv.writer(fh, delimiter="\t")
        writer.writerow(
            [
                "way_id",
                "ref",
                "root_relation_id",
                "leaf_relation_id",
                "direction",
                "role",
                "sequence_index",
            ]
        )
        writer.writerows(rows)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    source_pbf = Path(args.source_pbf).resolve()
    output = Path(args.output).resolve()

    relations = load_interstate_relations(source_pbf)
    rows = flatten_relation_memberships(relations)
    write_rows(rows, output)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
