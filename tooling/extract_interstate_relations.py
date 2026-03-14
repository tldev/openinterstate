#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
from collections import defaultdict
import re
import subprocess
import urllib.parse
from dataclasses import dataclass
from pathlib import Path


INTERSTATE_NETWORK = "US:I"
INTERSTATE_REF_RE = re.compile(r"^I?[\s-]*(\d+[A-Z]?)$")
CARDINAL_DIRECTIONS = {"north", "south", "east", "west"}


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
    members: list[RelationMember]


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
    if value in CARDINAL_DIRECTIONS:
        return value
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
    member_role: str,
    inherited_direction: str | None,
) -> str | None:
    role_direction = normalize_direction(member_role)
    if role_direction is not None:
        return role_direction
    if relation_direction is not None:
        return relation_direction
    return inherited_direction


def flatten_relation_memberships(
    relations: dict[int, InterstateRelation],
) -> list[tuple[int, str, int, int, str, str, int]]:
    rows: list[tuple[int, str, int, int, str, str, int]] = []
    roots = sorted(set(relations) - referenced_relation_ids(relations))
    visited_relations: set[int] = set()
    sequence_by_group: dict[tuple[int, str], int] = defaultdict(int)

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
            direction = effective_direction(relation.direction, member.role, inherited_direction)
            if member.member_type == "w":
                direction_key = direction or ""
                sequence_index = sequence_by_group[(root_relation_id, direction_key)]
                sequence_by_group[(root_relation_id, direction_key)] += 1
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

    for root_relation_id in roots:
        visit(root_relation_id, root_relation_id, relations[root_relation_id].direction, set())

    for relation_id in sorted(relations):
        if relation_id not in visited_relations:
            visit(relation_id, relation_id, relations[relation_id].direction, set())

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
