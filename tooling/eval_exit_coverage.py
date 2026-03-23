#!/usr/bin/env python3
"""Evaluate OI corridor exit coverage against ground truth ground truth.

Produces a single score (ground truth exact match %) and checks three hard gates:
  1. Corridor count >= minimum (default 262)
  2. Zero regressions against a saved baseline
  3. cargo test must have passed (checked externally)

Usage:
    python3 tooling/eval_exit_coverage.py [--save-baseline] [--baseline PATH]

Exit codes:
    0  All gates passed
    1  A gate failed (details printed to stderr)
"""
import argparse
import json
import os
import re
import sqlite3
import subprocess
import sys

PSQL = os.environ.get(
    "PSQL_BIN", "/opt/homebrew/Cellar/libpq/18.2/bin/psql"
)
DATABASE_URL = os.environ.get(
    "DATABASE_URL", "postgresql://osm:osm_dev@localhost:5434/osm"
)
GROUND_TRUTH_DB = os.environ.get(
    "GROUND_TRUTH_DB",
    "/Users/tjohnell/projects/pike/server/.data/ground_truth_portal.sqlite",
)
BASELINE_PATH = os.environ.get(
    "EVAL_BASELINE", "tooling/.eval_baseline.json"
)
MIN_CORRIDORS = 262


def query_psql(sql: str) -> str:
    result = subprocess.run(
        [PSQL, DATABASE_URL, "-t", "-A", "-F\t", "-c", sql],
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        print(f"psql error: {result.stderr}", file=sys.stderr)
        sys.exit(1)
    return result.stdout.strip()


def normalize_ground_truth_ref(ref_val: str) -> list[str]:
    """Expand ground truth compound refs into individual forms for matching.

    Returns list of alternative refs (NOT including the original).
    Only used so that if ground truth has "19A,B" and OI has "19A" + "19B",
    we count both halves as covered rather than missing "19A,B".
    """
    alts = []

    # Comma format: "11A,B" → "11A","11B"; "12A,12B" → "12A","12B"
    if "," in ref_val:
        parts = [p.strip() for p in ref_val.split(",") if p.strip()]
        if len(parts) >= 2:
            first = parts[0]
            m = re.match(r"^(\d+)", first)
            base = m.group(1) if m else ""
            for p in parts:
                if re.match(r"^\d", p):
                    alts.append(p)
                elif base:
                    alts.append(f"{base}{p}")

    # Slash format: "228 / 229" → "228","229"
    if "/" in ref_val:
        parts = [p.strip() for p in ref_val.split("/") if p.strip()]
        if len(parts) >= 2 and all(re.match(r"^\d", p) for p in parts):
            alts.extend(parts)

    # Semicolon: "143A;143B" → "143A","143B"
    if ";" in ref_val:
        parts = [p.strip() for p in ref_val.split(";") if p.strip()]
        if len(parts) >= 2:
            alts.extend(parts)

    # Dash-range with full refs: "1A-1B" → "1A","1B"; "14-14A-14B" → "14","14A","14B"
    if "-" in ref_val and "," not in ref_val:
        parts = [p.strip() for p in ref_val.split("-") if p.strip()]
        if len(parts) >= 2 and all(re.match(r"^\d", p) for p in parts):
            alts.extend(parts)

    # Letter-range: "108A-B" → "108A","108B"; "267B-A" → "267A","267B"
    m = re.match(r"^(\d+)([A-Z])-([A-Z])$", ref_val)
    if m:
        base, start, end = m.group(1), m.group(2), m.group(3)
        lo, hi = min(start, end), max(start, end)
        if ord(hi) - ord(lo) <= 5:
            for c in range(ord(lo), ord(hi) + 1):
                alts.append(f"{base}{chr(c)}")

    # Concatenated letters: "214AB" → "214A","214B"; "30BC" → "30B","30C"
    m = re.match(r"^(\d+)([A-Z]{2,})$", ref_val)
    if m:
        base, letters = m.group(1), m.group(2)
        if len(letters) <= 4:
            for ch in letters:
                alts.append(f"{base}{ch}")

    # Full-ref dash: "1A-1B" (already handled above if both parts start with digit)
    # Handle "0A-B" style (letter range with leading zero)
    m = re.match(r"^(\d+)([A-Z])-([A-Z])$", ref_val)
    if m and not alts:
        # Already handled above
        pass

    return alts


def load_ground_truth_pairs() -> set[tuple[str, str]]:
    conn = sqlite3.connect(GROUND_TRUTH_DB)
    rows = conn.execute(
        """
        SELECT DISTINCT h.display_name, e.sign_number
        FROM exits e
        JOIN request_exits re ON re.exit_id = e.exit_id
        JOIN exits_requests er ON er.cache_key = re.cache_key
        JOIN highway_in_states his ON his.highway_in_state_id = er.highway_in_state_id
        JOIN highways h ON h.highway_id = his.highway_id
        WHERE h.highway_type = 'Interstate'
          AND e.sign_number IS NOT NULL AND e.sign_number != ''
          AND h.display_name LIKE 'I-%'
        """
    ).fetchall()
    conn.close()
    return {(dn, sn.strip()) for dn, sn in rows}


def match_with_normalization(
    ground_truth_pairs: set[tuple[str, str]],
    oi_pairs: set[tuple[str, str]],
) -> set[tuple[str, str]]:
    """Match ground truth pairs against OI pairs, with compound-ref normalization.

    A compound ground truth ref like "19A,B" is considered matched if ALL of its
    expanded parts ("19A", "19B") are in OI. The original compound form
    counts as one match (not inflating the denominator).
    """
    matched = ground_truth_pairs & oi_pairs

    # For unmatched compound refs, check if expanded forms are all in OI
    for hw, ref in ground_truth_pairs - matched:
        alts = normalize_ground_truth_ref(ref)
        if alts and all((hw, alt) in oi_pairs for alt in alts):
            matched.add((hw, ref))

    return matched


def strip_highway_suffix(highway: str) -> str | None:
    """Strip directional suffix from highways like I-35E → I-35.

    Only strips single trailing letters (E/W/N/S/C) after the route
    number, which represent concurrent route splits in the US Interstate
    system (e.g. I-35E, I-35W, I-69C).
    """
    import re
    m = re.match(r'^(I-\d+)[A-Z]$', highway)
    return m.group(1) if m else None


def load_oi_pairs() -> set[tuple[str, str]]:
    raw = query_psql(
        "SELECT c.highway, ce.ref "
        "FROM corridor_exits ce "
        "JOIN corridors c ON c.corridor_id = ce.corridor_id "
        "WHERE ce.ref IS NOT NULL AND ce.ref != '' "
        "AND c.highway LIKE 'I-%'"
    )
    pairs = set()
    for line in raw.split("\n"):
        if not line:
            continue
        parts = line.split("\t")
        if len(parts) >= 2:
            hw, ref = parts[0], parts[1]
            pairs.add((hw, ref))
            # Also add base highway for letter-suffix routes (I-35E → I-35)
            base_hw = strip_highway_suffix(hw)
            if base_hw:
                pairs.add((base_hw, ref))
            # If ref is lettered like "153A", also count as covering "153"
            if re.match(r'^\d+[A-Z]$', ref):
                base_ref = ref[:-1]
                pairs.add((hw, base_ref))
                if base_hw:
                    pairs.add((base_hw, base_ref))
    return pairs


def load_corridor_count() -> int:
    raw = query_psql("SELECT COUNT(*) FROM corridors")
    return int(raw)


def load_baseline(path: str) -> set[tuple[str, str]] | None:
    if not os.path.exists(path):
        return None
    with open(path) as f:
        data = json.load(f)
    return {(pair[0], pair[1]) for pair in data["matched_pairs"]}


def save_baseline(path: str, matched: set[tuple[str, str]], score: float):
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    with open(path, "w") as f:
        json.dump(
            {
                "score": score,
                "matched_count": len(matched),
                "matched_pairs": sorted(matched),
            },
            f,
        )


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--save-baseline",
        action="store_true",
        help="Save current matched pairs as the regression baseline",
    )
    parser.add_argument(
        "--baseline",
        default=BASELINE_PATH,
        help=f"Path to baseline file (default: {BASELINE_PATH})",
    )
    parser.add_argument(
        "--min-corridors",
        type=int,
        default=MIN_CORRIDORS,
        help=f"Minimum corridor count gate (default: {MIN_CORRIDORS})",
    )
    parser.add_argument(
        "--json", action="store_true", help="Output results as JSON"
    )
    args = parser.parse_args()

    # Load data
    ground_truth_pairs = load_ground_truth_pairs()
    oi_pairs = load_oi_pairs()
    corridor_count = load_corridor_count()
    matched = match_with_normalization(ground_truth_pairs, oi_pairs)

    score = 100.0 * len(matched) / len(ground_truth_pairs) if ground_truth_pairs else 0.0

    # Gate checks
    gates_passed = True
    gate_results = {}

    # Gate 1: corridor count
    corridor_ok = corridor_count >= args.min_corridors
    gate_results["corridor_count"] = {
        "passed": corridor_ok,
        "value": corridor_count,
        "minimum": args.min_corridors,
    }
    if not corridor_ok:
        gates_passed = False

    # Gate 2: regression check
    baseline_matched = load_baseline(args.baseline)
    if baseline_matched is not None:
        regressions = baseline_matched - matched
        regression_ok = len(regressions) == 0
        gate_results["regressions"] = {
            "passed": regression_ok,
            "count": len(regressions),
            "examples": sorted(regressions)[:10],
        }
        if not regression_ok:
            gates_passed = False
    else:
        gate_results["regressions"] = {
            "passed": True,
            "count": 0,
            "note": "no baseline file found, skipping regression check",
        }

    results = {
        "score": round(score, 4),
        "matched": len(matched),
        "ground_truth_total": len(ground_truth_pairs),
        "oi_total": len(oi_pairs),
        "corridor_count": corridor_count,
        "gates_passed": gates_passed,
        "gates": gate_results,
    }

    if args.json:
        print(json.dumps(results, indent=2))
    else:
        print(f"Score: {score:.2f}% ({len(matched)}/{len(ground_truth_pairs)})")
        print(f"OI unique exits: {len(oi_pairs)}")
        print(f"Corridors: {corridor_count}")
        print(f"Gates: {'PASS' if gates_passed else 'FAIL'}")
        for name, gate in gate_results.items():
            status = "PASS" if gate["passed"] else "FAIL"
            if name == "corridor_count":
                print(f"  {name}: {status} ({gate['value']} >= {gate['minimum']})")
            elif name == "regressions":
                note = gate.get("note", "")
                if note:
                    print(f"  {name}: {status} ({note})")
                else:
                    print(f"  {name}: {status} ({gate['count']} regressions)")
                    if gate["count"] > 0:
                        for pair in gate["examples"]:
                            print(f"    lost: {pair[0]} {pair[1]}")

    if args.save_baseline:
        save_baseline(args.baseline, matched, score)
        if not args.json:
            print(f"\nBaseline saved to {args.baseline} ({len(matched)} pairs)")

    sys.exit(0 if gates_passed else 1)


if __name__ == "__main__":
    main()
