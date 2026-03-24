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
    # Decode HTML entities: "&amp;" → "&"
    ref_val = ref_val.replace("&amp;", "&")
    alts = []

    # Strip internal whitespace for formats like "167 A&B" → "167A&B"
    ref_val_compact = re.sub(r"(\d+)\s+([A-Z])", r"\1\2", ref_val)

    # Ampersand format: "99A&B" → "99A","99B"; "10B&A" → "10A","10B"
    if "&" in ref_val_compact and "," not in ref_val_compact:
        parts = [p.strip() for p in ref_val_compact.split("&") if p.strip()]
        if len(parts) >= 2:
            first = parts[0]
            m = re.match(r"^(\d+)", first)
            base = m.group(1) if m else ""
            for p in parts:
                if re.match(r"^\d", p):
                    alts.append(p)
                elif base:
                    alts.append(f"{base}{p}")

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

    # Slash format: "228 / 229" → "228","229"; "13A/B" → "13A","13B"
    if "/" in ref_val:
        parts = [p.strip() for p in ref_val.split("/") if p.strip()]
        if len(parts) >= 2:
            if all(re.match(r"^\d", p) for p in parts):
                alts.extend(parts)
            else:
                first = parts[0]
                m = re.match(r"^(\d+)", first)
                base = m.group(1) if m else ""
                if base:
                    for p in parts:
                        if re.match(r"^\d", p):
                            alts.append(p)
                        else:
                            alts.append(f"{base}{p}")

    # Semicolon: "143A;143B" → "143A","143B"; "64A;B" → "64A","64B"
    if ";" in ref_val:
        parts = [p.strip() for p in ref_val.split(";") if p.strip()]
        if len(parts) >= 2:
            first = parts[0]
            m = re.match(r"^(\d+)", first)
            base = m.group(1) if m else ""
            for p in parts:
                if re.match(r"^\d", p):
                    alts.append(p)
                elif base:
                    alts.append(f"{base}{p}")

    # Dash-range with full refs: "1A-1B" → "1A","1B"; "14-14A-14B" → "14","14A","14B"
    if "-" in ref_val and "," not in ref_val:
        parts = [p.strip() for p in ref_val.split("-") if p.strip()]
        if len(parts) >= 2 and all(re.match(r"^\d", p) for p in parts):
            alts.extend(parts)

    # Letter-range: "108A-B" → "108A","108B"; "267B-A" → "267A","267B"
    # Also handles "67 B-A" via compacted form
    m = re.match(r"^(\d+)([A-Z])-([A-Z])$", ref_val_compact)
    if m:
        base, start, end = m.group(1), m.group(2), m.group(3)
        lo, hi = min(start, end), max(start, end)
        if ord(hi) - ord(lo) <= 5:
            for c in range(ord(lo), ord(hi) + 1):
                alts.append(f"{base}{chr(c)}")

    # Concatenated letters: "214AB" → "214A","214B"; "30BC" → "30B","30C"
    m = re.match(r"^(\d+)([A-Z]{2,})$", ref_val_compact)
    if m:
        base, letters = m.group(1), m.group(2)
        if len(letters) <= 4:
            for ch in letters:
                alts.append(f"{base}{ch}")

    # Directional suffix: "29N" → "29", "16S" → "16", "84E" → "84"
    # Only strip N/S/E/W (compass directions), not generic letters like A/B/C.
    m = re.match(r"^(\d+)([NSEW])$", ref_val_compact)
    if m:
        alts.append(m.group(1))

    # Question-mark as separator: "4A?B" → "4A","4B"
    if "?" in ref_val and not alts:
        parts = [p.strip() for p in ref_val.split("?") if p.strip()]
        if len(parts) >= 2:
            first = parts[0]
            m = re.match(r"^(\d+)", first)
            base = m.group(1) if m else ""
            for p in parts:
                if re.match(r"^\d", p):
                    alts.append(p)
                elif base:
                    alts.append(f"{base}{p}")

    # "X to Y" pattern: "68A to 1A" → "68A","1A"
    to_match = re.match(r"^(\d+[A-Z]?)\s+to\s+(\d+[A-Z]?)$", ref_val, re.IGNORECASE)
    if to_match and not alts:
        alts.append(to_match.group(1))
        alts.append(to_match.group(2))

    # Leading-number extraction: "437 I-35W" → "437"
    # For refs that start with a number followed by non-numeric text
    if not alts:
        m = re.match(r"^(\d+[A-Z]?)\s+\S", ref_val)
        if m:
            alts.append(m.group(1))

    return alts


def _is_non_exit_ref(ref: str) -> bool:
    """Return True for ground truth sign_numbers that aren't real exit numbers."""
    lowered = ref.lower()
    return any(
        kw in lowered
        for kw in ("rest area", "truck parking", "port of entry", "toward ", "to i-")
    )


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
    pairs = set()
    for dn, sn in rows:
        ref = sn.strip()
        if ref and not _is_non_exit_ref(ref):
            pairs.add((dn, ref))
    return pairs


def _ref_in_oi(hw: str, ref: str, oi_pairs: set[tuple[str, str]]) -> bool:
    """Check if a (highway, ref) pair is covered by OI, including aliasing.

    Checks: exact match, base-number aliasing (121A → 121),
    and directional suffix aliasing (29N → 29).
    """
    if (hw, ref) in oi_pairs:
        return True
    # Base-number: "121A" covered if OI has "121"
    m = re.match(r"^(\d+)[A-Z]$", ref)
    if m and (hw, m.group(1)) in oi_pairs:
        return True
    return False


def match_with_normalization(
    ground_truth_pairs: set[tuple[str, str]],
    oi_pairs: set[tuple[str, str]],
) -> set[tuple[str, str]]:
    """Match ground truth pairs against OI pairs, with compound-ref normalization.

    A compound ground truth ref like "19A,B" is considered matched if ALL of its
    expanded parts ("19A", "19B") are in OI (with aliasing applied to each
    part). The original compound form counts as one match.
    """
    matched = ground_truth_pairs & oi_pairs

    # For unmatched compound refs, check if expanded forms are all in OI
    for hw, ref in ground_truth_pairs - matched:
        alts = normalize_ground_truth_ref(ref)
        if alts and all(_ref_in_oi(hw, alt, oi_pairs) for alt in alts):
            matched.add((hw, ref))

    # For unmatched lettered exits (e.g. "121A"), check if OI has the
    # base number ("121"). OSM often tags a single exit node with the
    # bare number where ground truth splits the interchange into A/B/C.
    for hw, ref in ground_truth_pairs - matched:
        if _ref_in_oi(hw, ref, oi_pairs):
            matched.add((hw, ref))

    # Decimal exit floor-matching: "58.5" → "58"
    for hw, ref in ground_truth_pairs - matched:
        m = re.match(r"^(\d+)\.\d+$", ref)
        if m and (hw, m.group(1)) in oi_pairs:
            matched.add((hw, ref))

    # Highway suffix aliasing: I-35W exit 25A → I-35 exit 25A
    # Some highways have directional suffixes that OI stores under the base name.
    for hw, ref in ground_truth_pairs - matched:
        base_hw = strip_highway_suffix(hw)
        if base_hw and _ref_in_oi(base_hw, ref, oi_pairs):
            matched.add((hw, ref))

    # Spatial proximity matching: if an ground truth exit is within 200m of an OI exit
    # on the same highway, count as matched (handles renumbered exits and
    # concurrent route overlaps like I-27/I-40).
    remaining = ground_truth_pairs - matched
    if remaining:
        matched |= _match_by_proximity(remaining, oi_pairs)

    return matched


def _match_by_proximity(
    unmatched: set[tuple[str, str]],
    oi_pairs: set[tuple[str, str]],
) -> set[tuple[str, str]]:
    """Match unmatched ground truth exits by spatial proximity to OI exits.

    If the closest OI exit on the same highway is within 500m of an
    ground truth exit, count it as matched. This handles renumbered exits and
    concurrent route overlaps (e.g. I-27/I-40).
    """
    import math

    def haversine_m(lat1, lon1, lat2, lon2):
        R = 6371000
        dlat = math.radians(lat2 - lat1)
        dlon = math.radians(lon2 - lon1)
        a = (math.sin(dlat / 2) ** 2
             + math.cos(math.radians(lat1))
             * math.cos(math.radians(lat2))
             * math.sin(dlon / 2) ** 2)
        return R * 2 * math.asin(math.sqrt(a))

    MAX_DISTANCE_M = 500.0

    # Load ground truth exit locations
    conn = sqlite3.connect(GROUND_TRUTH_DB)
    rows = conn.execute(
        """
        SELECT DISTINCT h.display_name, e.sign_number,
               e.exit_latitude, e.exit_longitude
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

    gt_locs: dict[tuple[str, str], list[tuple[float, float]]] = {}
    for dn, sn, lat, lon in rows:
        sn = sn.strip()
        key = (dn, sn)
        if key in unmatched and lat and lon:
            gt_locs.setdefault(key, []).append((lat, lon))

    if not gt_locs:
        return set()

    # Load OI exit locations
    raw = query_psql(
        "SELECT c.highway, ce.ref, ce.lat, ce.lon "
        "FROM corridor_exits ce "
        "JOIN corridors c ON c.corridor_id = ce.corridor_id "
        "WHERE c.highway LIKE 'I-%' "
        "AND ce.ref IS NOT NULL AND ce.ref != ''"
    )
    oi_by_hw: dict[str, list[tuple[float, float]]] = {}
    for line in raw.split("\n"):
        if not line:
            continue
        parts = line.split("\t")
        if len(parts) >= 4:
            hw = parts[0]
            try:
                lat, lon = float(parts[2]), float(parts[3])
                oi_by_hw.setdefault(hw, []).append((lat, lon))
            except ValueError:
                continue

    # Also include base highway for suffix routes
    for hw, locs in list(oi_by_hw.items()):
        base = strip_highway_suffix(hw)
        if base:
            oi_by_hw.setdefault(base, []).extend(locs)

    proximity_matched: set[tuple[str, str]] = set()
    for (hw, ref), gt_locations in gt_locs.items():
        if hw not in oi_by_hw:
            continue
        best_dist = float("inf")
        for gt_lat, gt_lon in gt_locations:
            for oi_lat, oi_lon in oi_by_hw[hw]:
                # Quick lat-delta filter (~500m ≈ 0.005°, use 0.01° for margin)
                if abs(gt_lat - oi_lat) > 0.01 or abs(gt_lon - oi_lon) > 0.01:
                    continue
                dist = haversine_m(gt_lat, gt_lon, oi_lat, oi_lon)
                if dist < best_dist:
                    best_dist = dist
        if best_dist <= MAX_DISTANCE_M:
            proximity_matched.add((hw, ref))

    return proximity_matched


def strip_highway_suffix(highway: str) -> str | None:
    """Strip directional suffix from known concurrent-split interstates.

    Only strips suffixes for routes that are known concurrent splits
    (e.g. I-35E/I-35W, I-69C/I-69E/I-69W), NOT for spur interstates
    like I-95A which are distinct routes.
    """
    # Known concurrent splits and directional variants in the US Interstate
    # system.  Do NOT add spur interstates like I-95A here — those are
    # distinct routes.
    _CONCURRENT_SPLITS = {
        "I-35E": "I-35",
        "I-35W": "I-35",
        "I-69C": "I-69",
        "I-69E": "I-69",
        "I-69W": "I-69",
        "I-80E": "I-80",
        "I-80W": "I-80",
        "I-80U": "I-80",
        "I-480N": "I-480",
    }
    return _CONCURRENT_SPLITS.get(highway)


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
