# Release Issues — `release-2026-03-16`

Pending data quality issues identified by inspecting the latest local release
from workspace `2d0972…` on goose-drive.

Release averages for reference:
- **Edges/corridor:** ~139
- **Exits/corridor:** ~125
- **POIs per exit:** ~5.2

---

## 1. I-344 — Extreme edge-to-exit ratio, single direction only

| Corridor | Dir | Edges | Exits | POIs |
|----------|-----|------:|------:|-----:|
| I-344    | N   |    48 |     3 |   11 |
| I-344    | S   |     — |     — |    — |

- 48 edges but only 3 exits gives a 16:1 edge-to-exit ratio (release average is ~1.1:1).
- No southbound corridor exists.
- Likely a corridor-builder or exit-matching defect. Needs investigation of the underlying OSM relation and the derived graph.

---

## ~~2. I-40 — Large edge-count asymmetry between directions~~ RESOLVED

**Fixed in `release-2026-03-17-bidi-fix`** — commit on branch `codex/pbf-sha-workspaces`.

**Root cause:** `identify_stop_nodes` in `compress.rs` only recognized unidirectional
pass-through nodes (in_degree=1, out_degree=1). Seven OSM ways in the Great Smoky
Mountains section of I-40 are tagged `highway=motorway, oneway=no` (a real undivided
2-lane section through the national park). Their bidirectional arcs gave interior nodes
in_degree=2, out_degree=2, preventing compression and producing ~608 fragment edges.

**Fix:** Added bidirectional pass-through detection in `identify_stop_nodes` — nodes
with in_degree=2, out_degree=2 where the incoming and outgoing neighbor sets are
identical are now recognized as compressible.

| Corridor | Dir | Before | After | Delta |
|----------|-----|-------:|------:|------:|
| I-40     | E   |    813 |   812 |    -1 |
| I-40     | W   |  1,426 |   829 |  -597 |

The fix also improved ~80 other corridors with similar non-oneway stretches. No
corridors lost, no exits changed, POI link delta of -87 (~0.05%) is negligible.

---

## 3. Near-zero POI corridors

These corridors have a POI-per-exit ratio well below the release average of ~5.2.

| Interstate | Exits | POIs | POIs/Exit |
|-----------|------:|-----:|----------:|
| I-335     |     6 |    0 |      0.00 |
| I-269     |    33 |    3 |      0.09 |
| I-155     |    20 |    8 |      0.40 |
| I-165     |    22 |    7 |      0.32 |
| I-73      |   106 |   21 |      0.20 |
| I-840     |    44 |   10 |      0.23 |
| I-530     |    45 |   49 |      1.09 |
| I-587     |    49 |   30 |      0.61 |

### ~~I-73~~ WON'T FIX — not a bug

Investigated via Overpass and local PostGIS. OSM has plenty of POIs near I-73
(e.g. 44 within 1,500m of Exit 102 in Greensboro), but zero I-73 exits have a
POI within the 800m linking radius. The nearest POI to Exit 102 is 808m away.
I-73's exit nodes sit on the mainline carriageway and commercial clusters are
consistently 1–3km down the exit ramps — the geometry just doesn't fit the 800m
radius that works for most interstates. Widening the radius globally or adding
per-exit adaptive linking is a design-level change, not a targeted fix.

### Remaining

- I-335, I-269, I-155, I-165, I-840, I-530, I-587 have not been investigated.

---

## 4. Single-direction corridors (non-beltway)

Beltways and loops (I-485, I-610, I-635) are expected to appear as a single traversal direction. The following are **not** loops but only have one direction:

| Interstate | Dir | Edges | Exits | POIs | Notes |
|-----------|-----|------:|------:|-----:|-------|
| I-344     | N   |    48 |     3 |   11 | See issue 1 |
| I-555     | N   |    45 |    38 |   77 | AR spur; may legitimately lack a signed opposite direction |
| I-575     | S   |    18 |    14 |   78 | GA spur; same question |

- Confirm whether OSM has both directions tagged for I-344, I-555, and I-575, or if the corridor builder is dropping one side.

---

## 5. I-270 South — Low exit yield on one segment

I-270 splits into multiple corridor segments. One southbound segment has a notably low exit-to-edge ratio:

| Corridor segment | Dir | Edges | Exits | POIs |
|-----------------|-----|------:|------:|-----:|
| Segment A       | S   |    61 |    19 |  119 |
| Segment B       | S   |    21 |    14 |  150 |
| Segment A       | N   |    38 |    28 |  256 |
| Segment B       | N   |    20 |    17 |  114 |

- Segment A South has a 3.2:1 edge-to-exit ratio vs ~1.4:1 for the other segments.
- May indicate a geometry fragment that didn't get proper exit matching, or an OSM tagging gap on that stretch.
