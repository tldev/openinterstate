# Naming Glossary

This glossary defines the preferred public vocabulary for OpenInterstate.

## Preferred Terms

### Corridor

A contiguous directional interstate path used for ordering and associating exits.

### Corridor Edge

A graph edge that belongs to a corridor.

### Corridor Exit

A normalized exit record associated with a corridor.

### Exit Alias

A raw or source exit record that maps to a corridor exit.

### Place

A point or area of interest reachable from an exit.

### Exit-Place Link

A candidate or selected relationship between an exit and a place.

### Exit-Place Score

Reachability or ranking metadata attached to an exit-place link.

### Reference Route

A route artifact intended for exploration, QA, examples, and simulation.

### Reference Route Anchor

An indexed anchor point used to associate a reference route with a location.

## Terms To Avoid In Public Surface Area

### Test Drive

This reads like an app feature rather than a public data artifact.

Preferred replacement: `reference route`

### Canonical

Useful internally, but too implementation-specific for most public docs and APIs.

Preferred replacement: `corridor exit`, `normalized exit`, or a more specific
term tied to the actual object.

### Candidate

Useful internally, but vague in public docs unless paired with a clearer object.

Preferred replacement: `exit-place link` or `draft link`, depending on context.

## Current Pike Name Mapping

- `highway_edges` -> `corridor_edges`
- `canonical_exits` -> `corridor_exits`
- `canonical_exit_aliases` -> `exit_aliases`
- `pois` -> `places`
- `exit_poi_candidates` -> `exit_place_links`
- `exit_poi_reachability` -> `exit_place_scores`
- `test_drive_routes` -> `reference_routes`
- `test_drive_route_anchors` -> `reference_route_anchors`
