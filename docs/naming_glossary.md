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

## Retired Internal Names

- `highway_edges` -> `corridor_edges`
- `canonical_exits` -> `corridor_exits`
- `canonical_exit_aliases` -> `exit_aliases`
- `pois` -> `places`
- `reference_routes` is the preferred public term for route artifacts
- `reference_route_anchors` is the preferred public term for route anchor points
