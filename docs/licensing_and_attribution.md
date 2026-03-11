# Licensing And Attribution

OpenInterstate is in active licensing review.

## Current Position

Repository code is published under the MIT license in `LICENSE`.

The public dataset release posture is not yet fully finalized. Current releases
should be treated conservatively as OSM-derived artifacts that require
attribution and may carry ODbL-related obligations depending on the final
release policy and any non-OSM separation decisions.

## Working Assumptions

1. OpenStreetMap-derived data requires attribution.
2. Derived database outputs may carry Open Database License obligations.
3. Any non-OSM overlays must be clearly separated from OSM-derived outputs.
4. Public releases must include attribution and lineage notes.

## Required Next

1. formalize the release-level data license posture
2. publish attribution language inside release artifacts and API responses
3. identify which artifacts are purely OSM-derived
4. separate any non-OSM overlays if they are introduced
5. review trademark and branding usage for third-party place names and logos

## Practical Rule

If a release cannot explain where its data came from and what obligations apply,
it is not ready to publish.
