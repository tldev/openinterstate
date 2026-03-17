# OpenInterstate Agent Notes

## Default Workspace

- Default the managed parent root to `/Volumes/goose-drive/openinterstate`.
- In normal operation, do not choose an explicit `OI_DATA_ROOT` up front. Instead, resolve the source PBF first, hash it with SHA-256, and use the workspace path `/Volumes/goose-drive/openinterstate/workspaces/pbf-sha256/<sha256>`.
- Treat `/Volumes/goose-drive/openinterstate/source-cache` as the shared raw-source download cache.
- Treat `/Volumes/goose-drive/openinterstate/cache/cargo` as the shared Rust build cache across all PBF workspaces.
- Let release artifacts default under the selected PBF workspace, which means `<workspace>/releases`, unless the user explicitly overrides the release root.
- Only use `--data-dir` or `OI_DATA_ROOT` when the user explicitly asks to pin an exact workspace path and bypass the SHA-derived default.

## Import And Derive Workflow

- Treat canonical PBF import and derive as separate stages when planning work.
- Prefer derive-only iteration against the existing canonical database whenever the current import is still usable.
- Before proposing or running any fresh PBF import, first compute or read the PBF SHA and check the matching workspace under `workspaces/pbf-sha256/<sha256>`.
- Reuse an existing downloaded source PBF, filtered canonical PBF, and canonical osm2pgsql import whenever their inputs and mappings are still valid.
- Avoid re-importing PBF data unless it is clearly necessary because the source changed, the import mapping changed, the canonical database is missing or invalid, or the user explicitly requests a re-import.

## Operational Bias

- When discussing or running local build commands, default to `--data-parent /Volumes/goose-drive/openinterstate` rather than a repo-local `.data/` directory or a hard-coded workspace path.
- If derive-stage work is requested, start from the assumption that the existing canonical import should be preserved and reused.
- If a re-import is required, explain why it is necessary before doing it.

## Pike SQLite Follow-On

- After exporting an OpenInterstate release locally, also build Pike's SQLite pack from that exact release archive unless the user explicitly says not to.
- Use Pike's supported release-driven pipeline entrypoint from `/Users/tjohnell/projects/pike/server`: `./pike-pipeline.sh build --release-file /abs/path/openinterstate-release-<release-id>.tar.gz --reachability-snapshot /Volumes/goose-drive/pike-osrm/reachability/pike.osrm-reachability.snapshot.pgdump`.
- Let Pike keep its own default output locations unless the user asks otherwise. The current default host pack output is `/Users/tjohnell/projects/pike/server/.data/packs/pike.sqlite` and the staged build file is `/Users/tjohnell/projects/pike/server/.data/packs/pike.sqlite.new`.
- After the Pike build finishes, validate the pack with `sqlite3` by checking `PRAGMA integrity_check;` and confirming the `meta` table reports the matching `openinterstate_release_id`.

## Named Comparison: Pike Interstate Exit Coverage Diff

- If the user asks to rerun the comparison, refer to it as `Pike Interstate Exit Coverage Diff`.
- Purpose: compare the latest OpenInterstate-derived Pike pack against the latest published Pike release pack, limited to Interstate corridor and exit coverage.
- Inputs:
  - OpenInterstate-derived Pike pack: `/Users/tjohnell/projects/pike/server/.data/packs/pike.sqlite`
  - Latest published Pike release pack on NFS: newest `/Volumes/goose-plex-media/pike/releases/*/pike.sqlite`
- Before comparing, stage the latest published Pike release pack off NFS into `/Users/tjohnell/projects/pike/server/.data/compare/`. If a same-size, same-mtime local staged copy already exists, reuse it instead of copying again.
- Compare by `highway + canonical_direction`, starting from the OpenInterstate-derived pack's Interstate routes.
- Union exits across duplicate corridor rows for the same `highway + canonical_direction` key before counting or diffing.
- For route-level exit comparison, use distinct exit `ref` values when present. If a route has no usable `ref` values, fall back to a stable label such as `name` or `exit_id`.
- Separate findings into at least three buckets:
  - likely real gaps where the published Pike release is a near-superset of the OpenInterstate-derived route
  - likely real gaps where the OpenInterstate-derived route is a near-superset of the published Pike release
  - likely key pollution or route conflation where one side has far more exits and low overlap
- Always report:
  - route-level counts for both packs
  - shared exit count
  - exits only in OpenInterstate-derived pack
  - exits only in published Pike release
  - a short list of representative exit refs from each side for the biggest differences
- Write a durable comparison CSV into `/Users/tjohnell/projects/pike/server/.data/compare/` named like `openinterstate-<release-id>-vs-pike-<release-stamp>-route-exit-compare.csv`.
