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
