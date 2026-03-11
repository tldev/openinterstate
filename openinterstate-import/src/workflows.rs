use std::collections::{BTreeSet, HashMap};

use crate::{exit_poi_linker, graph_builder, importer, nsi, parser, pbf_importer};

pub async fn backfill_way_pois_from_pbf_extracts(
    pool: &sqlx::PgPool,
    pbf_dir: &str,
    match_radius: i32,
    nsi_matcher: Option<&nsi::NsiBrandMatcher>,
    skip_reachability: bool,
) -> anyhow::Result<()> {
    let files = pbf_importer::list_pbf_files(pbf_dir)?;
    if files.is_empty() {
        anyhow::bail!("No .pbf files found in {pbf_dir}");
    }

    tracing::info!(
        "Way-POI backfill: parsing {} PBF extract files from {}",
        files.len(),
        pbf_dir
    );

    let mut all_way_pois: HashMap<String, parser::ParsedPOI> = HashMap::new();
    for (i, file) in files.iter().enumerate() {
        tracing::info!(
            "Parsing way POIs from PBF [{}/{}]: {}",
            i + 1,
            files.len(),
            file.display()
        );
        let parsed = pbf_importer::parse_pbf_extract_way_pois_only(file, nsi_matcher)?;
        for p in parsed {
            all_way_pois.entry(p.id.clone()).or_insert(p);
        }
        tracing::info!("  Aggregated way POIs={}", all_way_pois.len());
    }

    if all_way_pois.is_empty() {
        tracing::info!("No way POIs found in extracts");
        return Ok(());
    }

    let way_pois: Vec<parser::ParsedPOI> = all_way_pois.into_values().collect();
    let way_poi_ids: Vec<String> = way_pois.iter().map(|p| p.id.clone()).collect();

    tracing::info!("Upserting {} way-derived POIs...", way_pois.len());
    importer::import_pois(pool, &way_pois, false).await?;

    tracing::info!("Finding exits affected by way-POI backfill...");
    let affected_exits = exit_poi_linker::exits_near_pois(pool, &way_poi_ids, match_radius).await?;
    tracing::info!(
        "Found {} affected exits within {}m of way POIs",
        affected_exits.len(),
        match_radius
    );

    relink_and_score_affected_exits(pool, &affected_exits, match_radius, skip_reachability).await
}

pub async fn relink_existing_way_pois_only(
    pool: &sqlx::PgPool,
    match_radius: i32,
    skip_reachability: bool,
) -> anyhow::Result<()> {
    tracing::info!("Loading existing way-derived POIs from database...");
    let rows: Vec<(String,)> = sqlx::query_as("SELECT id FROM pois WHERE osm_type = 'way'")
        .fetch_all(pool)
        .await?;
    let way_poi_ids: Vec<String> = rows.into_iter().map(|(id,)| id).collect();

    if way_poi_ids.is_empty() {
        tracing::info!("No way-derived POIs found in database");
        return Ok(());
    }

    tracing::info!(
        "Found {} way-derived POIs. Finding affected exits...",
        way_poi_ids.len()
    );
    let affected_exits = exit_poi_linker::exits_near_pois(pool, &way_poi_ids, match_radius).await?;
    tracing::info!(
        "Found {} affected exits within {}m of way POIs",
        affected_exits.len(),
        match_radius
    );

    relink_and_score_affected_exits(pool, &affected_exits, match_radius, skip_reachability).await
}

async fn relink_and_score_affected_exits(
    pool: &sqlx::PgPool,
    affected_exits: &[String],
    match_radius: i32,
    skip_reachability: bool,
) -> anyhow::Result<()> {
    if affected_exits.is_empty() {
        tracing::info!("No affected exits found; skipping relink/reachability");
        return Ok(());
    }

    tracing::info!("Relinking affected exits only...");
    let relinked = exit_poi_linker::relink_exits_subset(pool, affected_exits, match_radius).await?;
    tracing::info!("Created {} exit-POI links for affected exits", relinked);

    if !skip_reachability {
        tracing::info!("Scoring reachability for affected exits only...");
        let filtered =
            exit_poi_linker::score_and_filter_poi_reachability_for_exits(pool, affected_exits)
                .await?;
        tracing::info!(
            "Removed {} weak/unreachable POI links for affected exits",
            filtered
        );
    } else {
        tracing::info!("Skipping reachability scoring (--skip-reachability)");
    }

    Ok(())
}

pub async fn import_from_pbf_extracts(
    pool: &sqlx::PgPool,
    pbf_dir: &str,
    match_radius: i32,
    nsi_matcher: Option<&nsi::NsiBrandMatcher>,
    skip_reachability: bool,
    incremental: bool,
    allow_destructive_replace: bool,
) -> anyhow::Result<()> {
    let mut files = pbf_importer::list_pbf_files(pbf_dir)?;
    if files.is_empty() {
        anyhow::bail!("No .pbf files found in {pbf_dir}");
    }

    let mut previously_imported: BTreeSet<String> = BTreeSet::new();
    if incremental {
        previously_imported = read_imported_pbf_set(pool).await?;
        files.retain(|p| {
            let key = p.file_name().and_then(|s| s.to_str()).unwrap_or_default();
            !previously_imported.contains(key)
        });
        if files.is_empty() {
            tracing::info!("No new PBF files to process in incremental mode.");
            return Ok(());
        }
    }

    tracing::info!(
        "Found {} PBF extract files in {} (extract-mode)",
        files.len(),
        pbf_dir
    );

    let mut all_exits: HashMap<String, parser::ParsedExit> = HashMap::new();
    let mut all_pois: HashMap<String, parser::ParsedPOI> = HashMap::new();
    let mut all_highways: HashMap<String, parser::ParsedHighway> = HashMap::new();
    let mut all_states: BTreeSet<String> = BTreeSet::new();

    for (i, file) in files.iter().enumerate() {
        tracing::info!(
            "Parsing PBF [{}/{}]: {}",
            i + 1,
            files.len(),
            file.display()
        );
        let parsed = pbf_importer::parse_pbf_extract(file, nsi_matcher)?;

        for e in parsed.exits {
            all_exits.entry(e.id.clone()).or_insert(e);
        }
        for p in parsed.pois {
            all_pois.entry(p.id.clone()).or_insert(p);
        }
        for h in parsed.highways {
            all_highways.entry(h.id.clone()).or_insert(h);
        }

        if let Some(state) = infer_state_code_from_pbf_file(file) {
            all_states.insert(state);
        }

        tracing::info!(
            "  Aggregated exits={} pois={} highways={}",
            all_exits.len(),
            all_pois.len(),
            all_highways.len()
        );
    }

    tracing::info!(
        "Total parsed from extracts: {} exits, {} POIs, {} highway/access ways",
        all_exits.len(),
        all_pois.len(),
        all_highways.len()
    );

    let replace_existing = !incremental && allow_destructive_replace;
    if !incremental && !allow_destructive_replace {
        tracing::info!(
            "Destructive replace disabled; running additive upsert mode for exits/pois/highway graph"
        );
    }

    tracing::info!("Importing exits...");
    let exits: Vec<_> = all_exits.into_values().collect();
    importer::import_exits(pool, &exits, replace_existing).await?;

    tracing::info!("Importing POIs...");
    let pois: Vec<_> = all_pois.into_values().collect();
    importer::import_pois(pool, &pois, replace_existing).await?;

    tracing::info!("Building highway graph from osm2pgsql tables...");
    let edge_count = graph_builder::build_graph(pool).await?;
    tracing::info!("Created {} highway edges", edge_count);

    tracing::info!("Linking exits to POIs (radius={}m)...", match_radius);
    let link_count = exit_poi_linker::link_exits_to_pois(pool, match_radius).await?;
    tracing::info!("Created {} exit-POI links", link_count);

    if !skip_reachability {
        let filtered = exit_poi_linker::score_and_filter_poi_reachability(pool).await?;
        tracing::info!("Removed {} weak/unreachable POI links", filtered);
    } else {
        tracing::info!("Skipping reachability scoring (--skip-reachability)");
    }

    let states: Vec<String> = merge_existing_states(pool, all_states).await?;
    importer::update_meta(pool, &states).await?;

    {
        let mut pbf_set = if incremental {
            previously_imported
        } else {
            BTreeSet::new()
        };
        for p in &files {
            if let Some(name) = p.file_name().and_then(|s| s.to_str()) {
                pbf_set.insert(name.to_string());
            }
        }
        write_imported_pbf_set(pool, &pbf_set).await?;
    }
    Ok(())
}

async fn read_imported_pbf_set(pool: &sqlx::PgPool) -> anyhow::Result<BTreeSet<String>> {
    let row: Option<(String,)> =
        sqlx::query_as("SELECT value FROM meta WHERE key = 'imported_pbf_files'")
            .fetch_optional(pool)
            .await?;
    let mut out = BTreeSet::new();
    if let Some((json,)) = row {
        if let Ok(v) = serde_json::from_str::<Vec<String>>(&json) {
            for f in v {
                out.insert(f);
            }
        }
    }
    Ok(out)
}

async fn write_imported_pbf_set(
    pool: &sqlx::PgPool,
    files: &BTreeSet<String>,
) -> anyhow::Result<()> {
    let payload = serde_json::to_string(&files.iter().cloned().collect::<Vec<_>>())?;
    sqlx::query(
        "INSERT INTO meta (key, value) VALUES ('imported_pbf_files', $1) \
         ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value",
    )
    .bind(payload)
    .execute(pool)
    .await?;
    Ok(())
}

async fn merge_existing_states(
    pool: &sqlx::PgPool,
    new_states: BTreeSet<String>,
) -> anyhow::Result<Vec<String>> {
    let mut merged = new_states;
    let existing: Option<(String,)> = sqlx::query_as("SELECT value FROM meta WHERE key = 'states'")
        .fetch_optional(pool)
        .await?;
    if let Some((raw,)) = existing {
        for s in raw.split(',') {
            let t = s.trim().to_uppercase();
            if t.len() == 2 {
                merged.insert(t);
            }
        }
    }
    Ok(merged.into_iter().collect())
}

fn infer_state_code_from_pbf_file(path: &std::path::Path) -> Option<String> {
    let stem = path.file_stem()?.to_str()?;
    let state = stem.split(['_', '-']).next()?.to_uppercase();
    (state.len() == 2 && state.chars().all(|c| c.is_ascii_alphabetic())).then_some(state)
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use super::infer_state_code_from_pbf_file;

    #[test]
    fn infer_state_code_accepts_expected_prefixes() {
        assert_eq!(
            infer_state_code_from_pbf_file(Path::new("/tmp/md-latest.osm.pbf")),
            Some("MD".to_string())
        );
        assert_eq!(
            infer_state_code_from_pbf_file(Path::new("/tmp/pa_extract.osm.pbf")),
            Some("PA".to_string())
        );
    }

    #[test]
    fn infer_state_code_rejects_non_state_prefixes() {
        assert_eq!(
            infer_state_code_from_pbf_file(Path::new("/tmp/conus.osm.pbf")),
            None
        );
        assert_eq!(
            infer_state_code_from_pbf_file(Path::new("/tmp/newyork.osm.pbf")),
            None
        );
    }
}
