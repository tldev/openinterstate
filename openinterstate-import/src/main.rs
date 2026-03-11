mod exit_poi_linker;
mod graph_builder;
mod importer;
mod nsi;
mod parser;
mod pbf_importer;
mod reference_routes;
mod workflows;

use clap::{ArgGroup, Parser};
use sqlx::PgPool;
use std::path::{Path, PathBuf};

#[derive(Parser, Debug)]
#[command(name = "openinterstate-import", about = "Fetch OSM data and import into PostGIS")]
#[command(
    group(
        ArgGroup::new("operation_mode")
            .args([
                "resume_exit_poi_only",
                "heal_components",
                "score_reachability_only",
                "relink_existing_way_pois_only",
                "backfill_way_pois_only",
                "build_graph_only",
                "build_corridors_only",
                "build_reference_routes_only",
            ])
            .multiple(false)
    )
)]
struct Cli {
    /// Import from OSM PBF extract files in a directory (production path).
    /// When present (or when `pbf_dir` exists), this path is preferred.
    #[arg(long)]
    from_pbf_dir: Option<String>,

    /// Default PBF extract directory used when --from-pbf-dir is not provided.
    #[arg(long, default_value = "data/extracts")]
    pbf_dir: String,

    /// Exit-POI match radius in meters (for linking)
    #[arg(long, default_value = "800")]
    match_radius: i32,

    /// Database URL
    #[arg(
        long,
        env = "DATABASE_URL",
        default_value = "postgres://osm:osm_dev@localhost:5433/osm"
    )]
    database_url: String,

    /// Path to NSI dist directory (contains json/nsi.json)
    #[arg(long, default_value = "nsi/node_modules/name-suggestion-index/dist")]
    nsi_dir: PathBuf,

    /// Skip access-road reachability scoring/filtering
    #[arg(long)]
    skip_reachability: bool,

    /// OSRM base URL used for reachability scoring.
    #[arg(long, env = "OSRM_URL", default_value = "http://localhost:5000")]
    osrm_url: String,

    /// Max number of parallel OSRM requests while scoring reachability.
    #[arg(long, env = "OSRM_PARALLELISM", default_value = "16")]
    osrm_parallelism: usize,

    /// Resume from already-imported exits/pois/highway data and run only
    /// exit->POI linking plus optional reachability scoring.
    #[arg(long)]
    resume_exit_poi_only: bool,

    /// Additive mode: upsert into existing DB and process only new PBF files.
    /// Intended for building CONUS in multiple runs without wiping prior data.
    #[arg(long)]
    incremental: bool,

    /// Allow destructive replacement behavior that clears product OSM-derived
    /// tables before import. Disabled by default to preserve long-running state.
    #[arg(long, env = "OI_ALLOW_PRODUCT_REPLACE", default_value_t = false)]
    allow_destructive_replace: bool,

    /// Parse/import only way-derived POIs from PBF extracts, then relink and
    /// reachability-score only affected exits.
    #[arg(long)]
    backfill_way_pois_only: bool,

    /// Skip PBF parsing/import and relink only exits affected by existing
    /// way-derived POIs already present in the database.
    #[arg(long)]
    relink_existing_way_pois_only: bool,

    /// Heal existing `exit_corridors.graph_component` values by exact graph-node
    /// membership against `highway_edges` (no heuristics).
    #[arg(long)]
    heal_components: bool,

    /// Score reachability from existing `exit_poi_candidates` only (no relink/import).
    #[arg(long = "score-reachability-only")]
    score_reachability_only: bool,

    /// Build compressed highway graph from `osm2pgsql_v2_highways` and
    /// `osm2pgsql_v2_exits_nodes`, writing `highway_edges` + `exit_corridors`.
    #[arg(long)]
    build_graph_only: bool,

    /// Build corridor abstractions from existing `highway_edges` and
    /// `exit_corridors`, writing into `corridors` + `corridor_exits` and
    /// updating `highway_edges.corridor_id`.
    #[arg(long)]
    build_corridors_only: bool,

    /// When used with --build-corridors-only, only process these highways
    /// (comma-separated, e.g. "I-40,I-75,I-64"). Skips DB writes and just
    /// prints corridor counts — fast iteration for debugging merge logic.
    #[arg(long)]
    highway_filter: Option<String>,

    /// When used with --build-corridors-only --highway-filter, write a GeoJSON
    /// visualization of edges and exits to this path.
    #[arg(long)]
    geojson_out: Option<String>,

    /// Build directional reference routes from existing `highway_edges` and
    /// write them into `reference_routes` + `reference_route_anchors`.
    #[arg(long)]
    build_reference_routes_only: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum OperationMode {
    ResumeExitPoiOnly,
    HealComponents,
    ScoreReachabilityOnly,
    RelinkExistingWayPoisOnly,
    BackfillWayPoisOnly,
    BuildGraphOnly,
    BuildCorridorsOnly,
    BuildReferenceRoutesOnly,
    FullImport,
}

impl Cli {
    fn operation_mode(&self) -> OperationMode {
        if self.resume_exit_poi_only {
            OperationMode::ResumeExitPoiOnly
        } else if self.heal_components {
            OperationMode::HealComponents
        } else if self.score_reachability_only {
            OperationMode::ScoreReachabilityOnly
        } else if self.relink_existing_way_pois_only {
            OperationMode::RelinkExistingWayPoisOnly
        } else if self.backfill_way_pois_only {
            OperationMode::BackfillWayPoisOnly
        } else if self.build_graph_only {
            OperationMode::BuildGraphOnly
        } else if self.build_corridors_only {
            OperationMode::BuildCorridorsOnly
        } else if self.build_reference_routes_only {
            OperationMode::BuildReferenceRoutesOnly
        } else {
            OperationMode::FullImport
        }
    }

    fn resolved_pbf_dir(&self) -> &str {
        self.from_pbf_dir.as_deref().unwrap_or(&self.pbf_dir)
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    init_tracing();
    run(Cli::parse()).await
}

fn init_tracing() {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "openinterstate_import=info".into()),
        )
        .init();
}

async fn run(cli: Cli) -> anyhow::Result<()> {
    let pool = connect_pool(&cli.database_url).await?;
    configure_osrm_env(&cli);
    tracing::info!("Connected to database");

    match cli.operation_mode() {
        OperationMode::ResumeExitPoiOnly => run_resume_exit_poi_only(&pool, &cli).await,
        OperationMode::HealComponents => run_heal_components(&pool).await,
        OperationMode::ScoreReachabilityOnly => run_score_reachability_only(&pool, &cli).await,
        OperationMode::RelinkExistingWayPoisOnly => {
            run_relink_existing_way_pois_only(&pool, &cli).await
        }
        OperationMode::BackfillWayPoisOnly => run_backfill_way_pois_only(&pool, &cli).await,
        OperationMode::BuildGraphOnly => run_build_graph(&pool).await,
        OperationMode::BuildCorridorsOnly => {
            run_build_corridors_only(
                &pool,
                cli.highway_filter.as_deref(),
                cli.geojson_out.as_deref(),
            )
            .await
        }
        OperationMode::BuildReferenceRoutesOnly => run_build_reference_routes_only(&pool).await,
        OperationMode::FullImport => run_full_import(&pool, &cli).await,
    }
}

async fn connect_pool(database_url: &str) -> anyhow::Result<PgPool> {
    let pool = sqlx::postgres::PgPoolOptions::new()
        .max_connections(20)
        .connect(database_url)
        .await?;
    Ok(pool)
}

fn configure_osrm_env(cli: &Cli) {
    std::env::set_var("OSRM_URL", &cli.osrm_url);
    std::env::set_var("OSRM_PARALLELISM", cli.osrm_parallelism.to_string());
}

async fn run_resume_exit_poi_only(pool: &PgPool, cli: &Cli) -> anyhow::Result<()> {
    tracing::info!(
        "Resuming exit->POI linking only (match_radius={}m, reachability={})",
        cli.match_radius,
        if cli.skip_reachability {
            "skipped"
        } else {
            "enabled"
        }
    );

    let linked = exit_poi_linker::link_exits_to_pois(pool, cli.match_radius).await?;
    tracing::info!("Created {} exit-POI links", linked);

    if cli.skip_reachability {
        tracing::info!("Skipping reachability scoring (--skip-reachability)");
    } else {
        let filtered = exit_poi_linker::score_and_filter_poi_reachability(pool).await?;
        tracing::info!("Removed {} weak/unreachable POI links", filtered);
    }

    tracing::info!("Resume-only linking complete!");
    Ok(())
}

async fn run_heal_components(pool: &PgPool) -> anyhow::Result<()> {
    let stats = graph_builder::heal_exit_corridors_from_graph_nodes(pool).await?;
    tracing::info!(
        "Heal complete: updated_rows={}, unresolved_no_edge={}, unresolved_ambiguous={}",
        stats.updated_rows,
        stats.unresolved_no_edge,
        stats.unresolved_ambiguous
    );
    Ok(())
}

async fn run_score_reachability_only(pool: &PgPool, cli: &Cli) -> anyhow::Result<()> {
    tracing::info!(
        "Scoring reachability from exit_poi_candidates only (osrm_url={})",
        cli.osrm_url,
    );
    let filtered = exit_poi_linker::score_and_filter_poi_reachability(pool).await?;
    tracing::info!("Removed {} weak/unreachable POI links", filtered);
    tracing::info!("Reachability scoring complete!");
    Ok(())
}

async fn run_relink_existing_way_pois_only(pool: &PgPool, cli: &Cli) -> anyhow::Result<()> {
    workflows::relink_existing_way_pois_only(pool, cli.match_radius, cli.skip_reachability).await?;
    tracing::info!("Existing way-POI relink complete!");
    Ok(())
}

async fn run_backfill_way_pois_only(pool: &PgPool, cli: &Cli) -> anyhow::Result<()> {
    let pbf_dir = cli.resolved_pbf_dir();
    ensure_pbf_dir_exists(pbf_dir)?;

    let nsi_matcher = load_nsi_matcher(&cli.nsi_dir);
    workflows::backfill_way_pois_from_pbf_extracts(
        pool,
        pbf_dir,
        cli.match_radius,
        nsi_matcher.as_ref(),
        cli.skip_reachability,
    )
    .await?;
    tracing::info!("Way-POI backfill complete!");
    Ok(())
}

async fn run_build_graph(pool: &PgPool) -> anyhow::Result<()> {
    tracing::info!("Building highway graph from osm2pgsql tables...");
    let edge_count = graph_builder::build_graph(pool).await?;
    tracing::info!("Graph build complete: {} edges", edge_count);
    Ok(())
}

async fn run_build_corridors_only(
    pool: &PgPool,
    highway_filter: Option<&str>,
    geojson_out: Option<&str>,
) -> anyhow::Result<()> {
    if let Some(filter) = highway_filter {
        let highways: Vec<&str> = filter.split(',').map(|s| s.trim()).collect();
        tracing::info!("LAB MODE: testing corridor logic for {:?}", highways);
        graph_builder::corridors::build_corridors_lab(pool, &highways, geojson_out).await?;
    } else {
        tracing::info!("Building corridors from derived highway_edges...");
        let stats = graph_builder::corridors::build_corridors(pool).await?;
        tracing::info!(
            "Corridor build complete: {} corridors, {} exits, {} edges updated",
            stats.corridors_created,
            stats.corridor_exits_created,
            stats.edges_updated
        );
    }
    Ok(())
}

async fn run_build_reference_routes_only(pool: &PgPool) -> anyhow::Result<()> {
    tracing::info!("Building reference routes from derived highway_edges...");
    reference_routes::build_reference_routes(pool).await?;
    tracing::info!("Reference route build complete!");
    Ok(())
}

async fn run_full_import(pool: &PgPool, cli: &Cli) -> anyhow::Result<()> {
    let pbf_dir = cli.resolved_pbf_dir();
    ensure_pbf_dir_exists(pbf_dir)?;

    let nsi_matcher = load_nsi_matcher(&cli.nsi_dir);
    workflows::import_from_pbf_extracts(
        pool,
        pbf_dir,
        cli.match_radius,
        nsi_matcher.as_ref(),
        cli.skip_reachability,
        cli.incremental,
        cli.allow_destructive_replace,
    )
    .await?;
    tracing::info!("Import complete!");
    Ok(())
}

fn ensure_pbf_dir_exists(pbf_dir: &str) -> anyhow::Result<()> {
    if Path::new(pbf_dir).exists() {
        Ok(())
    } else {
        anyhow::bail!("No PBF directory found at {pbf_dir}")
    }
}

fn load_nsi_matcher(nsi_dir: &Path) -> Option<nsi::NsiBrandMatcher> {
    if let Some(matcher) = nsi::NsiBrandMatcher::from_dir(nsi_dir) {
        tracing::info!("NSI brand matcher loaded from {}", nsi_dir.display());
        Some(matcher)
    } else {
        tracing::warn!(
            "NSI data not found at {} - brand canonicalization disabled",
            nsi_dir.display()
        );
        None
    }
}

#[cfg(test)]
mod tests {
    use clap::Parser;

    use super::{Cli, OperationMode};

    #[test]
    fn rejects_conflicting_operation_modes() {
        let parse = Cli::try_parse_from([
            "openinterstate-import",
            "--resume-exit-poi-only",
            "--score-reachability-only",
        ]);
        assert!(parse.is_err(), "conflicting mode flags should be rejected");
    }

    #[test]
    fn accepts_single_operation_mode() {
        let parse = Cli::try_parse_from(["openinterstate-import", "--score-reachability-only"]);
        assert!(parse.is_ok(), "single mode flag should parse successfully");
    }

    #[test]
    fn defaults_to_full_import_mode() {
        let cli = Cli::try_parse_from(["openinterstate-import"]).expect("parse default flags");
        assert_eq!(cli.operation_mode(), OperationMode::FullImport);
    }

    #[test]
    fn resolves_backfill_operation_mode() {
        let cli = Cli::try_parse_from(["openinterstate-import", "--backfill-way-pois-only"])
            .expect("parse backfill mode");
        assert_eq!(cli.operation_mode(), OperationMode::BackfillWayPoisOnly);
    }

    #[test]
    fn resolves_reference_route_build_mode() {
        let cli = Cli::try_parse_from(["openinterstate-import", "--build-reference-routes-only"])
            .expect("parse reference-route mode");
        assert_eq!(cli.operation_mode(), OperationMode::BuildReferenceRoutesOnly);
    }
}
