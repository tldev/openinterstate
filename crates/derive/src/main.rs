mod canonical_types;
mod graph;
mod interstate_relations;
mod routes;

use std::path::{Path, PathBuf};

use clap::{Parser, Subcommand};
use sqlx::PgPool;

#[derive(Debug, Parser)]
#[command(
    name = "openinterstate-derive",
    about = "Build OpenInterstate graph, corridor, and reference-route tables"
)]
#[command(subcommand_required = true)]
struct Cli {
    /// Database URL for the local PostGIS instance.
    #[arg(long, env = "DATABASE_URL")]
    database_url: String,

    /// Cached Interstate route relation membership file.
    #[arg(long, env = "INTERSTATE_RELATION_CACHE")]
    interstate_relation_cache: PathBuf,

    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand, PartialEq, Eq)]
enum Command {
    /// Run graph, corridor, and reference-route builders in sequence.
    All,
    /// Rebuild highway_edges and exit_corridors from canonical osm2pgsql tables.
    Graph,
    /// Rebuild official Interstate corridors and corridor_exits.
    Corridors,
    /// Rebuild reference_routes and reference_route_anchors from official corridors.
    Routes,
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
                .unwrap_or_else(|_| "openinterstate_derive=info".into()),
        )
        .init();
}

async fn run(cli: Cli) -> anyhow::Result<()> {
    let Cli {
        database_url,
        interstate_relation_cache,
        command,
    } = cli;
    let pool = connect_pool(&database_url).await?;
    tracing::info!("Connected to database");

    match command {
        Command::All => run_all(&pool, &interstate_relation_cache).await,
        Command::Graph => run_graph(&pool, &interstate_relation_cache).await,
        Command::Corridors => run_corridors(&pool, &interstate_relation_cache).await,
        Command::Routes => run_routes(&pool, &interstate_relation_cache).await,
    }
}

async fn connect_pool(database_url: &str) -> anyhow::Result<PgPool> {
    Ok(sqlx::postgres::PgPoolOptions::new()
        .max_connections(4)
        .connect(database_url)
        .await?)
}

async fn run_all(pool: &PgPool, interstate_relation_cache: &Path) -> anyhow::Result<()> {
    run_graph(pool, interstate_relation_cache).await?;
    run_corridors(pool, interstate_relation_cache).await?;
    run_routes(pool, interstate_relation_cache).await
}

async fn run_graph(pool: &PgPool, interstate_relation_cache: &Path) -> anyhow::Result<()> {
    tracing::info!("Building highway graph from canonical osm2pgsql tables");
    let edge_count = graph::build_graph(pool, interstate_relation_cache).await?;
    tracing::info!("Graph build complete: {} edges", edge_count);
    Ok(())
}

async fn run_corridors(pool: &PgPool, interstate_relation_cache: &Path) -> anyhow::Result<()> {
    tracing::info!("Building Interstate corridors");
    let stats = graph::relation_corridors::build_corridors(pool, interstate_relation_cache).await?;
    tracing::info!(
        "Corridor build complete: {} corridors, {} exits, {} edges updated",
        stats.corridors_created,
        stats.corridor_exits_created,
        stats.edges_updated
    );
    Ok(())
}

async fn run_routes(pool: &PgPool, interstate_relation_cache: &Path) -> anyhow::Result<()> {
    tracing::info!("Building reference routes from corridors");
    routes::build_reference_routes(pool, interstate_relation_cache).await?;
    tracing::info!("Reference route build complete");
    Ok(())
}

#[cfg(test)]
mod tests {
    use clap::Parser;

    use super::{Cli, Command};

    #[test]
    fn requires_a_subcommand() {
        let parse = Cli::try_parse_from([
            "openinterstate-derive",
            "--database-url",
            "postgres://db",
            "--interstate-relation-cache",
            "/tmp/cache.tsv",
        ]);
        assert!(parse.is_err(), "a derive step should be required");
    }

    #[test]
    fn parses_all_subcommand() {
        let cli = Cli::try_parse_from([
            "openinterstate-derive",
            "--database-url",
            "postgres://db",
            "--interstate-relation-cache",
            "/tmp/cache.tsv",
            "all",
        ])
        .expect("parse all subcommand");
        assert_eq!(cli.command, Command::All);
    }

    #[test]
    fn parses_corridors_subcommand() {
        let cli = Cli::try_parse_from([
            "openinterstate-derive",
            "--database-url",
            "postgres://db",
            "--interstate-relation-cache",
            "/tmp/cache.tsv",
            "corridors",
        ])
        .expect("parse corridors subcommand");
        assert_eq!(cli.command, Command::Corridors);
    }
}
