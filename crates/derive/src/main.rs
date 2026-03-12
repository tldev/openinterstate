mod canonical_types;
mod graph;
mod routes;

use clap::{Parser, Subcommand};
use sqlx::PgPool;
use std::path::{Path, PathBuf};

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

    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand, PartialEq, Eq)]
enum Command {
    /// Run graph, corridor, and reference-route builders in sequence.
    All,
    /// Rebuild highway_edges and exit_corridors from canonical osm2pgsql tables.
    Graph,
    /// Rebuild corridors and corridor_exits from highway_edges.
    Corridors {
        /// Optional comma-separated highway filter for corridor debugging.
        #[arg(long)]
        highway_filter: Option<String>,

        /// Optional GeoJSON output when highway_filter is set.
        #[arg(long)]
        geojson_out: Option<PathBuf>,
    },
    /// Rebuild reference_routes and reference_route_anchors from corridors.
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
    let pool = connect_pool(&cli.database_url).await?;
    tracing::info!("Connected to database");

    match cli.command {
        Command::All => run_all(&pool).await,
        Command::Graph => run_graph(&pool).await,
        Command::Corridors {
            highway_filter,
            geojson_out,
        } => run_corridors(&pool, highway_filter.as_deref(), geojson_out.as_deref()).await,
        Command::Routes => run_routes(&pool).await,
    }
}

async fn connect_pool(database_url: &str) -> anyhow::Result<PgPool> {
    Ok(sqlx::postgres::PgPoolOptions::new()
        .max_connections(20)
        .connect(database_url)
        .await?)
}

async fn run_all(pool: &PgPool) -> anyhow::Result<()> {
    run_graph(pool).await?;
    run_corridors(pool, None, None).await?;
    run_routes(pool).await
}

async fn run_graph(pool: &PgPool) -> anyhow::Result<()> {
    tracing::info!("Building highway graph from canonical osm2pgsql tables");
    let edge_count = graph::build_graph(pool).await?;
    tracing::info!("Graph build complete: {} edges", edge_count);
    Ok(())
}

async fn run_corridors(
    pool: &PgPool,
    highway_filter: Option<&str>,
    geojson_out: Option<&Path>,
) -> anyhow::Result<()> {
    if let Some(filter) = highway_filter {
        let highways: Vec<&str> = filter
            .split(',')
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .collect();
        let geojson_path = geojson_out.and_then(|path| path.to_str());
        tracing::info!("Building filtered corridors for {:?}", highways);
        graph::corridors::build_corridors_lab(pool, &highways, geojson_path).await?;
    } else {
        tracing::info!("Building corridors from highway_edges");
        let stats = graph::corridors::build_corridors(pool).await?;
        tracing::info!(
            "Corridor build complete: {} corridors, {} exits, {} edges updated",
            stats.corridors_created,
            stats.corridor_exits_created,
            stats.edges_updated
        );
    }
    Ok(())
}

async fn run_routes(pool: &PgPool) -> anyhow::Result<()> {
    tracing::info!("Building reference routes from corridors");
    routes::build_reference_routes(pool).await?;
    tracing::info!("Reference route build complete");
    Ok(())
}

#[cfg(test)]
mod tests {
    use clap::Parser;
    use std::path::PathBuf;

    use super::{Cli, Command};

    #[test]
    fn requires_a_subcommand() {
        let parse = Cli::try_parse_from(["openinterstate-derive", "--database-url", "postgres://db"]);
        assert!(parse.is_err(), "a derive step should be required");
    }

    #[test]
    fn parses_all_subcommand() {
        let cli = Cli::try_parse_from([
            "openinterstate-derive",
            "--database-url",
            "postgres://db",
            "all",
        ])
        .expect("parse all subcommand");
        assert_eq!(cli.command, Command::All);
    }

    #[test]
    fn parses_filtered_corridor_build() {
        let cli = Cli::try_parse_from([
            "openinterstate-derive",
            "--database-url",
            "postgres://db",
            "corridors",
            "--highway-filter",
            "I-40,I-75",
            "--geojson-out",
            "build/debug/corridors.geojson",
        ])
        .expect("parse corridors subcommand");

        assert_eq!(
            cli.command,
            Command::Corridors {
                highway_filter: Some("I-40,I-75".to_string()),
                geojson_out: Some(PathBuf::from("build/debug/corridors.geojson")),
            }
        );
    }
}
