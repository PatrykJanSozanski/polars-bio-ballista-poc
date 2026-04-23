use anyhow::{Context, Result, bail};
use ballista::prelude::SessionContextExt;
use clap::Parser;
use datafusion::arrow::util::pretty::pretty_format_batches;
use datafusion::prelude::SessionContext;
use polars_bio_ballista_poc::fixtures::prepare_fixtures;
use polars_bio_ballista_poc::overlap::register_overlap_udtf;
use std::path::{Path, PathBuf};

#[derive(Parser, Debug)]
struct Args {
    #[arg(long, default_value = "df://127.0.0.1:50050")]
    scheduler_url: String,
    #[arg(long, default_value = "fixtures/generated")]
    fixtures_dir: PathBuf,
    #[arg(long)]
    left: Option<PathBuf>,
    #[arg(long)]
    right: Option<PathBuf>,
    #[arg(long)]
    explain: bool,
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    let (left_path, right_path) = resolve_fixture_paths(&args)?;

    println!("connecting to scheduler {}", args.scheduler_url);
    let ctx: SessionContext = SessionContext::remote(&args.scheduler_url)
        .await
        .context("connecting to remote Ballista scheduler")?;
    register_overlap_udtf(&ctx);

    let sql = format!(
        "SELECT * FROM overlap_demo('{}', '{}') ORDER BY left_id, right_id",
        escape_sql_literal(&left_path),
        escape_sql_literal(&right_path)
    );

    if args.explain {
        let explain_sql = format!("EXPLAIN VERBOSE {sql}");
        let explain_batches = ctx.sql(&explain_sql).await?.collect().await?;
        println!("{}", pretty_format_batches(&explain_batches)?);
    }

    let batches = ctx.sql(&sql).await?.collect().await?;
    println!("{}", pretty_format_batches(&batches)?);

    Ok(())
}

fn resolve_fixture_paths(args: &Args) -> Result<(String, String)> {
    match (&args.left, &args.right) {
        (Some(left), Some(right)) => Ok((
            canonicalize(left)?.display().to_string(),
            canonicalize(right)?.display().to_string(),
        )),
        (None, None) => {
            let (left, right) = prepare_fixtures(&args.fixtures_dir)?;
            Ok((left.display().to_string(), right.display().to_string()))
        }
        _ => bail!("provide both --left and --right or neither of them"),
    }
}

fn canonicalize(path: &Path) -> Result<PathBuf> {
    path.canonicalize()
        .with_context(|| format!("canonicalizing path {}", path.display()))
}

fn escape_sql_literal(value: &str) -> String {
    value.replace('\'', "''")
}
