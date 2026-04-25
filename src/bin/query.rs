use anyhow::{Context, Result};
use ballista::prelude::{SessionConfigExt, SessionContextExt};
use clap::{Parser, ValueEnum};
use datafusion::arrow::util::pretty::pretty_format_batches;
use datafusion::execution::SessionStateBuilder;
use datafusion::prelude::{SessionConfig, SessionContext};
use polars_bio_ballista_poc::codec::PolarsBioBallistaLogicalCodec;
use polars_bio_ballista_poc::fixtures::ensure_e1_toy_parquet_fixtures;
use polars_bio_ballista_poc::operation::{
    do_approach_a_overlap, do_approach_b_overlap, do_polars_bio_direct_overlap,
    do_polars_bio_style_overlap, register_polars_bio_parquet_inputs,
};
use polars_bio_ballista_poc::overlap::register_overlap_udtf;
use std::sync::Arc;

const DEFAULT_LEFT: &str =
    "fixtures/polars-bio/exons/part-00001-47fafbb5-1cab-410c-9461-d10effacf760-c000.snappy.parquet";
const DEFAULT_RIGHT: &str =
    "fixtures/polars-bio/fBrain-DS14718/part-00001-a0d75244-2d87-41eb-a3eb-a18847c7cb87-c000.snappy.parquet";

#[derive(Clone, Copy, Debug, ValueEnum)]
enum ProviderMode {
    E1,
    Serialized,
    ApproachA,
    ApproachB,
    Direct,
}

#[derive(Parser, Debug)]
struct Args {
    #[arg(long, default_value = "df://127.0.0.1:50050")]
    scheduler_url: String,
    #[arg(long, default_value = DEFAULT_LEFT)]
    left: String,
    #[arg(long, default_value = DEFAULT_RIGHT)]
    right: String,
    #[arg(long)]
    explain: bool,
    #[arg(long, default_value_t = 20)]
    limit: usize,
    #[arg(long, value_enum, default_value_t = ProviderMode::Serialized)]
    provider_mode: ProviderMode,
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();

    println!("connecting to scheduler {}", args.scheduler_url);
    let session_config = SessionConfig::new_with_ballista()
        .with_ballista_logical_extension_codec(Arc::new(PolarsBioBallistaLogicalCodec::default()));
    let state = SessionStateBuilder::new()
        .with_default_features()
        .with_config(session_config)
        .build();
    let ctx: SessionContext = SessionContext::remote_with_state(&args.scheduler_url, state)
        .await
        .context("connecting to remote Ballista scheduler")?;

    println!("building overlap logical plan");
    let df = match args.provider_mode {
        ProviderMode::E1 => {
            let (left, right) = ensure_e1_toy_parquet_fixtures()
                .await
                .context("preparing toy parquet fixtures for e1")?;
            register_overlap_udtf(&ctx);
            let sql = format!(
                "SELECT * FROM overlap_demo('{}', '{}') ORDER BY left_id, right_id",
                left.replace('\'', "''"),
                right.replace('\'', "''")
            );
            ctx.sql(&sql)
                .await
                .context("building e1 overlap_demo plan")?
        }
        ProviderMode::Serialized => {
            println!("registering parquet fixtures");
            register_polars_bio_parquet_inputs(&ctx, &args.left, &args.right)
                .await
                .context("registering polars-bio parquet fixtures")?;
            do_polars_bio_style_overlap(&ctx, &args.left, &args.right)
                .await
                .context("building serialized overlap provider plan")?
        }
        ProviderMode::ApproachA => {
            println!("registering parquet fixtures");
            register_polars_bio_parquet_inputs(&ctx, &args.left, &args.right)
                .await
                .context("registering polars-bio parquet fixtures")?;
            do_approach_a_overlap(&ctx, &args.left, &args.right)
                .await
                .context("building approach-a overlap provider plan")?
        }
        ProviderMode::ApproachB => {
            println!("registering parquet fixtures");
            register_polars_bio_parquet_inputs(&ctx, &args.left, &args.right)
                .await
                .context("registering polars-bio parquet fixtures")?;
            do_approach_b_overlap(&ctx, &args.left, &args.right)
                .await
                .context("building approach-b overlap provider plan")?
        }
        ProviderMode::Direct => {
            println!("registering parquet fixtures");
            register_polars_bio_parquet_inputs(&ctx, &args.left, &args.right)
                .await
                .context("registering polars-bio parquet fixtures")?;
            do_polars_bio_direct_overlap(&ctx)
                .await
                .context("building direct overlap provider plan")?
        }
    };
    let df = if args.limit == 0 {
        df
    } else {
        df.limit(0, Some(args.limit))?
    };

    if args.explain {
        let explain_batches = df.clone().explain(true, false)?.collect().await?;
        println!("{}", pretty_format_batches(&explain_batches)?);
    }

    println!("collecting result batches");
    let batches = df.collect().await?;
    println!("{}", pretty_format_batches(&batches)?);

    Ok(())
}
