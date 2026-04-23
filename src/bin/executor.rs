use anyhow::Result;
use ballista_executor::executor_process::{ExecutorProcessConfig, start_executor_process};
use clap::Parser;
use std::fs;
use std::path::PathBuf;
use std::sync::Arc;

#[derive(Parser, Debug)]
struct Args {
    #[arg(long, default_value = "127.0.0.1")]
    bind_host: String,
    #[arg(long, default_value = "127.0.0.1")]
    external_host: String,
    #[arg(long, default_value_t = 50051)]
    port: u16,
    #[arg(long, default_value_t = 50052)]
    grpc_port: u16,
    #[arg(long, default_value = "127.0.0.1")]
    scheduler_host: String,
    #[arg(long, default_value_t = 50050)]
    scheduler_port: u16,
    #[arg(long, default_value_t = 2)]
    concurrent_tasks: usize,
    #[arg(long, default_value = "target/ballista/executor-1")]
    work_dir: PathBuf,
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    fs::create_dir_all(&args.work_dir)?;

    let config = ExecutorProcessConfig {
        bind_host: args.bind_host,
        external_host: Some(args.external_host),
        port: args.port,
        grpc_port: args.grpc_port,
        scheduler_host: args.scheduler_host,
        scheduler_port: args.scheduler_port,
        concurrent_tasks: args.concurrent_tasks,
        work_dir: Some(args.work_dir.display().to_string()),
        ..Default::default()
    };

    println!(
        "starting Ballista executor flight={} grpc={} work_dir={}",
        config.port,
        config.grpc_port,
        config.work_dir.as_deref().unwrap_or("<temp>")
    );

    start_executor_process(Arc::new(config)).await?;
    Ok(())
}
