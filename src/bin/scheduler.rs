use anyhow::Result;
use ballista_scheduler::cluster::BallistaCluster;
use ballista_scheduler::config::SchedulerConfig;
use ballista_scheduler::scheduler_process::start_server;
use clap::Parser;
use polars_bio_ballista_poc::codec::PolarsBioBallistaLogicalCodec;
use std::net::SocketAddr;
use std::sync::Arc;

#[derive(Parser, Debug)]
struct Args {
    #[arg(long, default_value = "127.0.0.1")]
    bind_host: String,
    #[arg(long, default_value = "127.0.0.1")]
    external_host: String,
    #[arg(long, default_value_t = 50050)]
    bind_port: u16,
    #[arg(long, default_value = "ballista-poc")]
    namespace: String,
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    let addr: SocketAddr = format!("{}:{}", args.bind_host, args.bind_port).parse()?;

    let mut config = SchedulerConfig::default()
        .with_namespace(args.namespace)
        .with_hostname(args.external_host)
        .with_port(args.bind_port);
    config.bind_host = args.bind_host;
    config.override_logical_codec = Some(Arc::new(PolarsBioBallistaLogicalCodec::default()));

    println!(
        "starting Ballista scheduler on {} for namespace {}",
        addr, config.namespace
    );

    let cluster = BallistaCluster::new_from_config(&config).await?;
    start_server(cluster, addr, Arc::new(config)).await?;
    Ok(())
}
