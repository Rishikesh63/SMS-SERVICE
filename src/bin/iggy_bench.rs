use anyhow::Result;
use bytes::Bytes;
use clap::{Parser, Subcommand};
use iggy::clients::client::IggyClient;
use iggy::prelude::*;
use serde::{Deserialize, Serialize};
use std::fs;
use std::path::PathBuf;
use std::time::{Duration, Instant};

/// =============================
/// CLI
/// =============================
#[derive(Parser)]
#[command(name = "iggy-bench")]
#[command(about = "Iggy High-Level Benchmark Tool")]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Single-threaded pinned producer benchmark
    PinnedProducer {
        #[arg(default_value = "tcp")]
        protocol: String,

        #[arg(long, default_value = "100000")]
        messages: usize,

        #[arg(long, default_value = "1000")]
        batch_size: usize,

        #[arg(long, default_value = "1024")]
        message_size: usize,

        #[arg(long)]
        output: Option<PathBuf>,

        #[arg(long, default_value = "local")]
        identifier: String,
    },
}

/// =============================
/// Benchmark Data
/// =============================
#[derive(Serialize, Deserialize)]
struct BenchmarkResults {
    benchmark_type: String,
    identifier: String,
    timestamp: String,
    total_messages: usize,
    total_duration_ms: f64,
    throughput_msg_sec: f64,
    throughput_mb_sec: f64,
    latencies: LatencyStats,
}

#[derive(Serialize, Deserialize)]
struct LatencyStats {
    min_ms: f64,
    max_ms: f64,
    avg_ms: f64,
    p50_ms: f64,
    p95_ms: f64,
    p99_ms: f64,
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();

    match cli.command {
        Commands::PinnedProducer {
            messages,
            batch_size,
            message_size,
            output,
            identifier,
            ..
        } => {
            run_pinned_producer(messages, batch_size, message_size, output, &identifier).await?;
        }
    }

    Ok(())
}

/// =============================
/// Benchmark Logic
/// =============================
async fn run_pinned_producer(
    total_messages: usize,
    batch_size: usize,
    message_size: usize,
    output_dir: Option<PathBuf>,
    identifier: &str,
) -> Result<()> {
    println!("======================================================");
    println!("Pinned Producer Benchmark (High-Level Iggy)");
    println!("======================================================");

    let client = IggyClient::from_connection_string(
        "iggy://iggy:iggy@localhost:8090"
    )?;
    client.connect().await?;

    let mut producer = client
        .producer("bench_stream", "bench_topic")?
        .direct(
            DirectConfig::builder()
                .batch_length(batch_size as u32)
                .linger_time(IggyDuration::new(Duration::from_millis(1)))
                .build(),
        )
        .partitioning(Partitioning::balanced())
        .build();

    producer.init().await?;

    let num_batches = total_messages / batch_size;
    let mut latencies = Vec::with_capacity(num_batches);

    println!("Sending {} messages in {} batches\n", total_messages, num_batches);

    let overall_start = Instant::now();

    for batch_idx in 0..num_batches {
        let mut batch = Vec::with_capacity(batch_size);

        for _ in 0..batch_size {
            batch.push(
                IggyMessage::from_bytes(Bytes::from(vec![0u8; message_size]))?
            );
        }

        let batch_start = Instant::now();
        producer.send(batch).await?;
        latencies.push(batch_start.elapsed());

        if (batch_idx + 1) % 10 == 0 {
            print!(
                "\rProgress: {}/{} batches",
                batch_idx + 1,
                num_batches
            );
            use std::io::Write;
            std::io::stdout().flush()?;
        }
    }

    let total_duration = overall_start.elapsed();
    println!("\n\nBenchmark completed\n");

    // -----------------------------
    // Stats
    // -----------------------------
    latencies.sort();
    let to_ms = |d: Duration| d.as_secs_f64() * 1000.0;

    let min = to_ms(latencies[0]);
    let max = to_ms(*latencies.last().unwrap());
    let avg = latencies.iter().map(|d| to_ms(*d)).sum::<f64>() / latencies.len() as f64;
    let p50 = to_ms(latencies[latencies.len() / 2]);
    let p95 = to_ms(latencies[latencies.len() * 95 / 100]);
    let p99 = to_ms(latencies[latencies.len() * 99 / 100]);

    let throughput_msg_sec = total_messages as f64 / total_duration.as_secs_f64();
    let throughput_mb_sec =
        throughput_msg_sec * message_size as f64 / 1024.0 / 1024.0;

    println!("Throughput:");
    println!("  {:.0} msg/sec", throughput_msg_sec);
    println!("  {:.2} MB/sec\n", throughput_mb_sec);

    println!("Latency per batch ({} msgs):", batch_size);
    println!("  Min: {:.2} ms", min);
    println!("  Avg: {:.2} ms", avg);
    println!("  P50: {:.2} ms", p50);
    println!("  P95: {:.2} ms", p95);
    println!("  P99: {:.2} ms", p99);
    println!("  Max: {:.2} ms\n", max);

    if let Some(dir) = output_dir {
        fs::create_dir_all(&dir)?;
        let result = BenchmarkResults {
            benchmark_type: "pinned-producer".into(),
            identifier: identifier.into(),
            timestamp: chrono::Utc::now().to_rfc3339(),
            total_messages,
            total_duration_ms: total_duration.as_secs_f64() * 1000.0,
            throughput_msg_sec,
            throughput_mb_sec,
            latencies: LatencyStats {
                min_ms: min,
                max_ms: max,
                avg_ms: avg,
                p50_ms: p50,
                p95_ms: p95,
                p99_ms: p99,
            },
        };

        let path = dir.join(format!("pinned-producer-{}.json", identifier));
        fs::write(&path, serde_json::to_string_pretty(&result)?)?;
        println!("Results written to {:?}", path);
    }

    println!("======================================================");
    Ok(())
}
