/// Pinned Producer Benchmark (Single-threaded, one producer)
/// Tests: Sequential message publishing, single connection
/// 
/// Usage: cargo r --bin iggy-bench -r -- pinned-producer tcp [--messages N] [--batch-size N] [--output DIR]
///
/// This benchmark measures:
/// - Single-threaded producer throughput
/// - Message batching efficiency
/// - TCP connection performance
/// - Serialization overhead

use anyhow::Result;
use bytes::Bytes;
use clap::{Parser, Subcommand};
use iggy::client::{Client, MessageClient, UserClient};
use iggy::clients::client::IggyClient;
use iggy::identifier::Identifier;
use iggy::messages::send_messages::{Message, Partitioning};
use serde::{Deserialize, Serialize};
use std::time::{Duration, Instant};
use std::fs;
use std::path::PathBuf;

#[derive(Parser)]
#[command(name = "iggy-bench")]
#[command(about = "Iggy Message Broker Benchmarking Tool", long_about = None)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Pinned producer benchmark (single-threaded)
    PinnedProducer {
        /// Protocol: tcp or http
        protocol: String,
        
        /// Number of messages to send
        #[arg(long, default_value = "100000")]
        messages: usize,
        
        /// Batch size
        #[arg(long, default_value = "1000")]
        batch_size: usize,
        
        /// Message size in bytes
        #[arg(long, default_value = "1024")]
        message_size: usize,
        
        /// Output directory for results
        #[arg(long)]
        output: Option<PathBuf>,
        
        /// Benchmark identifier
        #[arg(long, default_value = "local")]
        identifier: String,
    },
    
    /// Producer benchmark (multi-threaded)
    Producer {
        protocol: String,
        #[arg(long, default_value = "100000")]
        messages: usize,
        #[arg(long, default_value = "1000")]
        batch_size: usize,
        #[arg(long, default_value = "4")]
        producers: usize,
    },
    
    /// Consumer benchmark
    Consumer {
        protocol: String,
        #[arg(long, default_value = "4")]
        consumers: usize,
    },
}

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
    system_info: SystemInfo,
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

#[derive(Serialize, Deserialize)]
struct SystemInfo {
    os: String,
    arch: String,
    cores: usize,
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();

    match cli.command {
        Commands::PinnedProducer {
            protocol,
            messages,
            batch_size,
            message_size,
            output,
            identifier,
        } => {
            run_pinned_producer(&protocol, messages, batch_size, message_size, output, &identifier).await?;
        }
        Commands::Producer {
            protocol,
            messages,
            batch_size,
            producers,
        } => {
            println!("Multi-producer benchmark not yet implemented");
        }
        Commands::Consumer {
            protocol,
            consumers,
        } => {
            println!("Consumer benchmark not yet implemented");
        }
    }

    Ok(())
}

async fn run_pinned_producer(
    protocol: &str,
    total_messages: usize,
    batch_size: usize,
    message_size: usize,
    output_dir: Option<PathBuf>,
    identifier: &str,
) -> Result<()> {
    println!("======================================================");
    println!("Pinned Producer Benchmark (Iggy)");
    println!("======================================================");
    println!();
    println!("Configuration:");
    println!("  Protocol:      {}", protocol);
    println!("  Messages:      {}", total_messages);
    println!("  Batch Size:    {}", batch_size);
    println!("  Message Size:  {} bytes", message_size);
    println!("  Identifier:    {}", identifier);
    println!();

    // Connect to Iggy
    let client = IggyClient::default();
    client.connect().await?;
    client.login_user("iggy", "iggy").await?;
    println!("Connected to Iggy ({})", protocol);

    let stream_id = 1;
    let topic_id = 1;
    let partition_id = 1;

    let num_batches = total_messages / batch_size;
    let mut latencies = Vec::with_capacity(num_batches);

    println!("\nStarting benchmark...\n");
    let overall_start = Instant::now();

    for batch_num in 0..num_batches {
        let mut messages = Vec::with_capacity(batch_size);
        
        // Create batch
        for i in 0..batch_size {
            let msg_id = batch_num * batch_size + i;
            let payload = vec![0u8; message_size];
            messages.push(Message::new(None, Bytes::from(payload), None));
        }

        // Measure batch send time
        let batch_start = Instant::now();
        
        client.send_messages(
            &Identifier::numeric(stream_id)?,
            &Identifier::numeric(topic_id)?,
            &Partitioning::partition_id(partition_id),
            &mut messages,
        ).await?;
        
        let batch_latency = batch_start.elapsed();
        latencies.push(batch_latency);

        if (batch_num + 1) % 10 == 0 {
            print!("\rProgress: {}/{} batches ({} messages)", 
                batch_num + 1, num_batches, (batch_num + 1) * batch_size);
            use std::io::Write;
            std::io::stdout().flush()?;
        }
    }

    let total_duration = overall_start.elapsed();
    
    println!("\rBenchmark completed.                                  ");
    println!();

    // Calculate statistics
    let throughput_msg_sec = total_messages as f64 / total_duration.as_secs_f64();
    let throughput_mb_sec = throughput_msg_sec * message_size as f64 / 1024.0 / 1024.0;

    latencies.sort();
    let min_lat = latencies.first().unwrap().as_secs_f64() * 1000.0;
    let max_lat = latencies.last().unwrap().as_secs_f64() * 1000.0;
    let avg_lat = latencies.iter().sum::<Duration>().as_secs_f64() * 1000.0 / latencies.len() as f64;
    let p50_lat = latencies[latencies.len() / 2].as_secs_f64() * 1000.0;
    let p95_lat = latencies[latencies.len() * 95 / 100].as_secs_f64() * 1000.0;
    let p99_lat = latencies[latencies.len() * 99 / 100].as_secs_f64() * 1000.0;

    // Display results
    println!("======================================================");
    println!("Benchmark Results");
    println!("======================================================");
    println!();
    println!("Throughput:");
    println!("  {:.0} messages/sec", throughput_msg_sec);
    println!("  {:.2} MB/sec", throughput_mb_sec);
    println!();
    println!("Latency (per batch of {} messages):", batch_size);
    println!("  Min:  {:.2} ms", min_lat);
    println!("  Avg:  {:.2} ms", avg_lat);
    println!("  P50:  {:.2} ms", p50_lat);
    println!("  P95:  {:.2} ms", p95_lat);
    println!("  P99:  {:.2} ms", p99_lat);
    println!("  Max:  {:.2} ms", max_lat);
    println!();
    println!("Average per-message latency: {:.3} ms", avg_lat / batch_size as f64);
    println!();

    // Save results if output directory specified
    if let Some(output_path) = output_dir {
        let results = BenchmarkResults {
            benchmark_type: "pinned-producer".to_string(),
            identifier: identifier.to_string(),
            timestamp: chrono::Utc::now().to_rfc3339(),
            total_messages,
            total_duration_ms: total_duration.as_secs_f64() * 1000.0,
            throughput_msg_sec,
            throughput_mb_sec,
            latencies: LatencyStats {
                min_ms: min_lat,
                max_ms: max_lat,
                avg_ms: avg_lat,
                p50_ms: p50_lat,
                p95_ms: p95_lat,
                p99_ms: p99_lat,
            },
            system_info: SystemInfo {
                os: std::env::consts::OS.to_string(),
                arch: std::env::consts::ARCH.to_string(),
                cores: num_cpus::get(),
            },
        };

        fs::create_dir_all(&output_path)?;
        let json_path = output_path.join(format!("pinned-producer-{}.json", identifier));
        fs::write(&json_path, serde_json::to_string_pretty(&results)?)?;
        println!("Results saved to: {:?}", json_path);
    }

    println!("======================================================");

    Ok(())
}
