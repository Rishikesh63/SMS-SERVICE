#  Rust SMS + AI Messaging Platform

A high-performance, modular Rust system for storing, processing, and interacting with conversation messages using **Turso**, **Iggy**, **Groq**, and **SignalWire**, served via **Axum**.

---

#  Architecture

This system is designed as a scalable pipeline:

**SMS / API → Iggy Broker → Consumers → Database + AI → SMS Reply**

---

##  Main Components & File Purposes

| File | Responsibility |
|------|---------------|
| `src/main.rs` | Example CLI for storing and retrieving conversations |
| `src/lib.rs` | Library root and Turso connector |
| `src/models.rs` | Data models for conversations and messages |
| `src/store.rs` | All Turso database operations |
| `src/message_broker.rs` | Iggy broker client and publishing |
| `src/ai_service.rs` | AI message generation via Groq |
| `src/signalwire.rs` | SMS sending client |
| `src/consumers.rs` | Consumers for processing messages |
| `src/zero_copy.rs` | Zero-copy serialization utilities |
| `src/sms_server.rs` | Axum HTTP server |
| `src/bin/iggy_bench.rs` | Benchmark tool for measuring Iggy broker performance (throughput, latency, batching). Useful for testing and optimization. |
| `src/producer/main.rs` | Example producer |
| **Producer** (`src/producer/main.rs`) | Generates and sends SMS messages into the system using the MessageBroker. |
| `src/consumer/main.rs` | Launches TursoConsumer and AIConsumer from consumers.rs |
| **Consumer** (`src/consumer/main.rs`) | Entry point that runs both TursoConsumer (stores messages in Turso DB) and AIConsumer (generates AI replies and sends SMS) using the shared logic in consumers.rs. |
| `src/consumers.rs` | Contains TursoConsumer and AIConsumer implementations for modular message processing. |

Each module follows a single-responsibility principle, making the system easy to extend and maintain.

---

# 🛠️ Setup & Run Guide

---

## 1. Install Rust

```bash
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
```

---

## 2. Configure Environment

```bash
cp .env.example .env
```

Update `.env`:

```env
TURSO_DATABASE_URL=libsql://your-database.turso.io
TURSO_AUTH_TOKEN=your-auth-token-here
GROQ_API_KEY=your-groq-api-key-here
AI_MODEL=your-model-name
SIGNALWIRE_PROJECT_ID=your-project-id
SIGNALWIRE_AUTH_TOKEN=your-auth-token
SIGNALWIRE_SPACE_URL=your-space.signalwire.com
SIGNALWIRE_FROM_NUMBER=+1234567890
```

---


## 4. Build & Run SMS Server

For Linux/macOS:
```bash
./start-sms-server.sh
```

For Windows:
```bat
start-sms-server.bat
```

These scripts check for required environment files and Rust installation, then build and start the SMS server automatically.

---
## 3. Start Iggy Broker
```bash
docker pull apache/iggy:latest
docker run --rm -p 8090:8090 apache/iggy:latest
```
---

## 4. Build Project

```bash
cargo build --release
```

---

## 5. Run SMS Server

```bash
cargo run --bin sms-server --release
```

---

## 6. (Optional) Expose Localhost

```bash
ngrok http 3000
```

---

#  Benchmarking

Run Iggy performance tests:

```bash
cargo run --bin iggy-bench -- pinned-producer tcp
```

---

#  Key Features

* High-throughput message streaming with **Iggy**
* Durable storage via **Turso**
* AI responses using **Groq**
* SMS integration through **SignalWire**
* Zero-copy message processing
* Modular and scalable design

---