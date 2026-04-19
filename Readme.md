<div align="center">

# ⚡ Adaptive Real-Time Stream Analytics
### Using Probabilistic Sketches with Load-Aware Request Routing

**BTP Project · LNMIIT**  
*Palak Khemchandani · Naman Aggrawal · Abhinav Dogra*

[![Python](https://img.shields.io/badge/Python-3.11%2B-blue?logo=python&logoColor=white)](https://python.org)
[![FastAPI](https://img.shields.io/badge/FastAPI-0.110-009688?logo=fastapi&logoColor=white)](https://fastapi.tiangolo.com)
[![Kafka](https://img.shields.io/badge/Kafka-7.5.0-231F20?logo=apachekafka&logoColor=white)](https://kafka.apache.org)
[![Redis](https://img.shields.io/badge/Redis-7-DC382D?logo=redis&logoColor=white)](https://redis.io)
[![Tests](https://img.shields.io/badge/Tests-28%20passed-22c55e?logo=pytest&logoColor=white)](#running-tests)
[![Future Scope](https://img.shields.io/badge/Research-Future%20Scope-black?logo=gitbook&logoColor=white)](futurescope.md)

</div>

---

## Table of Contents

1. [What This Project Does](#what-this-project-does)
2. [Architecture Overview](#architecture-overview)
3. [How Each Part Works](#how-each-part-works)
   - [Probabilistic Sketches](#probabilistic-sketches)
   - [Adaptive Controller](#adaptive-controller)
   - [Load-Aware Router](#load-aware-router)
   - [Future Scope & Research Directions](futurescope.md)
   - [Stream Ingestion (Kafka)](#stream-ingestion-kafka)
   - [State Store (Redis)](#state-store-redis)
   - [Analytics API (FastAPI)](#analytics-api-fastapi)
   - [Web Dashboard](#web-dashboard)
   - [C++ Worker](#c-worker)
4. [Quick Start](#quick-start)
5. [Running Components](#running-components)
6. [Dashboard Guide — What Each Section Shows](#dashboard-guide)
7. [Testing Guide — Sample Queries & Verification](#testing-guide)
8. [Running Tests](#running-tests)
9. [Configuration Reference](#configuration-reference)
10. [API Reference](#api-reference)
11. [Sketch Parameter Templates](#sketch-parameter-templates)
12. [Running the Simulation](#running-the-simulation)
13. [Project Structure](#project-structure)
14. [How the Math Works](#how-the-math-works)
15. [Troubleshooting](#troubleshooting)

---

## What This Project Does

This system processes a **high-velocity stream of events** (millions per second) in real time and answers three core questions — **without storing every event**:

| Query | Algorithm | Guarantee |
|---|---|---|
| How often did `item_X` appear? | Count-Min Sketch | ≤ ε·N additive error |
| How many distinct items exist? | HyperLogLog | ≈ 1.04/√m relative error |
| Which items appear most often? | Misra–Gries | Top-k with bounded error |

On top of that, it **self-tunes** in real time:
- A **controller** monitors P99 latency and memory pressure, and switches sketch precision up/down automatically.
- A **load-aware router** picks the best worker node to answer each query based on latency, memory, and accuracy capability.

---

## Architecture Overview

```
                              ┌─────────────────────────────┐
  Event                       │     Adaptive Controller      │
  Stream ──► Kafka Topic ────►│  (tunes sketch params every  │
             (stream_events)  │   5 s based on thresholds)   │
                    │         └─────────────┬───────────────┘
                    ▼                       │ set_template commands
          ┌─────────────────┐               │
          │ Analytics Worker│◄──────────────┘
          │  (CMS, HLL, MG) │
          └────────┬────────┘
                   │ writes metrics & sketch state
                   ▼
          ┌─────────────────┐
          │ Redis State Store│◄──────── Set params / read metrics
          └────────┬────────┘
                   │
          ┌────────▼────────┐
          │ FastAPI API      │◄──── HTTP Queries
          │  + Dashboard     │          │
          └────────┬────────┘          ▼
                   │         ┌──────────────────┐
                   └────────►│ Load-Aware Router │
                             │  (scores workers, │
                             │   picks the best) │
                             └──────────────────┘
```

---

## How Each Part Works

### Probabilistic Sketches

These live inside the **analytics worker** and process every Kafka event:

#### Count-Min Sketch (CMS)
A 2D array of counters with **w** columns and **d** rows. Each event `x` increments `d` counters (one per hash function). To query frequency: return the **minimum** of those `d` counters.

- **Error guarantee:** `f̂(x) ≤ f(x) + ε·N` with probability `1 − δ`
- **Parameters:** `w = ⌈e/ε⌉` columns, `d = ⌈ln(1/δ)⌉` rows
- **Memory:** O(w·d) integers
- **Update cost:** O(d) hash operations

```
Low template : w=512,  d=3  → ε=0.0053, δ=0.05
Med template : w=2048, d=5  → ε=0.0013, δ=0.007
High template: w=8192, d=7  → ε=0.0003, δ=0.0009
```

#### HyperLogLog (HLL)
Uses **m = 2^p** registers. For each element, hash it, take the first `p` bits as a register index, and record the position of the **leftmost 1-bit** in the remainder. Cardinality is estimated via harmonic mean of `2^registers[i]`.

- **Error:** `≈ 1.04 / √m` relative standard error
- **Memory:** O(m) bytes (one 6-bit register per slot)

```
p=10 → 1,024 registers  → ~1.6 KB, ±3.2% error
p=12 → 4,096 registers  → ~6.4 KB, ±1.6% error
p=14 → 16,384 registers → ~25 KB,  ±0.8% error
```

#### Misra–Gries (Heavy Hitters)
Maintains at most `k−1` candidate items. When a new item arrives:
- If it's already a candidate → increment its count
- Else if there are fewer than `k−1` candidates → add it with count 1
- Else → decrement all counts by 1 and evict zeros

Items surviving this process appear more than `N/k` times. Error ≤ `N/k`.

---

### Adaptive Controller

Runs as a **background asyncio task** in the API server, triggered every `CONTROLLER_INTERVAL_SEC` seconds (default: 5 s).

It reads metrics for each registered worker from Redis and applies these rules **in order**:

```
if   P99 latency > LATENCY_HIGH_MS:   → reduce_precision(worker)   [downgrade template]
elif CMS error > ERROR_HIGH
     AND memory is available:          → increase_precision(worker)  [upgrade template]
elif memory usage > MEMORY_HIGH_PERCENT: → offload_state(worker)    [shrink MG window / reduce w]
```

The controller writes commands back to Redis (`worker:{id}:cmd`). Workers poll this key and hot-swap their sketch parameters without dropping events.

---

### Load-Aware Router

Every incoming query is scored against all alive worker nodes using **Equation 3** from the paper:

```
score(node, query) =  w1 × (capability / query.accuracy_req)
                    − w2 × (p95_latency / query.latency_sla)
                    − w3 × memory_percent
```

The query is forwarded to the **highest-scoring node**. Default weights: `w1=0.5, w2=0.3, w3=0.2`.

`capability` is a normalised score computed from the current sketch template:
- `low → 0.33`    (fast, less memory, lower accuracy)
- `medium → 0.66` (balanced)
- `high → 1.0`    (most accurate, most memory)

---

### Stream Ingestion (Kafka)

`stream/producer.py` generates synthetic events following a **Zipfian distribution** (heavy-tailed, like real web traffic) and publishes them as JSON to the Kafka topic `stream_events`.

```json
{"key": "item_42", "ts": 1710000000.123}
```

The analytics worker(s) consume from this topic, update their local sketch instances, and periodically write aggregated metrics to Redis.

> **Architecture Note:** Each worker uses a **unique Kafka consumer group** (`btp-analytics-workers-{worker_id}`). This means every worker receives **ALL events** independently — this is an analytics fan-out where each node maintains its own full sketch, not a partitioned workload.

**Why Zipf?** Real-world traffic is heavily skewed — a handful of URLs, users, or products account for the majority of events. Zipf(s=1.2) simulates this skew.

---

### Concept Drift Simulation

Standard analytics systems often fail when the "Trending" items change suddenly. This project includes a **Concept Drift Generator** to demonstrate the adaptive nature of sliding-window sketches.

- **Non-Stationary Data:** By using the `--shift-interval` flag in the producer, the system periodically rotates the Zipfian ranks. 
- **The Shift:** If `--shift-interval 30` is set, every 30 seconds the most frequent item (e.g., `item_1`) is swapped with a random item from the tail.
- **Verification:** This allows you to observe the **Sliding-Window Misra-Gries** in real-time as it "forgets" the old king and identifies the new trending item within seconds on the dashboard.

---

### State Store (Redis)

Redis is the **shared blackboard** between all components:

| Key pattern | Contents | Written by | Read by |
|---|---|---|---|
| `worker:{id}:metrics` | P99 lat, mem %, CMS error, HLL cardinality, HH list | Worker | API, Router, Controller |
| `worker:{id}:latency_history` | last 50 P99 samples | Worker | API (dashboard charts) |
| `worker:{id}:cmd` | `{action, template}` commands | Controller | Worker |
| `controller:log` | Decision log (timestamp, message) | Controller | Dashboard |
| `router:query_log` | Routing decisions | Router | Dashboard |

All keys have a 60-second TTL so dead workers are evicted automatically.

---

### Analytics API (FastAPI)

`api/app.py` serves all query and metrics endpoints. The adaptive controller runs as an `asyncio` background task started at server boot. Static dashboard files are served from `dashboard/`.

The API is documented interactively at **[http://localhost:8000/docs](http://localhost:8000/docs)**.

---

### Web Dashboard

`dashboard/index.html` polls `/metrics/cluster` every 2 seconds and renders a full real-time analytics interface. See the [Dashboard Guide](#dashboard-guide) below for a detailed walkthrough of every section.

---

### C++ Worker

The `cpp/` directory contains a high-performance **C++20 drop-in replacement** for the Python analytics worker. It connects to Kafka via `librdkafka`, runs the same CMS/HLL/MG sketches in C++, and writes to the same Redis keys — so the Python API, Router, and Controller work unchanged.

**Key performance advantages:**
- **xxHash (XXH64)** for hashing — ~10× faster than SHA-256
- **`std::countl_zero()`** for HLL — single CPU instruction vs. Python while-loop
- **`std::shared_mutex`** — concurrent reads without Python's GIL

---

## Quick Start

### Prerequisites
- [Docker Desktop](https://www.docker.com/products/docker-desktop/) (running)
- Python 3.11+

### 1 — Clone and install dependencies

```bash
git clone <repo-url>
cd <project-dir>
pip install -r requirements.txt
```

> **Note:** `requirements.txt` uses `kafka-python-ng` (Python 3.12/3.13 compatible fork of `kafka-python`).

### 2 — Start infrastructure (Kafka + Redis)

```bash
docker compose up -d zookeeper kafka redis
```

Wait about 30 seconds for Kafka to become healthy. Check with:

```bash
docker ps   # btp_kafka should show "(healthy)"
```

### 3 — Start the API server (Terminal 1)

```bash
python3 -m uvicorn api.app:app --host 0.0.0.0 --port 8000 --reload
```

Open **[http://localhost:8000](http://localhost:8000)** — the dashboard loads immediately (empty state).

### 4 — Start a worker (Terminal 2)

```bash
python3 stream/worker.py --worker-id worker-0 --template medium
```

### 5 — Start the producer (Terminal 3)

```bash
python3 stream/producer.py --rate 2000 --zipf 1.2 --duration 120
```

### 6 — Watch the dashboard come alive!

Go to **[http://localhost:8000](http://localhost:8000)** — you should see:
- Live Sketch Estimates updating every 2 seconds
- P99 latency chart populating
- Heavy hitters table filling up
- Controller log showing decisions

### 7 — (Optional) Start more workers

You can run **as many workers as you want** — just give each a unique `--worker-id`:

```bash
# Terminal 4
python3 stream/worker.py --worker-id worker-1 --template high

# Terminal 5
python3 stream/worker.py --worker-id worker-2 --template low

# Terminal 6
python3 stream/worker.py --worker-id worker-3 --template medium
```

Each worker gets its own Kafka consumer group, so all of them receive the full stream independently. The dashboard, router, and controller will **auto-detect** every new worker.

With multiple workers at different precision templates, the **Router Scores** panel shows workers competing — the router picks the best one for each query type based on accuracy, latency, and memory.

---

## Running Components

### Stream Producer

```bash
python3 stream/producer.py [OPTIONS]

Options:
  --rate           EVENT/s to generate (default 5000, 0 = unlimited)
  --zipf           Zipf skewness s parameter (default 1.2)
  --items          Vocabulary size / number of unique items (default 10000)
  --duration       Run for N seconds then stop (default 0 = infinite)
  --shift-interval Rotate rank distribution every N seconds to simulate Concept Drift
```

Examples:
```bash
# Gentle 1000 event/s stream for 60 s
python3 stream/producer.py --rate 1000 --duration 60

# Heavy skewed traffic, run forever
python3 stream/producer.py --rate 10000 --zipf 1.5

# Uniform distribution (no skew)
python3 stream/producer.py --zipf 0.0

# Concept Drift Simulation (Heavy hitters swap every 30 seconds)
python3 stream/producer.py --rate 2000 --shift-interval 30
```

### Python Analytics Worker

```bash
python3 stream/worker.py [OPTIONS]

Options:
  --worker-id   Unique worker identifier (default: worker-0)
  --template    Sketch precision: low | medium | high (default: medium)
  --servers     Kafka bootstrap servers (default: from .env)
  --topic       Kafka topic to consume (default: from .env)
```

You can run **any number of workers** simultaneously — each with a unique ID and optionally a different precision template:
```bash
python3 stream/worker.py --worker-id worker-0 --template medium &
python3 stream/worker.py --worker-id worker-1 --template high &
python3 stream/worker.py --worker-id worker-2 --template low &
python3 stream/worker.py --worker-id worker-3 --template medium &
python3 stream/worker.py --worker-id worker-4 --template high &
```

All workers auto-register in Redis, and the router/controller will manage them automatically. There is **no upper limit** on the number of workers.

### API Server (development)

```bash
python3 -m uvicorn api.app:app --host 0.0.0.0 --port 8000 --reload
```

### Running the C++ Worker (optional)

Requires: `cmake ≥ 3.21`, `g++/clang++ C++20`, `ninja`, `librdkafka-dev`.

```bash
# macOS
brew install cmake ninja librdkafka

# Ubuntu/Debian
apt install cmake ninja-build librdkafka-dev

# Build
cd cpp
cmake -B build -DCMAKE_BUILD_TYPE=Release
cmake --build build -- -j$(nproc)

# Run (infrastructure must be up)
./build/btp_worker --worker-id worker-cpp --template medium

# Or via Docker (no local build tools needed)
docker compose --profile cpp up -d
```

---

## Dashboard Guide

The dashboard at **[http://localhost:8000](http://localhost:8000)** is split into several sections. Here is exactly what each part shows and why it matters:

### 1. Live Sketch Estimates (Top Row)

Three large cards that prominently display the **live answers** from each probabilistic sketch:

| Card | Algorithm | What It Shows |
|---|---|---|
| **Unique Items** | HyperLogLog | Estimated number of distinct items in the stream, with relative error (e.g., ±1.62%) |
| **Top Heavy Hitter** | Misra-Gries | The most frequent item in the current sliding window and its percentage share |
| **Top Item Frequency** | Count-Min Sketch | The estimated frequency count for the top item, with the CMS error bound |

These cards update every 2 seconds and have hover animations for visual feedback.

### 2. KPI Strip (Metrics Bar)

Six compact metric boxes:

| KPI | Source | Meaning |
|---|---|---|
| HLL Cardinality | HyperLogLog | Total unique items seen |
| CMS Total Events | Count-Min Sketch | Total events processed by the worker |
| P99 Latency | Worker metrics | 99th percentile sketch update latency in ms |
| CPU / Mem Usage | psutil | Average CPU% and Memory% across all workers |
| Throughput | Calculated | Real-time events per second (calculated from CMS total delta) |

### 3. Charts Row

- **P99 Latency History** — A multi-line chart showing the latency trend per worker over time. Each worker is a different color. The LIVE badge pulses green.
- **CPU & Memory per Worker** — A grouped bar chart showing both CPU and Memory usage per worker side by side.

### 4. Heavy Hitters Table

A ranked table of the top-12 most frequent items detected by Misra-Gries. Each row shows:
- Rank, Item name, Estimated count, Share bar (visual percentage)

### 5. Router Node Scores

Tabbed panel (Frequency / Cardinality / Heavy Hitters) showing how the Load-Aware Router ranks each worker. The best worker gets a ★. Shows the template badge (LOW/MEDIUM/HIGH) and the computed score from Equation 3.

### 6. Controller Decisions Log

A rolling log of adaptive controller actions, color-coded:
- 🟢 **Green** = upgrade (increased precision)
- 🔴 **Red** = downgrade (reduced precision for lower latency)
- 🟡 **Amber** = memory pressure (shrunk MG window)
- 🟣 **Purple** = manual override

### 7. Query Explorer

Interactive panel to **test queries live**:
- **Frequency tab:** Enter an item key (e.g., `item_1`) and press Run to see the CMS frequency estimate, error bounds, and which worker handled the query.
- **Cardinality tab:** Click Run to get the HLL unique count estimate.
- **Heavy Hitters tab:** Click Run to get the top-N items from Misra-Gries.

### 8. Manual Override

Force a specific worker to use a specific template (Low/Medium/High). Useful for demonstrating the adaptive controller's behavior — override to "Low", then watch the controller detect high error and upgrade it back.

### 9. Algorithm Reference Cards (Bottom)

Four cards summarizing the mathematical guarantees of each algorithm, including the error formulas and parameter relationships.

---

## Testing Guide

### Step 1: Start the System

```bash
# Terminal 1: Infrastructure
docker compose up -d zookeeper kafka redis

# Terminal 2: API Server
python3 -m uvicorn api.app:app --host 0.0.0.0 --port 8000 --reload

# Terminal 3: Worker
python3 stream/worker.py --worker-id worker-0 --template medium

# Terminal 4: Producer (stream events for 2 minutes)
python3 stream/producer.py --rate 2000 --zipf 1.2 --duration 120
```

### Step 2: Test Frequency Query (Count-Min Sketch)

```bash
curl "http://localhost:8000/query/frequency?key=item_1"
```

**Expected Response:**
```json
{
  "key": "item_1",
  "routed_to": "worker-0",
  "router_score": 0.1727,
  "cms_error_bound": 323.31,
  "cms_total_count": 243590,
  "note": "Frequency estimate = true_count ± cms_error_bound (ε=0.00133, δ=0.00674)",
  "worker_metrics": {
    "template": "medium",
    "p95_latency_ms": null,
    "memory_percent": 81.7
  }
}
```

**What to verify:** `item_1` should have the highest estimated frequency because Zipf(s=1.2) makes rank-1 items appear most often.

### Step 3: Test Cardinality Query (HyperLogLog)

```bash
curl "http://localhost:8000/query/cardinality"
```

**Expected Response:**
```json
{
  "estimated_cardinality": 9847,
  "relative_error": 0.0163,
  "hll_precision": 12,
  "routed_to": "worker-0",
  "router_score": 0.1972
}
```

**What to verify:** With `--items 10000`, the HLL estimate should be close to 10,000 (±1.6% for p=12).

### Step 4: Test Heavy Hitters Query (Misra-Gries)

```bash
curl "http://localhost:8000/query/heavy-hitters?n=5"
```

**Expected Response:**
```json
{
  "heavy_hitters": [["item_1", 890], ["item_2", 412], ["item_3", 280], ["item_4", 198], ["item_5", 148]],
  "window_total": 10000,
  "error_bound": 50.0,
  "mg_k": 200,
  "routed_to": "worker-0",
  "router_score": 0.1283
}
```

**What to verify:** Items should be sorted by frequency. `item_1` should always be first due to the Zipf distribution.

### Step 5: Test the Router

```bash
# See how the router scores each worker
curl "http://localhost:8000/metrics/router?q=frequency"
```

### Step 6: Test Manual Override

```bash
# Force worker-0 to use the "low" precision template
curl -X POST "http://localhost:8000/control/set-params" \
  -H "Content-Type: application/json" \
  -d '{"worker_id": "worker-0", "template": "low"}'
```

Then watch the **Controller Decisions** log in the dashboard — the adaptive controller should detect the low accuracy and upgrade the template back within 15-30 seconds.

### Step 7: Run the Comparative Simulation

This standalone benchmark requires **no Kafka or Redis**:

```bash
python3 scripts/run_simulation.py --events 100000 --nodes 3
```

**Expected Output:**
```
Config                                          P99 (ms)   Mem (MB)    HLL err
--------------------------------------------------------------------------------
1. Baseline (static + round-robin)                0.0049     0.1011     0.0163
2. Adaptive params + round-robin                  0.0062     0.1011     0.0163
3. Static params + smart router                   0.0909     0.1011     0.0163
4. Fully adaptive (adaptive + smart)              0.1634     0.1011     0.0163
```

---

## Running Tests

### Python Unit Tests (no Kafka/Redis needed)

```bash
pytest tests/ -v
```

This runs **28 tests** covering:
- **8 Router tests** — scoring formula, node ranking, edge cases
- **6 CMS tests** — error bounds, no-undercount guarantee, memory
- **7 HLL tests** — cardinality accuracy, duplicate handling, precision levels
- **7 MG tests** — heavy hitter detection, window eviction, sorting

### C++ Unit Tests (after building)

```bash
./cpp/build/test_sketches
```

---

## Configuration Reference

Edit **`.env`** to change thresholds without touching code:

| Variable | Default | Effect |
|---|---|---|
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092` | Kafka broker address |
| `KAFKA_TOPIC` | `stream_events` | Topic to produce/consume |
| `REDIS_HOST` | `localhost` | Redis host |
| `REDIS_PORT` | `6379` | Redis port |
| `API_PORT` | `8000` | FastAPI listen port |
| `LATENCY_HIGH_MS` | `200` | P99 threshold → controller downgrades precision |
| `MEMORY_HIGH_PERCENT` | `80` | RAM % threshold → controller shrinks sketches |
| `ERROR_HIGH` | `0.05` | CMS error ratio → controller upgrades precision |
| `CONTROLLER_INTERVAL_SEC` | `5` | How often the controller loop runs |
| `ROUTER_W1` | `0.5` | Weight for accuracy in routing score |
| `ROUTER_W2` | `0.3` | Weight for latency penalty |
| `ROUTER_W3` | `0.2` | Weight for memory penalty |

---

## API Reference

All endpoints return JSON. Interactive docs: **[http://localhost:8000/docs](http://localhost:8000/docs)**

### Query Endpoints

| Method | Endpoint | Description |
|---|---|---|
| `GET` | `/query/frequency?key=X` | CMS frequency estimate for key X |
| `GET` | `/query/cardinality` | HLL distinct count estimate |
| `GET` | `/query/heavy-hitters?n=10` | Top-N Misra–Gries heavy hitters |

All query endpoints route through the **load-aware router** and include routing metadata in the response.

### Metrics Endpoints

| Method | Endpoint | Description |
|---|---|---|
| `GET` | `/metrics/cluster` | Live health metrics for all workers |
| `GET` | `/metrics/sketches` | Current sketch parameters per worker |
| `GET` | `/metrics/router?q=frequency` | Router utility scores per node |
| `GET` | `/metrics/controller-log` | Recent adaptive controller decisions |
| `GET` | `/metrics/query-log` | Recent query routing decisions |

### Control Endpoints

| Method | Endpoint | Body | Description |
|---|---|---|---|
| `POST` | `/control/set-params` | `{"worker_id": "w0", "template": "high"}` | Force a precision template on a worker |
| `GET`  | `/health` | — | Liveness probe |

---

## Sketch Parameter Templates

The controller and manual override use three precision levels:

| Template | CMS width × depth | HLL precision | MG k | Approx memory |
|---|---|---|---|---|
| **Low** | 512 × 3 | p=10 (1,024 regs) | 50 | ~12 KB |
| **Medium** | 2,048 × 5 | p=12 (4,096 regs) | 200 | ~90 KB |
| **High** | 8,192 × 7 | p=14 (16,384 regs) | 500 | ~500 KB |

The controller switches templates based on thresholds in `.env`. Workers apply new templates without restarting — they allocate a new sketch and migrate atomically.

---

## Running the Simulation

The simulation (`scripts/run_simulation.py`) is a **standalone benchmark** that runs entirely **in-process** — no Kafka, Redis, or Docker needed. It reproduces the four experimental configurations from Section 7.2 of the paper:

```bash
python3 scripts/run_simulation.py [OPTIONS]

Options:
  --events  Total events to simulate (default 100,000)
  --nodes   Number of simulated worker nodes (default 3)
  --zipf    Zipf skewness parameter (default 1.2)
  --items   Vocabulary size (default 10,000)
```

| Config | Router | Parameters | Description |
|---|---|---|---|
| 1 | Round-robin | Static | **Baseline** |
| 2 | Round-robin | Adaptive | Controller tunes params |
| 3 | Smart router | Static | Load-aware routing only |
| 4 | Smart router | Adaptive | **Fully adaptive** |

Results are printed to stdout and saved to `experiment_results.json`.

---

## Project Structure

```
.
├── api/
│   ├── app.py              ← FastAPI app, all HTTP endpoints, controller task
│   └── __init__.py
├── controller/
│   ├── controller.py       ← Adaptive parameter controller (asyncio loop)
│   └── __init__.py
├── router/
│   ├── router.py           ← Load-aware router (Eq. 3 scoring)
│   └── __init__.py
├── state/
│   ├── store.py            ← Redis state store (read/write worker metrics)
│   └── __init__.py
├── stream/
│   ├── producer.py         ← Kafka producer (Zipfian synthetic events)
│   ├── worker.py           ← Python analytics worker (CMS, HLL, MG, Kafka consumer)
│   └── __init__.py
├── dashboard/
│   ├── index.html          ← Single-page real-time dashboard
│   └── static/
│       ├── style.css       ← Premium dark-mode UI with animations
│       └── dashboard.js    ← Poll loop, Chart.js charts, router tabs, query explorer
├── cpp/
│   ├── CMakeLists.txt      ← Build system (auto-fetches deps via FetchContent)
│   ├── sketches/
│   │   ├── count_min_sketch.hpp  ← CMS (xxHash, template-parametrised)
│   │   ├── hyperloglog.hpp       ← HLL (std::countl_zero, p-precision)
│   │   └── misra_gries.hpp       ← Sliding-window MG
│   ├── worker/
│   │   ├── worker.cpp      ← Kafka consumer daemon, hot-swap templates
│   │   └── redis_reporter.hpp  ← Serialises sketch state to Redis
│   └── tests/
│       └── test_sketches.cpp   ← Catch2 unit tests for all sketches
├── tests/
│   ├── test_sketches.py    ← Python unit tests for CMS, HLL, MG (20 tests)
│   └── test_router.py      ← Router scoring unit tests (8 tests)
├── scripts/
│   └── run_simulation.py   ← In-process comparative simulation (Section 7.2)
├── Dockerfile.worker        ← Multi-stage Docker build for C++ worker
├── docker-compose.yml       ← Zookeeper + Kafka + Redis + C++ worker (profiles)
├── requirements.txt         ← Python deps (kafka-python-ng, fastapi, redis, …)
├── .env                     ← Runtime thresholds and connection config
└── README.md
```

---

## How the Math Works

### Controller Cost Function (Section 4)

The controller minimises:

```
C(θ, r) = α · LatencyViolation(θ, r)
         + β · Memory(θ)
         + γ · AccuracyPenalty(θ, r)
```

In practice this is implemented as **rule-based threshold checks** (heuristic optimization) to keep latency O(1) at controller invocation time.

### Router Scoring (Equation 3)

```
score_i(q) = w1 · (capability_i / α_q)
           − w2 · (p95_latency_i / L_q)
           − w3 · memory_usage_i
```

Where:
- `capability_i` = normalised sketch precision (0.33 / 0.66 / 1.0 for Low/Med/High)
- `α_q` = query accuracy requirement (default 1.0)
- `L_q` = query SLA latency (default 1.0)
- `p95_latency_i` = 95th-percentile query latency of node _i_
- `memory_usage_i` = fraction of RAM used (0–1)

---

## Troubleshooting

### Kafka not starting
```
docker compose down -v
docker compose up -d zookeeper kafka redis
# Wait 30 seconds
docker ps  # Check for "(healthy)"
```

### Workers show "0 events processed"
Make sure the **producer is running**:
```bash
python3 stream/producer.py --rate 1000
```
And that Kafka is healthy. Check producer output for "Connected to Kafka" message.

### Dashboard shows "Disconnected"
- Ensure the API server is running (`python3 -m uvicorn api.app:app ...`)
- Check the browser console (F12) for errors
- Try manually visiting `http://localhost:8000/health` — should return `{"status": "ok"}`

### Controller keeps downgrading templates
The default memory threshold is 80%. If your system is using >80% RAM, the controller will keep triggering. Either:
- Close other applications to free memory
- Increase the threshold: `MEMORY_HIGH_PERCENT=95` in `.env`

### Port 8000 already in use
```bash
lsof -i :8000  # Find the process
kill -9 <PID>  # Then restart the API server
```

---

## References

1. Cormode & Muthukrishnan (2005). *An improved data stream summary: the Count-Min Sketch and its applications.* Journal of Algorithms.
2. Flajolet, Fusy, Gandouet & Meunier (2007). *HyperLogLog: the analysis of a near-optimal cardinality estimation algorithm.* AOFA.
3. Misra & Gries (1982). *Finding repeated items.* Science of Computer Programming.
