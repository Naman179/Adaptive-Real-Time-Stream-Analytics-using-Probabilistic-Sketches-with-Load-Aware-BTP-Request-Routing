# Adaptive Real-Time Stream Analytics
## Using Probabilistic Sketches with Load-Aware Request Routing

**BTP Project** — Palak Khemchandani, Naman Aggrawal, Abhinav Dogra  
Department of Computer Science, LNMIIT

---

## Architecture

```
Kafka Topic ──► Analytics Workers ──► Redis State Store
                    │ (CMS + HLL + MG)       │
                    │                        ├──► Adaptive Controller (auto-tunes params)
                    │                        ├──► Load-Aware Router   (Eq. 3 scoring)
                    │                        └──► FastAPI + Dashboard
```

| Component | Description |
|---|---|
| **Count-Min Sketch** | Frequency estimation with ε·N additive error guarantee |
| **HyperLogLog** | Cardinality estimation, ~1.04/√m relative error |
| **Sliding-Window Misra-Gries** | Heavy hitter detection over time windows |
| **Adaptive Controller** | Rule-based loop, dynamically tunes Low/Medium/High templates |
| **Load-Aware Router** | Equation (3) node scoring: accuracy, latency, memory |
| **Web Dashboard** | Real-time charts, heavy hitters table, controller log |

---

## Prerequisites

- **Docker Desktop** (running)
- **Python 3.11+**

---

## Quick Start

### 1 — Start infrastructure
```bash
docker compose up -d
```

### 2 — Install Python dependencies
```bash
pip install -r requirements.txt
```

### 3 — Start a worker (in Terminal 1)
```bash
python stream/worker.py --worker-id worker-0 --template medium
```

### 4 — Start a second worker (in Terminal 2, optional)
```bash
python stream/worker.py --worker-id worker-1 --template high
```

### 5 — Start the API server (in Terminal 3)
```bash
python -m uvicorn api.app:app --reload --host 0.0.0.0 --port 8000
```

### 6 — Start streaming data (in Terminal 4)
```bash
python stream/producer.py --rate 2000 --zipf 1.2
```

### 7 — Open the dashboard
Visit **http://localhost:8000** in your browser.

---

## API Reference

| Endpoint | Description |
|---|---|
| `GET /query/frequency?key=X` | CMS estimate for key X |
| `GET /query/cardinality` | HLL distinct count estimate |
| `GET /query/heavy-hitters?n=10` | Top-10 Misra-Gries heavy hitters |
| `GET /metrics/cluster` | All worker metrics |
| `GET /metrics/sketches` | Current sketch parameters |
| `GET /metrics/router?q=frequency` | Router node scores |
| `GET /metrics/controller-log` | Recent controller decisions |
| `POST /control/set-params` | Manual template override |
| `GET /docs` | Interactive API docs (Swagger) |

---

## Running Tests

```bash
# Unit tests (no Kafka/Redis needed)
pytest tests/test_sketches.py tests/test_router.py -v

# All tests
pytest tests/ -v
```

---

## Configuration

Edit `.env` to tune thresholds:

| Variable | Default | Description |
|---|---|---|
| `LATENCY_HIGH_MS` | 200 | P99 threshold to downgrade precision |
| `MEMORY_HIGH_PERCENT` | 80 | Memory threshold to shrink MG window |
| `ERROR_HIGH` | 0.05 | CMS relative error to upgrade precision |
| `CONTROLLER_INTERVAL_SEC` | 5 | Controller loop period |
| `ROUTER_W1` / `W2` / `W3` | 0.5/0.3/0.2 | Router scoring weights |

---

## Sketch Parameter Templates

| Template | CMS (w×d) | HLL precision | MG k |
|---|---|---|---|
| **Low** | 512 × 3 | p=10 (1024 regs) | 50 |
| **Medium** | 2048 × 5 | p=12 (4096 regs) | 200 |
| **High** | 8192 × 7 | p=14 (16384 regs) | 500 |

---

## Project Structure

```
BTP_PROJECT/
├── sketches/            # CMS, HLL, Misra-Gries implementations
├── stream/              # Kafka producer + analytics worker
├── state/               # Redis state store
├── controller/          # Adaptive parameter controller
├── router/              # Load-aware request router
├── api/                 # FastAPI application
├── dashboard/           # Real-time web dashboard
├── tests/               # Unit tests
├── docker-compose.yml
├── requirements.txt
└── .env
```
