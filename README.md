# UTS — Pub-Sub Log Aggregator

A local-only, idempotent event aggregator service built with **FastAPI**, **asyncio**, and **SQLite**.

## Architecture

```
Publisher(s)
    │  POST /publish (single or batch)
    ▼
FastAPI  ──► asyncio.Queue ──► Consumer Task
                                    │
                               DedupStore (SQLite)
                                    │
                         /events  /stats  /health
```

| Component | Responsibility |
|-----------|----------------|
| `src/models.py` | Pydantic event schema + response models |
| `src/dedup.py` | SQLite-backed `(topic, event_id)` dedup store |
| `src/stats.py` | In-memory counters (received / unique / dropped / uptime) |
| `src/queue.py` | `asyncio.Queue` wrapper + background consumer coroutine |
| `src/main.py` | FastAPI app, lifespan, endpoint handlers |
| `src/publisher.py` | Standalone publisher for docker-compose demo |

---

## Quick Start

### Option A — Docker Compose (recommended)

```bash
# Build and start both aggregator + publisher
docker compose up --build

# Aggregator is available at http://localhost:8080
# Publisher runs once, sends 200 unique events + 10 duplicates, then exits
```

### Option B — Docker only (aggregator)

```bash
docker build -t uts-aggregator .

docker run -d \
  --name uts-aggregator \
  -p 8080:8080 \
  -v uts_data:/app/data \
  uts-aggregator
```

### Option C — Local (no Docker)

```bash
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt

# SQLite will be written to /app/data/dedup.db
# Override with env var for local dev:
export DEDUP_DB_PATH=./data/dedup.db
mkdir -p data

python -m src.main
```

---

## API Reference

### `POST /publish`

Accept a single event or a JSON array of events.

```bash
# Single event
curl -X POST http://localhost:8080/publish \
  -H "Content-Type: application/json" \
  -d '{
    "topic": "orders",
    "event_id": "550e8400-e29b-41d4-a716-446655440000",
    "timestamp": "2026-04-20T10:00:00Z",
    "source": "order-service",
    "payload": {"order_id": 99}
  }'

# Response
{"accepted": 1, "rejected": 0}
```

```bash
# Batch of events
curl -X POST http://localhost:8080/publish \
  -H "Content-Type: application/json" \
  -d '[{"topic":"a","event_id":"id-1","timestamp":"2026-04-20T10:00:00Z","source":"s"},
       {"topic":"b","event_id":"id-2","timestamp":"2026-04-20T10:01:00Z","source":"s"}]'
```

**Event schema (all fields required except `payload`):**

| Field | Type | Rules |
|-------|------|-------|
| `topic` | string | non-empty |
| `event_id` | string | non-empty, UUID/nanoid recommended |
| `timestamp` | ISO8601 string | parsed to `datetime` |
| `source` | string | non-empty |
| `payload` | object | optional |

Returns `422` only when the entire body is malformed JSON; individual invalid events in a batch are counted as `rejected`.

---

### `GET /events?topic=<name>`

```bash
# All events for a topic
curl http://localhost:8080/events?topic=orders

# All events (all topics)
curl http://localhost:8080/events
```

Response:
```json
{
  "events": [
    {"topic": "orders", "event_id": "...", "processed_at": "2026-04-20T10:00:00+00:00"}
  ],
  "count": 1
}
```

---

### `GET /stats`

```bash
curl http://localhost:8080/stats
```

Response:
```json
{
  "received": 100,
  "unique_processed": 90,
  "duplicate_dropped": 10,
  "topics": ["orders", "payments"],
  "uptime_seconds": 42.5
}
```

---

### `GET /health`

```bash
curl http://localhost:8080/health
# {"status": "ok", "queue_size": 0}
```

---

## Running Tests

```bash
# Install deps
pip install -r requirements.txt

# Run all tests
pytest

# Verbose with output
pytest -v -s

# Run specific test files
pytest tests/test_dedup.py -v
pytest tests/test_api.py -v
pytest tests/test_stress.py -v
```

### Test Coverage

| Test | File | Description |
|------|------|-------------|
| `test_dedup_single` | test_dedup.py | Single event processed once |
| `test_dedup_duplicate` | test_dedup.py | Same event 3x → unique=1, dropped=2 |
| `test_schema_valid` | test_api.py | Valid event passes Pydantic |
| `test_schema_invalid` | test_api.py | Missing `event_id` → rejected |
| `test_stats_consistent` | test_api.py | Stats match actual counts |
| `test_events_by_topic` | test_api.py | `/events?topic=` filters correctly |
| `test_persistence_simulation` | test_dedup.py | Re-init same DB → still deduplicates |
| `test_batch_publish` | test_api.py | 100 events (20 dups) → correct counts |
| `test_stress` | test_stress.py | 5000 events (1000 dups) in < 10s |
| `test_restart_dedup` | test_dedup.py | New DedupStore on same file → dedup persists |

---

## Design Decisions

- **No external brokers** — pure `asyncio.Queue` in-process pipeline
- **SQLite dedup** — survives container restarts via Docker volume at `/app/data`
- **`asyncio.to_thread`** — all blocking `sqlite3` calls offloaded so the event loop is never stalled
- **At-least-once simulation** — publisher re-sends 10 events; consumer silently drops them with `[DUPLICATE]` log
- **Non-root container** — `appuser` created in Dockerfile per assignment spec
- **Batch publish** — accepts both `{}` and `[{}, {}]` in a single endpoint
