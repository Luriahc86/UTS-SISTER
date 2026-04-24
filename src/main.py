import asyncio
import logging
import os
from contextlib import asynccontextmanager
from pathlib import Path
from typing import AsyncGenerator

from fastapi import FastAPI, Query, Request
from fastapi.responses import JSONResponse
from pydantic import ValidationError

from src.dedup import DedupStore
from src.models import Event, PublishResponse, StatsResponse
from src.queue import EventQueue
from src.stats import StatsCounter

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
logger = logging.getLogger(__name__)

DB_PATH = Path(os.getenv("DEDUP_DB_PATH", "/app/data/dedup.db"))

dedup_store = DedupStore(db_path=DB_PATH)
event_queue = EventQueue()
stats = StatsCounter()
_stop_event = asyncio.Event()
_consumer_task: asyncio.Task | None = None

@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None, None]:
    global _consumer_task, _stop_event

    _stop_event = asyncio.Event()
    await dedup_store.init()
    _consumer_task = asyncio.create_task(
        event_queue.consume(dedup_store, stats, _stop_event),
        name="event-consumer",
    )
    logger.info("Aggregator service started")

    yield

    logger.info("Shutting down — draining queue (size=%d)", event_queue.size)
    _stop_event.set()
    if _consumer_task:
        await asyncio.wait_for(_consumer_task, timeout=60.0)
    logger.info("Aggregator service stopped")


app = FastAPI(
    title="UTS Pub-Sub Log Aggregator",
    description="Agregator event idempoten dengan penyimpanan deduplikasi di SQLite.",
    version="1.0.0",
    lifespan=lifespan,
)

@app.post("/publish", response_model=PublishResponse, status_code=202)
async def publish(request: Request) -> PublishResponse:
    """
    Menerima objek event tunggal ATAU array berisi beberapa objek event.
    Mengembalikan jumlah event yang diterima/ditolak.
    """
    body = await request.json()
    raw_events: list = body if isinstance(body, list) else [body]

    accepted = 0
    rejected = 0
    valid_events: list[Event] = []

    for raw in raw_events:
        try:
            event = Event.model_validate(raw)
            valid_events.append(event)
            accepted += 1
        except (ValidationError, Exception) as exc:
            logger.warning("Rejected event: %s — %s", raw, exc)
            rejected += 1

    if valid_events:
        stats.record_received(len(valid_events))
        await event_queue.enqueue_many(valid_events)

    return PublishResponse(accepted=accepted, rejected=rejected)

@app.get("/events")
async def get_events(
    topic: str | None = Query(default=None, description="Menyaring (filter) event berdasarkan topik"),
) -> JSONResponse:
    """
    Mengembalikan event yang telah diproses, dengan opsi penyaringan berdasarkan topik.
    """
    events = await dedup_store.get_events(topic=topic)
    return JSONResponse(content={"events": events, "count": len(events)})

@app.get("/stats", response_model=StatsResponse)
async def get_stats() -> StatsResponse:
    """Mengembalikan statistik hasil keseluruhan proses aggregasi."""
    topics = await dedup_store.get_topics()
    return StatsResponse(
        received=stats.received,
        unique_processed=stats.unique_processed,
        duplicate_dropped=stats.duplicate_dropped,
        topics=topics,
        uptime_seconds=round(stats.uptime_seconds, 3),
    )

@app.get("/health")
async def health() -> dict:
    return {"status": "ok", "queue_size": event_queue.size}

if __name__ == "__main__":
    import uvicorn

    uvicorn.run(
        "src.main:app",
        host="0.0.0.0",
        port=int(os.getenv("PORT", "8080")),
        log_level=os.getenv("LOG_LEVEL", "info").lower(),
    )
