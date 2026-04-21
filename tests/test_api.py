"""
Pengujian tingkat API menggunakan TestClient FastAPI (via httpx).
Mencakup pengujian fungsionalitas: test_schema_valid, test_schema_invalid, test_stats_consistent,
        test_events_by_topic, test_batch_publish.
"""

import asyncio
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path

import pytest
import pytest_asyncio
from httpx import ASGITransport, AsyncClient


def _evt(topic: str = "test", event_id: str | None = None, **kwargs) -> dict:
    return {
        "topic": topic,
        "event_id": event_id or str(uuid.uuid4()),
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "source": "test-suite",
        "payload": {"k": "v"},
        **kwargs,
    }


async def _drain(mod, timeout: float = 3.0) -> None:
    """Tunggu sampai consumer selesai memproses seluruh event yang ada di dalam antrean."""
    deadline = time.monotonic() + timeout
    while mod.event_queue.size > 0 and time.monotonic() < deadline:
        await asyncio.sleep(0.05)
    await asyncio.sleep(0.15)


# ---------------------------------------------------------------------------
# Fixture per pengujian: DB yang dibuat baru + singleton terisolasi + siklus hidup penuh (full lifespan)
# ---------------------------------------------------------------------------

@pytest_asyncio.fixture
async def client(tmp_path: Path, monkeypatch):
    db_path = tmp_path / "test.db"

    import src.main as main_mod
    from src.dedup import DedupStore
    from src.stats import StatsCounter
    from src.queue import EventQueue

    new_store = DedupStore(db_path=db_path)
    new_stats = StatsCounter()
    new_queue = EventQueue()

    monkeypatch.setattr(main_mod, "dedup_store", new_store)
    monkeypatch.setattr(main_mod, "stats", new_stats)
    monkeypatch.setattr(main_mod, "event_queue", new_queue)

    async with main_mod.app.router.lifespan_context(main_mod.app):
        async with AsyncClient(
            transport=ASGITransport(app=main_mod.app), base_url="http://test"
        ) as c:
            yield c, main_mod


# ---------------------------------------------------------------------------
# Tes 3 — skema valid lolos dengan pengecekan dari Pydantic
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_schema_valid(client) -> None:
    c, _ = client
    resp = await c.post("/publish", json=_evt())
    assert resp.status_code == 202
    data = resp.json()
    assert data["accepted"] == 1
    assert data["rejected"] == 0


# ---------------------------------------------------------------------------
# Tes 4 — event_id terlewatkan (missing) → ditolak (rejected)
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_schema_invalid(client) -> None:
    c, _ = client
    bad = {
        "topic": "orders",
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "source": "test",
    }
    resp = await c.post("/publish", json=bad)
    assert resp.status_code == 202
    data = resp.json()
    assert data["accepted"] == 0
    assert data["rejected"] == 1


# ---------------------------------------------------------------------------
# Tes 5 — endpoint stats konsisten mengikuti penghitungan asli nyata
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_stats_consistent(client) -> None:
    c, mod = client
    events = [_evt(topic="metrics") for _ in range(5)]
    dup = _evt(topic="metrics", event_id=events[0]["event_id"])

    await c.post("/publish", json=events)
    await c.post("/publish", json=dup)
    await _drain(mod)

    resp = await c.get("/stats")
    assert resp.status_code == 200
    s = resp.json()

    assert s["received"] == 6
    assert s["unique_processed"] == 5
    assert s["duplicate_dropped"] == 1
    assert "metrics" in s["topics"]
    assert s["uptime_seconds"] >= 0


# ---------------------------------------------------------------------------
# Tes 6 — GET /events?topic= hanya mengembalikan rentetan event dalam topik tsb
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_events_by_topic(client) -> None:
    c, mod = client
    await c.post("/publish", json=[_evt(topic="alpha"), _evt(topic="alpha")])
    await c.post("/publish", json=[_evt(topic="beta"), _evt(topic="beta")])
    await _drain(mod)

    resp_alpha = await c.get("/events", params={"topic": "alpha"})
    assert resp_alpha.status_code == 200
    alpha_data = resp_alpha.json()
    assert alpha_data["count"] == 2
    assert all(e["topic"] == "alpha" for e in alpha_data["events"])

    assert (await c.get("/events", params={"topic": "beta"})).json()["count"] == 2
    assert (await c.get("/events")).json()["count"] == 4


# ---------------------------------------------------------------------------
# Tes 8 — metode batch senilai 100 event (20 diantaranya duplikat) → mengharapkan hitungan yang akurat/benar
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_batch_publish(client) -> None:
    c, mod = client
    unique_events = [_evt(topic="bulk") for _ in range(80)]
    dup_events = [
        _evt(topic="bulk", event_id=unique_events[i]["event_id"])
        for i in range(20)
    ]
    batch = unique_events + dup_events  # 100 total

    resp = await c.post("/publish", json=batch)
    assert resp.status_code == 202
    assert resp.json()["accepted"] == 100
    assert resp.json()["rejected"] == 0

    await _drain(mod, timeout=5.0)

    s = (await c.get("/stats")).json()
    assert s["unique_processed"] == 80
    assert s["duplicate_dropped"] == 20
    assert s["received"] == 100
