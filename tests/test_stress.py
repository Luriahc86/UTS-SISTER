"""
Uji tekanan tanggungan/beban (Stress test) — mem-push pengiriman sejumlah 5000 event (1000 darinya merupakan jenis yang terduplikat) setidaknya hanya dalam kurun limit < 10 detik.
Tes 9.
"""

import asyncio
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path

import pytest
import pytest_asyncio
from httpx import ASGITransport, AsyncClient


def _evt(topic: str, event_id: str | None = None) -> dict:
    return {
        "topic": topic,
        "event_id": event_id or str(uuid.uuid4()),
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "source": "stress-test",
    }


@pytest_asyncio.fixture
async def client(tmp_path: Path, monkeypatch):
    db_path = tmp_path / "stress.db"

    import src.main as main_mod
    from src.dedup import DedupStore
    from src.stats import StatsCounter
    from src.queue import EventQueue

    monkeypatch.setattr(main_mod, "dedup_store", DedupStore(db_path=db_path))
    monkeypatch.setattr(main_mod, "stats", StatsCounter())
    monkeypatch.setattr(main_mod, "event_queue", EventQueue())

    async with main_mod.app.router.lifespan_context(main_mod.app):
        async with AsyncClient(
            transport=ASGITransport(app=main_mod.app), base_url="http://test"
        ) as c:
            yield c, main_mod


@pytest.mark.asyncio
async def test_stress(client) -> None:
    """
    Mengeksekusi pendistribusian menembus jumlah maksimum senilai 5000 event dengan akumulasi keseluruhan rincian:
      - 4000 event yang segar/baru (unique events)
      - Sebanyak 1000 duplikat sisanya (perlakuan proses yang mere-send balik di range awal id: 1000 event_ids pertama)
    Yakinkan jangkauan waktu absolut penyelesaiannya bernilai kurang alias < dari 10s (detik), dengan pencocokan expected variable result, yaitu: unique=4000, dropped=1000.
    """
    c, mod = client
    UNIQUE = 4000
    DUPS = 1000
    BATCH_SIZE = 250

    unique_events = [_evt(topic="stress") for _ in range(UNIQUE)]
    dup_events = [
        _evt(topic="stress", event_id=unique_events[i]["event_id"])
        for i in range(DUPS)
    ]
    all_events = unique_events + dup_events  # menghasilkan populasi sejumlah 5000 kejadian event

    start = time.monotonic()

    # Kirim secara massal/batch secara paralel/berbarengan
    batches = [all_events[i:i + BATCH_SIZE] for i in range(0, len(all_events), BATCH_SIZE)]
    responses = await asyncio.gather(*[c.post("/publish", json=b) for b in batches])
    for resp in responses:
        assert resp.status_code == 202

    # Mengosongkan memori di antrean/queue
    deadline = time.monotonic() + 9.0
    while mod.event_queue.size > 0 and time.monotonic() < deadline:
        await asyncio.sleep(0.05)
    await asyncio.sleep(0.2)

    elapsed = time.monotonic() - start
    assert elapsed < 10.0, f"Stress test exceeded 10s: {elapsed:.2f}s"

    s = (await c.get("/stats")).json()
    assert s["received"] == 5000
    assert s["unique_processed"] == UNIQUE
    assert s["duplicate_dropped"] == DUPS
