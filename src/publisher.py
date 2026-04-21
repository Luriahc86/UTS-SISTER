"""
Skrip publisher mandiri (standalone) untuk simulasi sistem log.

Penggunaan (di dalam container):
    python -m src.publisher [--host HOST] [--port PORT] [--count N] [--topic TOPIC]

Perilaku (Behaviour):
    • Mengirim `count` event unik ke agregator
    • Kemudian mengirim ulang 10 event pertama (simulasi pengiriman at-least-once / setidaknya sekali)
    • Melaporkan total yang diterima/ditolak
"""

import argparse
import asyncio
import logging
import uuid
from datetime import datetime, timezone

import httpx

logging.basicConfig(
    level="INFO",
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("publisher")


def _make_event(topic: str, source: str, event_id: str | None = None) -> dict:
    return {
        "topic": topic,
        "event_id": event_id or str(uuid.uuid4()),
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "source": source,
        "payload": {"generated_by": "publisher.py"},
    }


async def run(host: str, port: int, count: int, topic: str, batch_size: int) -> None:
    base_url = f"http://{host}:{port}"
    logger.info("Publisher connecting to %s", base_url)

    events = [_make_event(topic=topic, source="publisher") for _ in range(count)]

    # Menduplikasi 10 event pertama untuk menyimulasikan kejadian pengiriman 'at-least-once'
    duplicates = [_make_event(topic=topic, source="publisher", event_id=e["event_id"]) for e in events[:10]]
    all_events = events + duplicates

    total_accepted = 0
    total_rejected = 0

    async with httpx.AsyncClient(base_url=base_url, timeout=30.0) as client:
        for i in range(0, len(all_events), batch_size):
            batch = all_events[i : i + batch_size]
            try:
                resp = await client.post("/publish", json=batch)
                resp.raise_for_status()
                data = resp.json()
                total_accepted += data.get("accepted", 0)
                total_rejected += data.get("rejected", 0)
                logger.info(
                    "Batch %d/%d sent — accepted=%d rejected=%d",
                    i // batch_size + 1,
                    -(-len(all_events) // batch_size),
                    data.get("accepted", 0),
                    data.get("rejected", 0),
                )
            except httpx.HTTPError as exc:
                logger.error("HTTP error on batch %d: %s", i // batch_size + 1, exc)

    logger.info(
        "Done. total_sent=%d total_accepted=%d total_rejected=%d duplicates_sent=%d",
        len(all_events),
        total_accepted,
        total_rejected,
        len(duplicates),
    )

    # Mengambil laporan dan menampilkan statistik
    async with httpx.AsyncClient(base_url=base_url, timeout=10.0) as client:
        resp = await client.get("/stats")
        if resp.status_code == 200:
            logger.info("Aggregator stats: %s", resp.json())


def main() -> None:
    parser = argparse.ArgumentParser(description="iNEED UTS Publisher")
    parser.add_argument("--host", default="aggregator", help="Host agregator")
    parser.add_argument("--port", type=int, default=8080, help="Port agregator")
    parser.add_argument("--count", type=int, default=100, help="Jumlah event unik yang akan dikirim")
    parser.add_argument("--topic", default="test-topic", help="Topik event")
    parser.add_argument("--batch-size", type=int, default=50, help="Event per permintaan POST")
    args = parser.parse_args()

    asyncio.run(run(args.host, args.port, args.count, args.topic, args.batch_size))


if __name__ == "__main__":
    main()
