import asyncio
import logging
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from src.dedup import DedupStore
    from src.models import Event
    from src.stats import StatsCounter

logger = logging.getLogger(__name__)

# Buffer cukup besar agar publisher tidak pernah terblokir ketika terdapat beban traffic (burst traffic)
QUEUE_MAX_SIZE = 20_000
# Jumlah event yang ditarik dari antrian per literasi worker konsumer (untuk mikro-batching)
CONSUMER_BATCH_SIZE = 50


class EventQueue:
    """
    Pembungkus tipis asyncio.Queue yang menyediakan fungsi pembantu di sisi produsen/konsumer.
    """

    def __init__(self) -> None:
        self._queue: asyncio.Queue["Event"] = asyncio.Queue(maxsize=QUEUE_MAX_SIZE)

    async def enqueue(self, event: "Event") -> None:
        await self._queue.put(event)

    async def enqueue_many(self, events: list["Event"]) -> None:
        for event in events:
            await self._queue.put(event)

    @property
    def size(self) -> int:
        return self._queue.qsize()

    # ------------------------------------------------------------------
    # Konsumer yang berjalan di latar belakang (Background consumer)
    # ------------------------------------------------------------------

    async def consume(
        self,
        dedup: "DedupStore",
        stats: "StatsCounter",
        stop_event: asyncio.Event,
    ) -> None:
        """
        Coroutine yang berjalan lama untuk mengosongkan antrean dan memproses daftar event.
        Mengandalkan pemrosesan mikro-batch: menarik event hingga ukuran CONSUMER_BATCH_SIZE per siklus
        bertujuan meminimalisir overhead asyncio serta tidak melakukan bloking alur asyncio.
        """
        logger.info("Consumer task started")

        while not (stop_event.is_set() and self._queue.empty()):
            # Mengambil data mikro-batch tanpa pemblokiran yang berlangsung lebih dari 0.1 detik
            batch: list["Event"] = []
            try:
                # Memblok hingga waktu tertentu saja untuk menunggu item yang pertama datang
                first = await asyncio.wait_for(self._queue.get(), timeout=0.1)
                batch.append(first)
            except asyncio.TimeoutError:
                continue

            # Menguras tambahan event lain yang barangkali juga telah masuk antrean (tanpa bloking)
            while len(batch) < CONSUMER_BATCH_SIZE:
                try:
                    batch.append(self._queue.get_nowait())
                except asyncio.QueueEmpty:
                    break

            # Melakukan pemrosesan batch data
            try:
                await _process_batch(batch, dedup, stats)
            except Exception:
                logger.exception("Unexpected error processing batch of %d events", len(batch))
            finally:
                for _ in batch:
                    self._queue.task_done()

        logger.info("Consumer task stopped")


# ------------------------------------------------------------------
# Logika pemrosesan internal
# ------------------------------------------------------------------

async def _process_batch(
    events: list["Event"],
    dedup: "DedupStore",
    stats: "StatsCounter",
) -> None:
    """
    Memproses kumpulan (mikro-batch) events.
    Memanfaatkan bulk_check_and_insert supaya dalam 1 batch akses ke database cukup dilakukan 1 kali (satu trip) saja.
    """
    new_events, dup_count = await dedup.bulk_check_and_insert(events)
    stats.record_unique(len(new_events))
    stats.record_duplicate(dup_count)

    for event in events:
        if event in new_events:
            logger.debug(
                "[PROCESSED] topic=%s event_id=%s source=%s",
                event.topic, event.event_id, event.source,
            )
        else:
            logger.warning(
                "[DUPLICATE] topic=%s event_id=%s dropped=true",
                event.topic, event.event_id,
            )


async def _process_event(
    event: "Event",
    dedup: "DedupStore",
    stats: "StatsCounter",
) -> None:
    """Pemrosesan satu buah event tunggal (dipertahankan guna kepentingan backward-compatibility / penggunaan langsung)."""
    if await dedup.is_duplicate(event.topic, event.event_id):
        stats.record_duplicate()
        logger.warning(
            "[DUPLICATE] topic=%s event_id=%s dropped=true",
            event.topic, event.event_id,
        )
        return
    await dedup.mark_processed(event.topic, event.event_id)
    stats.record_unique()
    logger.debug(
        "[PROCESSED] topic=%s event_id=%s source=%s",
        event.topic, event.event_id, event.source,
    )
