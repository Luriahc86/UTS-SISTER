import asyncio
import logging
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from src.models import Event

logger = logging.getLogger(__name__)

DB_PATH = Path("/app/data/dedup.db")


class DedupStore:

    def __init__(self, db_path: Path = DB_PATH) -> None:
        self.db_path = db_path

    async def init(self) -> None:
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        await asyncio.to_thread(self._create_table)
        logger.info("DedupStore initialised at %s", self.db_path)

    def _create_table(self) -> None:
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute("PRAGMA synchronous=NORMAL")
            conn.execute("PRAGMA cache_size=-32000")
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS processed_events (
                    topic        TEXT NOT NULL,
                    event_id     TEXT NOT NULL,
                    processed_at TEXT NOT NULL,
                    PRIMARY KEY (topic, event_id)
                )
                """
            )
            conn.execute("CREATE INDEX IF NOT EXISTS idx_topic ON processed_events (topic)")
            conn.commit()

    async def is_duplicate(self, topic: str, event_id: str) -> bool:
        return await asyncio.to_thread(self._check_exists, topic, event_id)

    async def mark_processed(self, topic: str, event_id: str) -> None:
        now = datetime.now(timezone.utc).isoformat()
        await asyncio.to_thread(self._insert, topic, event_id, now)


    async def bulk_check_and_insert(
        self, events: list["Event"]
    ) -> tuple[list["Event"], int]:

        return await asyncio.to_thread(self._bulk_process, events)

    def _bulk_process(
        self, events: list["Event"]
    ) -> tuple[list["Event"], int]:
        now = datetime.now(timezone.utc).isoformat()
        new_events: list["Event"] = []
        dup_count = 0

        with sqlite3.connect(self.db_path) as conn:
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute("PRAGMA synchronous=NORMAL")

            for event in events:
                row = conn.execute(
                    "SELECT 1 FROM processed_events WHERE topic=? AND event_id=?",
                    (event.topic, event.event_id),
                ).fetchone()

                if row is not None:
                    dup_count += 1
                else:
                    try:
                        conn.execute(
                            "INSERT INTO processed_events (topic, event_id, processed_at) VALUES (?,?,?)",
                            (event.topic, event.event_id, now),
                        )
                        new_events.append(event)
                    except sqlite3.IntegrityError:
                        dup_count += 1

            conn.commit()

        return new_events, dup_count

    async def get_events(self, topic: str | None = None) -> list[dict]:
        return await asyncio.to_thread(self._fetch_events, topic)

    async def get_topics(self) -> list[str]:
        return await asyncio.to_thread(self._fetch_topics)

    def _check_exists(self, topic: str, event_id: str) -> bool:
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("PRAGMA journal_mode=WAL")
            row = conn.execute(
                "SELECT 1 FROM processed_events WHERE topic = ? AND event_id = ?",
                (topic, event_id),
            ).fetchone()
        return row is not None

    def _insert(self, topic: str, event_id: str, processed_at: str) -> None:
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute("PRAGMA synchronous=NORMAL")
            try:
                conn.execute(
                    "INSERT INTO processed_events (topic, event_id, processed_at) VALUES (?, ?, ?)",
                    (topic, event_id, processed_at),
                )
                conn.commit()
            except sqlite3.IntegrityError:

                logger.debug("IntegrityError saat insert — data sudah ada: %s/%s", topic, event_id)

    def _fetch_events(self, topic: str | None) -> list[dict]:
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            if topic:
                rows = conn.execute(
                    "SELECT topic, event_id, processed_at FROM processed_events WHERE topic = ? ORDER BY processed_at",
                    (topic,),
                ).fetchall()
            else:
                rows = conn.execute(
                    "SELECT topic, event_id, processed_at FROM processed_events ORDER BY topic, processed_at"
                ).fetchall()
        return [dict(row) for row in rows]

    def _fetch_topics(self) -> list[str]:
        with sqlite3.connect(self.db_path) as conn:
            rows = conn.execute(
                "SELECT DISTINCT topic FROM processed_events ORDER BY topic"
            ).fetchall()
        return [row[0] for row in rows]
