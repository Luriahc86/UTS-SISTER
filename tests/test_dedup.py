"""
Pengujian untuk komponen DedupStore (berperan sebagai lapisan pembersihan data duplikat dengan SQLite).
Mencakup list pengujian: test_dedup_single, test_dedup_duplicate, test_persistence_simulation,
        test_restart_dedup.
"""

import asyncio
import tempfile
from pathlib import Path

import pytest
import pytest_asyncio

from src.dedup import DedupStore


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest_asyncio.fixture
async def store(tmp_path: Path) -> DedupStore:
    """Instansi DedupStore baru yang diselipkan/ditunjang pada file temp SQLite."""
    s = DedupStore(db_path=tmp_path / "test_dedup.db")
    await s.init()
    return s


# ---------------------------------------------------------------------------
# Tes 1 — event tunggal yang berjalan/diproses akan terproses setidaknya sekali saja (exactly once)
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_dedup_single(store: DedupStore) -> None:
    """Event anyar yang mengandung struktur list kombinasi (topic, event_id) segar ditiadakan flag/status identifikasi duplikat."""
    topic, event_id = "orders", "evt-001"

    assert not await store.is_duplicate(topic, event_id)
    await store.mark_processed(topic, event_id)
    assert await store.is_duplicate(topic, event_id)


# ---------------------------------------------------------------------------
# Tes 2 — pendeteksian duplikat tepat sesaat pasca insersi pertamanya
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_dedup_duplicate(store: DedupStore) -> None:
    """
    Jika event yang sama dikirimkan 3 kali → maka otomatis cuma akan berjalan 1 kali event perlakuan aksi penambahan nilai (insert);
    sesudahnya untuk request is_duplicate berulang-ulang nilainya selalu memunculkan hasil True.
    """
    topic, event_id = "payments", "evt-dup"

    # Penjalan panggilan waktu pertama: tidak dihitung/dikenali ke dalam entri data duplikat
    assert not await store.is_duplicate(topic, event_id)
    await store.mark_processed(topic, event_id)

    # Waktu kesempatan yang kedua/ketiga: terdeteksi selaku data berganda (duplikat)
    assert await store.is_duplicate(topic, event_id)
    assert await store.is_duplicate(topic, event_id)

    # Sekadar menampilkan rekam hasil eksklusif pada memori penyimpanan (satu entitas saja di catatan DB)
    events = await store.get_events(topic=topic)
    assert len(events) == 1
    assert events[0]["event_id"] == event_id


# ---------------------------------------------------------------------------
# Tes 7 — pengamatan simulasi persistensi dan keawetan dari DB (menghidupkan mode inisial secara persis pada referensi file DB terdahulu/awal)
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_persistence_simulation(tmp_path: Path) -> None:
    """
    Pertama kita inisialisasi insert event lewat store_a, kemudian memicu bukanya store_b yang mengait ke file yang bersangkutan tadi.
    store_b harus segera paham sedari dini kalau datanya sama yang berartikan merupakan identifikasi duplikat / tak pantas di-insert kembali ke list.
    """
    db_path = tmp_path / "persist.db"

    store_a = DedupStore(db_path=db_path)
    await store_a.init()
    await store_a.mark_processed("invoices", "evt-persist")

    # Membuka kembali DB yang sama
    store_b = DedupStore(db_path=db_path)
    await store_b.init()

    assert await store_b.is_duplicate("invoices", "evt-persist")


# ---------------------------------------------------------------------------
# Tes 10 — mengunci koneksi dan melaunching ulang penghubung SQLite
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_restart_dedup(tmp_path: Path) -> None:
    """
    DedupStore menstimulasikan pendirian ruang koneksi yang fresh demi perlakuan pemanggilan sqlite3.connect per detiknya,
    sehingga kata 'restarting' itu maknanya ya perakitan DedupStore kembali pada wadah/file aslinya semata.
    Tentunya data duplikat bakal mutlak selalu terbaca/terdeteksi dengan optimal.
    """
    db_path = tmp_path / "restart.db"

    first = DedupStore(db_path=db_path)
    await first.init()
    await first.mark_processed("logs", "evt-restart")
    del first  # Izinkan mekanisme Garbage Collection (GC) menangani sisa referensi/lintasan yang macet

    second = DedupStore(db_path=db_path)
    await second.init()
    assert await second.is_duplicate("logs", "evt-restart")
    assert not await second.is_duplicate("logs", "evt-brand-new")
