import time
from dataclasses import dataclass, field


@dataclass
class StatsCounter:
    """
    Pencatat statistik pergerakan secara in-memory yang mengadopsi pola thread-safe tingkat menengah.
    Seluruh perubahan dilakukan pada sebuah tugas consumer asyncio yang sama,
    jadi tidak diharuskan menggunakan metode penguncian mutiplikasi (lock). Pembacaan oleh fungsi endpoint akan aman terjadi
    lantaran GIL di CPython membuat proses pembacaan pada nilai integer sudah sepenuhnya atomik.
    """

    received: int = 0
    unique_processed: int = 0
    duplicate_dropped: int = 0
    _start_time: float = field(default_factory=time.monotonic, repr=False)

    def record_received(self, count: int = 1) -> None:
        self.received += count

    def record_unique(self, count: int = 1) -> None:
        self.unique_processed += count

    def record_duplicate(self, count: int = 1) -> None:
        self.duplicate_dropped += count

    @property
    def uptime_seconds(self) -> float:
        return time.monotonic() - self._start_time

    def reset(self) -> None:
        """Mereset semua fungsi penghitungan serta jam mula uptime (cocok diterapkan pasa saat testing)"""
        self.received = 0
        self.unique_processed = 0
        self.duplicate_dropped = 0
        self._start_time = time.monotonic()
