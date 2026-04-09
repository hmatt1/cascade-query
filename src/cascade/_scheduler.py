from __future__ import annotations

import threading
from collections import deque
from typing import Any, Callable


class WorkStealingExecutor:
    def __init__(self, workers: int) -> None:
        self._workers = max(1, workers)
        self._deques = [deque() for _ in range(self._workers)]
        self._lock = threading.RLock()
        self._pending = 0
        self._shutdown = False
        self._wake = threading.Condition(self._lock)

    def submit_indexed(self, index: int, fn: Callable[[], Any]) -> None:
        worker_idx = index % self._workers
        with self._wake:
            self._deques[worker_idx].append((index, fn))
            self._pending += 1
            self._wake.notify_all()

    def run(self, size: int) -> list[Any]:
        results: list[Any] = [None] * size
        errors: list[BaseException | None] = [None] * size
        threads: list[threading.Thread] = []

        def worker_loop(worker_idx: int) -> None:
            while True:
                task = self._take_task(worker_idx)
                if task is None:
                    return
                idx, fn = task
                try:
                    results[idx] = fn()
                except BaseException as exc:  # pragma: no cover - defensive branch
                    errors[idx] = exc
                finally:
                    with self._wake:
                        self._pending -= 1
                        self._wake.notify_all()

        for idx in range(self._workers):
            thread = threading.Thread(target=worker_loop, args=(idx,), daemon=True)
            threads.append(thread)
            thread.start()

        for thread in threads:
            thread.join()

        for error in errors:
            if error is not None:
                raise error
        return results

    def _take_task(self, worker_idx: int) -> tuple[int, Callable[[], Any]] | None:
        with self._wake:
            while True:
                local = self._deques[worker_idx]
                if local:
                    return local.pop()
                for idx, bucket in enumerate(self._deques):
                    if idx == worker_idx:
                        continue
                    if bucket:
                        return bucket.popleft()
                if self._pending == 0 or self._shutdown:
                    return None
                self._wake.wait(timeout=0.01)
