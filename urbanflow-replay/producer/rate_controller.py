"""Absolute-deadline event pacing that avoids cumulative sleep drift."""

from __future__ import annotations

import time
from collections.abc import Callable


class RateController:
    def __init__(
        self,
        events_per_second: float,
        clock: Callable[[], float] = time.perf_counter,
        sleeper: Callable[[float], None] = time.sleep,
    ) -> None:
        if events_per_second <= 0:
            raise ValueError("events_per_second must be positive")
        self.events_per_second = float(events_per_second)
        self._clock = clock
        self._sleeper = sleeper
        self._start = clock()
        self._sequence = 0

    def wait_for_next(self) -> None:
        self._sequence += 1
        deadline = self._start + self._sequence / self.events_per_second
        delay = deadline - self._clock()
        if delay > 0:
            self._sleeper(delay)
