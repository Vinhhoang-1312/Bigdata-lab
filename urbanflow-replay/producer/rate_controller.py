"""Absolute-deadline event pacing and proportional virtual clock acceleration."""

from __future__ import annotations

import time
from collections.abc import Callable
from datetime import datetime


class RateController:
    """Fixed events-per-second pacing avoiding cumulative sleep drift."""

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


class VirtualClockController:
    """Proportional time-accelerated pacing controller with optional burst capping.

    Calculates inter-event sleep as:
        sleep_seconds = (current_event_time - previous_event_time) / speedup
    Zero delays for duplicate timestamps and non-negative sleeps for out-of-order events.
    """

    def __init__(
        self,
        speedup: float = 60.0,
        max_rate: float | None = None,
        clock: Callable[[], float] = time.perf_counter,
        sleeper: Callable[[float], None] = time.sleep,
    ) -> None:
        if speedup <= 0:
            raise ValueError("speedup must be positive")
        if max_rate is not None and max_rate <= 0:
            raise ValueError("max_rate must be positive")
        self.speedup = float(speedup)
        self.max_rate = float(max_rate) if max_rate is not None else None
        self._clock = clock
        self._sleeper = sleeper
        self._first_event_dt: datetime | None = None
        self._wall_start: float | None = None
        self._last_emit_wall: float | None = None

    def wait_for_event(self, current_event_dt: datetime) -> None:
        """Paces current event to an absolute virtual schedule, capping bursts via max_rate."""
        now = self._clock()
        if self._first_event_dt is None or self._wall_start is None:
            self._first_event_dt = current_event_dt
            self._wall_start = now
            self._last_emit_wall = now
            return

        # 1. Absolute virtual target time relative to stream start (eliminates drift)
        dt_seconds = (current_event_dt - self._first_event_dt).total_seconds()
        virtual_target = self._wall_start + max(0.0, dt_seconds / self.speedup)

        # 2. Burst-limiter target time (earliest safe time for next emission)
        if self.max_rate is not None and self._last_emit_wall is not None:
            rate_target = self._last_emit_wall + (1.0 / self.max_rate)
            emit_at = max(virtual_target, rate_target)
        else:
            emit_at = virtual_target

        delay = emit_at - now
        if delay > 0:
            self._sleeper(delay)
            self._last_emit_wall = self._clock()
        else:
            self._last_emit_wall = now

