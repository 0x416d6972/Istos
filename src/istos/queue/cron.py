"""A small, dependency-free cron parser for periodic scheduling.

Supports the five standard fields — minute, hour, day-of-month, month,
day-of-week — with ``*``, ranges (``1-5``), steps (``*/15``, ``1-5/2``) and lists
(``1,15,30``). Day-of-week is 0-6 with Sunday 0 (7 also accepted for Sunday).
When both day-of-month and day-of-week are restricted the match is the union, as
in Vixie cron. Seconds are not supported (one-minute resolution).
"""

from __future__ import annotations

import datetime as _dt
from typing import Set, Tuple


class CronError(ValueError):
    """Raised for a malformed cron expression."""


# (low, high) for each of the five fields, in order.
_RANGES: Tuple[Tuple[int, int], ...] = ((0, 59), (0, 23), (1, 31), (1, 12), (0, 6))


def _parse_field(field: str, lo: int, hi: int) -> Set[int]:
    values: Set[int] = set()
    for part in field.split(","):
        step = 1
        base = part
        if "/" in part:
            base, step_s = part.split("/", 1)
            try:
                step = int(step_s)
            except ValueError:
                raise CronError(f"bad step in {part!r}")
        if base == "*":
            start, end = lo, hi
        elif "-" in base:
            a, b = base.split("-", 1)
            try:
                start, end = int(a), int(b)
            except ValueError:
                raise CronError(f"bad range in {part!r}")
        else:
            try:
                start = end = int(base)
            except ValueError:
                raise CronError(f"bad value in {part!r}")
        if step < 1 or start < lo or end > hi or start > end:
            raise CronError(f"field out of range: {part!r} (allowed {lo}-{hi})")
        values.update(range(start, end + 1, step))
    return values


class CronSchedule:
    """A parsed cron expression that can compute its next firing time."""

    def __init__(self, expr: str) -> None:
        fields = expr.split()
        if len(fields) != 5:
            raise CronError(
                f"cron expression needs 5 fields (min hour dom month dow), got {len(fields)}"
            )
        self.expr = expr
        self.minute = _parse_field(fields[0], *_RANGES[0])
        self.hour = _parse_field(fields[1], *_RANGES[1])
        self.dom = _parse_field(fields[2], *_RANGES[2])
        self.month = _parse_field(fields[3], *_RANGES[3])
        # 7 is Sunday, including inside lists and ranges ("1,7", "5-7"). The
        # field is parsed through 7 and then folded back into 0.
        dow = _parse_field(fields[4], 0, 7)
        if 7 in dow:
            dow.discard(7)
            dow.add(0)
        self.dow = dow
        self._dom_restricted = fields[2] != "*"
        self._dow_restricted = fields[4] != "*"

    def _matches(self, t: _dt.datetime) -> bool:
        if t.minute not in self.minute or t.hour not in self.hour or t.month not in self.month:
            return False
        cron_dow = (t.weekday() + 1) % 7  # Python Mon=0..Sun=6 → cron Sun=0..Sat=6
        dom_ok = t.day in self.dom
        dow_ok = cron_dow in self.dow
        if self._dom_restricted and self._dow_restricted:
            return dom_ok or dow_ok
        return dom_ok and dow_ok

    def _day_matches(self, t: _dt.datetime) -> bool:
        cron_dow = (t.weekday() + 1) % 7  # Python Mon=0..Sun=6 → cron Sun=0..Sat=6
        dom_ok = t.day in self.dom
        dow_ok = cron_dow in self.dow
        if self._dom_restricted and self._dow_restricted:
            return dom_ok or dow_ok
        return dom_ok and dow_ok

    def next_after(self, after: _dt.datetime) -> _dt.datetime:
        """The first firing strictly after ``after`` (minute resolution).

        Jumps to the next matching month, day, hour, or minute instead of
        testing every minute. The horizon is eight years so a February 29
        expression still resolves across non-leap years.
        """
        t = after.replace(second=0, microsecond=0) + _dt.timedelta(minutes=1)
        horizon = t + _dt.timedelta(days=366 * 8)
        minutes = sorted(self.minute)
        hours = sorted(self.hour)
        while t < horizon:
            if t.month not in self.month:
                year, month = t.year, t.month + 1
                if month == 13:
                    year, month = year + 1, 1
                t = t.replace(year=year, month=month, day=1, hour=0, minute=0)
                continue
            if not self._day_matches(t):
                t = (t + _dt.timedelta(days=1)).replace(hour=0, minute=0)
                continue
            if t.hour not in self.hour:
                later = [h for h in hours if h > t.hour]
                if later:
                    t = t.replace(hour=later[0], minute=minutes[0])
                else:
                    t = (t + _dt.timedelta(days=1)).replace(hour=hours[0], minute=minutes[0])
                continue
            if t.minute not in self.minute:
                later = [m for m in minutes if m > t.minute]
                if later:
                    t = t.replace(minute=later[0])
                else:
                    t = t.replace(minute=minutes[0]) + _dt.timedelta(hours=1)
                continue
            return t
        raise CronError(f"no matching time within eight years for {self.expr!r}")
