"""Cron expression parsing and next-firing computation."""

import datetime as dt

import pytest

from istos.queue.cron import CronError, CronSchedule


def test_every_minute():
    c = CronSchedule("* * * * *")
    base = dt.datetime(2026, 7, 12, 10, 30, 15)
    assert c.next_after(base) == dt.datetime(2026, 7, 12, 10, 31)


def test_top_of_each_hour():
    c = CronSchedule("0 * * * *")
    assert c.next_after(dt.datetime(2026, 7, 12, 10, 30)) == dt.datetime(2026, 7, 12, 11, 0)


def test_daily_midnight():
    c = CronSchedule("0 0 * * *")
    assert c.next_after(dt.datetime(2026, 7, 12, 10, 0)) == dt.datetime(2026, 7, 13, 0, 0)


def test_step_and_list():
    c = CronSchedule("*/15 * * * *")
    assert c.next_after(dt.datetime(2026, 7, 12, 10, 1)) == dt.datetime(2026, 7, 12, 10, 15)
    c2 = CronSchedule("5,35 * * * *")
    assert c2.next_after(dt.datetime(2026, 7, 12, 10, 10)) == dt.datetime(2026, 7, 12, 10, 35)


def test_day_of_week_sunday():
    # 2026-07-12 is a Sunday. "0 9 * * 0" → 09:00 on Sundays.
    c = CronSchedule("0 9 * * 0")
    nxt = c.next_after(dt.datetime(2026, 7, 12, 10, 0))  # after 9am Sunday
    assert nxt == dt.datetime(2026, 7, 19, 9, 0)         # next Sunday
    assert nxt.weekday() == 6                            # Sunday


def test_dom_dow_union():
    # Vixie cron: when both dom and dow are set, match either. "0 0 13 * 5" =
    # midnight on the 13th OR any Friday.
    c = CronSchedule("0 0 13 * 5")
    # 2026-07-13 is a Monday (the 13th) → matches via dom.
    assert c.next_after(dt.datetime(2026, 7, 12, 12, 0)) == dt.datetime(2026, 7, 13, 0, 0)


def test_invalid_expressions():
    for bad in ["* * * *", "60 * * * *", "* 24 * * *", "*/0 * * * *", "5-1 * * * *"]:
        with pytest.raises(CronError):
            CronSchedule(bad)
