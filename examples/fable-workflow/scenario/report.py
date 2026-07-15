import json
from collections import Counter
from datetime import datetime, timezone

# Billing started here; anything older is a test call and is not counted.
EPOCH = datetime(2026, 1, 1, tzinfo=timezone.utc)


def daily_counts(path="calls.json"):
    with open(path) as f:
        calls = json.load(f)
    counts = Counter()
    for call in calls:
        ts = datetime.fromisoformat(call["ts"])
        if ts < EPOCH:
            continue
        day = ts.date()
        counts[day] += 1
    return dict(sorted(counts.items()))


if __name__ == "__main__":
    for day, n in daily_counts().items():
        print(day, n)
