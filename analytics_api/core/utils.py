from datetime import datetime, time, timedelta, timezone
from enum import Enum


class TimePeriod(str, Enum):
    TODAY = "today"
    LAST_7_DAYS = "last_7_days"
    LAST_30_DAYS = "last_30_days"


def get_time_bounds(period: TimePeriod):
    """
    Calculates exact start and end datetimes.
    Current period ends exactly NOW, but starts at 00:00:00 of the target day.
    Previous period is calculated for period-over-period growth comparisons.
    """
    now = datetime.now(timezone.utc)
    today_midnight = datetime.combine(now.date(), time.min, tzinfo=timezone.utc)

    if period == TimePeriod.TODAY:
        start_time = today_midnight
        prev_start = start_time - timedelta(days=1)
        prev_end = start_time
    elif period == TimePeriod.LAST_7_DAYS:
        start_time = today_midnight - timedelta(days=7)
        prev_start = start_time - timedelta(days=7)
        prev_end = start_time
    elif period == TimePeriod.LAST_30_DAYS:
        start_time = today_midnight - timedelta(days=30)
        prev_start = start_time - timedelta(days=30)
        prev_end = start_time
    else:
        raise ValueError("Invalid TimePeriod")

    return {
        "current_start": start_time,
        "current_end": now,
        "prev_start": prev_start,
        "prev_end": prev_end,
    }
