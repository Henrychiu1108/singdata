"""Unit tests for pure parsing and event-time behavior."""

from datetime import UTC, datetime
from decimal import Decimal

import pytest

from event_log_aggregator.domain import EventParseError, parse_event, window_start_for


def test_parse_event_interprets_naive_timestamp_as_utc() -> None:
    event = parse_event("e1001,u01,2026-08-26T10:00:03,15.25")

    assert event.event_id == "e1001"
    assert event.event_time == datetime(2026, 8, 26, 10, 0, 3, tzinfo=UTC)
    assert event.amount == Decimal("15.25")


@pytest.mark.parametrize(
    "raw_line",
    [
        "bad_line",
        ",u01,2026-08-26T10:00:03,15",
        "e1001,,2026-08-26T10:00:03,15",
        "e1001,u01,not-a-time,15",
        "e1001,u01,2026-08-26T10:00:03,nan",
    ],
)
def test_parse_event_rejects_invalid_records(raw_line: str) -> None:
    with pytest.raises(EventParseError):
        parse_event(raw_line)


def test_window_start_uses_event_time_boundary() -> None:
    assert window_start_for(
        datetime(2026, 8, 26, 10, 0, 59, tzinfo=UTC), 60
    ) == datetime(2026, 8, 26, 10, 0, 0, tzinfo=UTC)
    assert window_start_for(
        datetime(2026, 8, 26, 10, 1, 0, tzinfo=UTC), 60
    ) == datetime(2026, 8, 26, 10, 1, 0, tzinfo=UTC)