"""Domain types and pure event-time calculations."""

from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation


@dataclass(frozen=True)
class ParsedEvent:
    """A validated source event before persistence."""

    event_id: str
    user_id: str
    event_time: datetime
    amount: Decimal


class EventParseError(ValueError):
    """Raised when one physical log record cannot become an event."""


def parse_event(raw_line: str) -> ParsedEvent:
    """Parses a CSV-like log record with exactly four required fields."""
    fields = raw_line.split(",")
    if len(fields) != 4:
        raise EventParseError("Expected exactly four comma-separated fields.")

    event_id, user_id, event_time_value, amount_value = (
        field.strip() for field in fields
    )
    if not event_id:
        raise EventParseError("eventId must not be empty.")
    if not user_id:
        raise EventParseError("userId must not be empty.")

    try:
        event_time = datetime.fromisoformat(event_time_value)
    except ValueError as error:
        raise EventParseError("eventTime must be ISO-8601.") from error
    if event_time.tzinfo is None:
        event_time = event_time.replace(tzinfo=UTC)
    else:
        event_time = event_time.astimezone(UTC)

    try:
        amount = Decimal(amount_value)
    except InvalidOperation as error:
        raise EventParseError("amount must be a decimal number.") from error
    if not amount.is_finite():
        raise EventParseError("amount must be finite.")

    return ParsedEvent(event_id, user_id, event_time, amount)


def window_start_for(event_time: datetime, window_seconds: int) -> datetime:
    """Returns the UTC start of the fixed event-time window containing an event."""
    if window_seconds <= 0:
        raise ValueError("window_seconds must be greater than zero.")
    if event_time.tzinfo is None:
        raise ValueError("event_time must include a timezone.")

    event_time_utc = event_time.astimezone(UTC)
    epoch_seconds = int(event_time_utc.timestamp())
    return datetime.fromtimestamp(
        epoch_seconds - (epoch_seconds % window_seconds), tz=UTC
    )