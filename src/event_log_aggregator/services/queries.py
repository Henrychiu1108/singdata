"""Read-side services backed by persisted window aggregates."""

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal

from sqlalchemy import func, select
from sqlalchemy.orm import Session, sessionmaker

from event_log_aggregator.models import UserWindowAggregate


@dataclass(frozen=True)
class WindowStatistic:
    """One user's persisted fixed-window measure."""

    window_start: datetime
    event_count: int
    total_amount: Decimal


@dataclass(frozen=True)
class TopUser:
    """One ranked user over a requested event-time range."""

    user_id: str
    event_count: int
    total_amount: Decimal


class AggregateQueryService:
    """Executes all externally visible aggregate reads."""

    def __init__(self, session_factory: sessionmaker[Session]) -> None:
        self._session_factory = session_factory

    def user_windows(
        self,
        user_id: str,
        start: datetime,
        end: datetime,
        window_seconds: int,
    ) -> list[WindowStatistic]:
        """Gets fully contained persisted windows for one user over [start, end)."""
        query = (
            select(UserWindowAggregate)
            .where(
                UserWindowAggregate.user_id == user_id,
                UserWindowAggregate.window_seconds == window_seconds,
                UserWindowAggregate.window_start >= start,
                UserWindowAggregate.window_start < end,
            )
            .order_by(UserWindowAggregate.window_start)
        )
        with self._session_factory() as session:
            rows = session.scalars(query).all()
        return [
            WindowStatistic(row.window_start, row.event_count, row.total_amount)
            for row in rows
        ]

    def top_users(
        self,
        start: datetime,
        end: datetime,
        window_seconds: int,
        limit: int,
    ) -> list[TopUser]:
        """Ranks users by total amount over complete buckets in [start, end)."""
        query = (
            select(
                UserWindowAggregate.user_id,
                func.sum(UserWindowAggregate.event_count).label("event_count"),
                func.sum(UserWindowAggregate.total_amount).label("total_amount"),
            )
            .where(
                UserWindowAggregate.window_seconds == window_seconds,
                UserWindowAggregate.window_start >= start,
                UserWindowAggregate.window_start < end,
            )
            .group_by(UserWindowAggregate.user_id)
            .order_by(
                func.sum(UserWindowAggregate.total_amount).desc(),
                UserWindowAggregate.user_id.asc(),
            )
            .limit(limit)
        )
        with self._session_factory() as session:
            rows = session.execute(query).all()
        return [TopUser(row.user_id, row.event_count, row.total_amount) for row in rows]