"""Persistent entities for ingestion, recovery, and querying."""

from datetime import datetime
from decimal import Decimal

from sqlalchemy import (
    BigInteger,
    DateTime,
    Integer,
    Numeric,
    String,
    Text,
    UniqueConstraint,
)
from sqlalchemy.orm import Mapped, mapped_column

from event_log_aggregator.database import Base


class Event(Base):
    """A valid source event; event_id uniqueness provides idempotency."""

    __tablename__ = "events"

    event_id: Mapped[str] = mapped_column(String(128), primary_key=True)
    user_id: Mapped[str] = mapped_column(String(128), index=True)
    event_time: Mapped[datetime] = mapped_column(DateTime(timezone=True), index=True)
    amount: Mapped[Decimal] = mapped_column(Numeric(18, 2))
    ingested_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))


class UserWindowAggregate(Base):
    """An event-time fixed-window aggregate for one user."""

    __tablename__ = "user_window_aggregates"
    __table_args__ = (
        UniqueConstraint("user_id", "window_start", "window_seconds"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    user_id: Mapped[str] = mapped_column(String(128), index=True)
    window_start: Mapped[datetime] = mapped_column(DateTime(timezone=True), index=True)
    window_seconds: Mapped[int] = mapped_column(Integer)
    event_count: Mapped[int] = mapped_column(BigInteger, default=0)
    total_amount: Mapped[Decimal] = mapped_column(Numeric(18, 2), default=Decimal("0"))


class ConsumerCheckpoint(Base):
    """Durable byte position for one consumed log file."""

    __tablename__ = "consumer_checkpoints"

    source_name: Mapped[str] = mapped_column(String(256), primary_key=True)
    byte_offset: Mapped[int] = mapped_column(BigInteger, default=0)
    file_device: Mapped[int | None] = mapped_column(BigInteger, nullable=True)
    file_inode: Mapped[int | None] = mapped_column(BigInteger, nullable=True)


class IngestionError(Base):
    """An invalid physical record retained for audit and diagnosis."""

    __tablename__ = "ingestion_errors"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    source_name: Mapped[str] = mapped_column(String(256), index=True)
    byte_offset: Mapped[int] = mapped_column(BigInteger)
    raw_line: Mapped[str] = mapped_column(Text)
    reason: Mapped[str] = mapped_column(Text)
    recorded_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))