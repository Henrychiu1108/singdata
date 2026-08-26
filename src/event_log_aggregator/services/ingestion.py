"""Durable incremental consumption of an append-only event log."""

import os
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm import Session, sessionmaker

from event_log_aggregator.domain import (
    EventParseError,
    ParsedEvent,
    parse_event,
    window_start_for,
)
from event_log_aggregator.models import (
    ConsumerCheckpoint,
    Event,
    IngestionError,
    UserWindowAggregate,
)


@dataclass(frozen=True)
class IngestionResult:
    """Counts produced while processing currently complete source records."""

    processed_records: int = 0
    inserted_events: int = 0
    rejected_records: int = 0

    def add(self, *, inserted: bool, rejected: bool) -> "IngestionResult":
        """Returns a result including one processed record."""
        return IngestionResult(
            processed_records=self.processed_records + 1,
            inserted_events=self.inserted_events + int(inserted),
            rejected_records=self.rejected_records + int(rejected),
        )


class EventLogConsumer:
    """Reads complete lines and commits source progress with each result."""

    def __init__(
        self,
        session_factory: sessionmaker[Session],
        log_path: Path,
        window_seconds: int,
    ) -> None:
        if window_seconds <= 0:
            raise ValueError("window_seconds must be greater than zero.")
        self._session_factory = session_factory
        self._log_path = log_path
        self._window_seconds = window_seconds
        self._source_name = str(log_path.resolve())

    def process_available_records(self) -> IngestionResult:
        """Consumes every newline-terminated record currently available in the log."""
        if not self._log_path.exists():
            return IngestionResult()

        file_status = self._log_path.stat()
        offset = self._initial_offset(file_status)
        result = IngestionResult()

        with self._log_path.open("rb") as log_file:
            log_file.seek(offset)
            while raw_record := log_file.readline():
                next_offset = log_file.tell()
                if not raw_record.endswith(b"\n"):
                    break
                raw_line = raw_record.rstrip(b"\r\n").decode("utf-8", errors="replace")
                inserted, rejected = self._process_record(
                    raw_line=raw_line,
                    byte_offset=offset,
                    next_offset=next_offset,
                    file_status=file_status,
                )
                result = result.add(inserted=inserted, rejected=rejected)
                offset = next_offset

        return result

    def _initial_offset(self, file_status: os.stat_result) -> int:
        with self._session_factory() as session:
            checkpoint = session.get(ConsumerCheckpoint, self._source_name)
            if checkpoint is None:
                return 0
            identity_changed = (
                checkpoint.file_device != file_status.st_dev
                or checkpoint.file_inode != file_status.st_ino
            )
            if identity_changed or checkpoint.byte_offset > file_status.st_size:
                return 0
            return checkpoint.byte_offset

    def _process_record(
        self,
        *,
        raw_line: str,
        byte_offset: int,
        next_offset: int,
        file_status: os.stat_result,
    ) -> tuple[bool, bool]:
        now = datetime.now(UTC)
        with self._session_factory() as session, session.begin():
            try:
                event = parse_event(raw_line)
            except EventParseError as error:
                session.add(
                    IngestionError(
                        source_name=self._source_name,
                        byte_offset=byte_offset,
                        raw_line=raw_line,
                        reason=str(error),
                        recorded_at=now,
                    )
                )
                self._advance_checkpoint(session, next_offset, file_status)
                return False, True

            inserted = self._insert_event_and_aggregate(session, event, now)
            self._advance_checkpoint(session, next_offset, file_status)
            return inserted, False

    def _insert_event_and_aggregate(
        self, session: Session, event: ParsedEvent, ingested_at: datetime
    ) -> bool:
        event_insert = (
            insert(Event)
            .values(
                event_id=event.event_id,
                user_id=event.user_id,
                event_time=event.event_time,
                amount=event.amount,
                ingested_at=ingested_at,
            )
            .on_conflict_do_nothing(index_elements=[Event.event_id])
            .returning(Event.event_id)
        )
        if session.execute(event_insert).scalar_one_or_none() is None:
            return False

        window_start = window_start_for(event.event_time, self._window_seconds)
        aggregate_insert = insert(UserWindowAggregate).values(
            user_id=event.user_id,
            window_start=window_start,
            window_seconds=self._window_seconds,
            event_count=1,
            total_amount=event.amount,
        )
        aggregate_upsert = aggregate_insert.on_conflict_do_update(
            constraint="user_window_aggregates_user_id_window_start_window_seconds_key",
            set_={
                "event_count": UserWindowAggregate.event_count + 1,
                "total_amount": UserWindowAggregate.total_amount + event.amount,
            },
        )
        session.execute(aggregate_upsert)
        return True

    def _advance_checkpoint(
        self, session: Session, next_offset: int, file_status: os.stat_result
    ) -> None:
        checkpoint = session.get(ConsumerCheckpoint, self._source_name)
        if checkpoint is None:
            checkpoint = ConsumerCheckpoint(source_name=self._source_name)
            session.add(checkpoint)
        checkpoint.byte_offset = next_offset
        checkpoint.file_device = file_status.st_dev
        checkpoint.file_inode = file_status.st_ino