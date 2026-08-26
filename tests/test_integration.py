"""Integration tests for PostgreSQL-backed recovery and query correctness."""

from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path

from fastapi.testclient import TestClient
from sqlalchemy import func, select
from sqlalchemy.orm import Session, sessionmaker

from event_log_aggregator.api import create_app
from event_log_aggregator.config import Settings
from event_log_aggregator.models import ConsumerCheckpoint, Event, IngestionError
from event_log_aggregator.services.ingestion import EventLogConsumer
from event_log_aggregator.services.queries import AggregateQueryService


def test_consumer_tolerates_bad_data_deduplicates_and_uses_event_time(
    session_factory: sessionmaker[Session], tmp_path: Path
) -> None:
    log_path = tmp_path / "events.log"
    log_path.write_text(
        "e1001,u01,2026-08-26T10:00:03,15\n"
        "e1002,u02,2026-08-26T10:00:05,20\n"
        "e1001,u01,2026-08-26T10:00:03,15\n"
        "e1003,u01,2026-08-26T09:59:58,8\n"
        "bad_line\n",
        encoding="utf-8",
    )

    result = EventLogConsumer(session_factory, log_path, 60).process_available_records()

    assert result.processed_records == 5
    assert result.inserted_events == 3
    assert result.rejected_records == 1
    queries = AggregateQueryService(session_factory)
    user_windows = queries.user_windows(
        "u01",
        datetime(2026, 8, 26, 9, 59, tzinfo=UTC),
        datetime(2026, 8, 26, 10, 2, tzinfo=UTC),
        60,
    )
    assert [(row.event_count, row.total_amount) for row in user_windows] == [
        (1, Decimal("8.00")),
        (1, Decimal("15.00")),
    ]
    assert [user.user_id for user in queries.top_users(
        datetime(2026, 8, 26, 9, 59, tzinfo=UTC),
        datetime(2026, 8, 26, 10, 2, tzinfo=UTC),
        60,
        2,
    )] == ["u02", "u01"]
    with session_factory() as session:
        assert session.scalar(select(func.count()).select_from(Event)) == 3
        assert session.scalar(select(func.count()).select_from(IngestionError)) == 1


def test_restart_and_partial_line_do_not_double_count_or_lose_event(
    session_factory: sessionmaker[Session], tmp_path: Path
) -> None:
    log_path = tmp_path / "events.log"
    log_path.write_text("e1001,u01,2026-08-26T10:00:03,15\n", encoding="utf-8")

    first_consumer = EventLogConsumer(session_factory, log_path, 60)
    assert first_consumer.process_available_records().inserted_events == 1
    with log_path.open("a", encoding="utf-8") as log_file:
        log_file.write("e1002,u01,2026-08-26T10:00:04")

    restarted_consumer = EventLogConsumer(session_factory, log_path, 60)
    assert restarted_consumer.process_available_records().processed_records == 0
    with log_path.open("a", encoding="utf-8") as log_file:
        log_file.write(",5\n")
    assert restarted_consumer.process_available_records().inserted_events == 1
    assert restarted_consumer.process_available_records().inserted_events == 0

    with session_factory() as session:
        assert session.scalar(select(func.count()).select_from(Event)) == 2
        checkpoint = session.scalar(select(ConsumerCheckpoint))
        assert checkpoint is not None
        assert checkpoint.byte_offset == log_path.stat().st_size


def test_api_exposes_window_stats_and_deterministic_top_k(
    session_factory: sessionmaker[Session], tmp_path: Path
) -> None:
    log_path = tmp_path / "events.log"
    log_path.write_text(
        "e1,u02,2026-08-26T10:00:00,10\n"
        "e2,u01,2026-08-26T10:00:01,10\n",
        encoding="utf-8",
    )
    EventLogConsumer(session_factory, log_path, 60).process_available_records()
    settings = Settings(events_log_path=log_path, default_window_seconds=60)
    app = create_app(settings, session_factory, start_consumer=False)

    with TestClient(app) as client:
        response = client.get(
            "/top-users",
            params={
                "start": "2026-08-26T10:00:00Z",
                "end": "2026-08-26T10:01:00Z",
                "k": 2,
            },
        )

    assert response.status_code == 200
    assert [item["user_id"] for item in response.json()] == ["u01", "u02"]