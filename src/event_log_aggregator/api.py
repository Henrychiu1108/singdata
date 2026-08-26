"""HTTP interface for health checks and persisted aggregate queries."""

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from datetime import datetime
from decimal import Decimal
from typing import Annotated

from fastapi import FastAPI, HTTPException, Query
from pydantic import BaseModel
from sqlalchemy import text
from sqlalchemy.orm import Session, sessionmaker

from event_log_aggregator.config import Settings
from event_log_aggregator.database import create_session_factory
from event_log_aggregator.runner import ConsumerRunner
from event_log_aggregator.services.ingestion import EventLogConsumer
from event_log_aggregator.services.queries import AggregateQueryService


class WindowStatisticResponse(BaseModel):
    """JSON representation of a fixed-window statistic."""

    window_start: datetime
    event_count: int
    total_amount: Decimal


class TopUserResponse(BaseModel):
    """JSON representation of a ranked user."""

    user_id: str
    event_count: int
    total_amount: Decimal


def create_app(
    settings: Settings | None = None,
    session_factory: sessionmaker[Session] | None = None,
    start_consumer: bool = True,
) -> FastAPI:
    """Creates the API application and optionally starts background consumption."""
    configured_settings = settings or Settings()
    configured_session_factory = session_factory or create_session_factory(
        configured_settings.database_url
    )
    query_service = AggregateQueryService(configured_session_factory)
    consumer = EventLogConsumer(
        configured_session_factory,
        configured_settings.events_log_path,
        configured_settings.default_window_seconds,
    )

    @asynccontextmanager
    async def lifespan(_: FastAPI) -> AsyncIterator[None]:
        runner = ConsumerRunner(consumer, configured_settings.poll_interval_seconds)
        if start_consumer:
            runner.start()
        try:
            yield
        finally:
            if start_consumer:
                runner.stop()

    app = FastAPI(title="Event Log Aggregator", lifespan=lifespan)

    @app.get("/healthz")
    def healthz() -> dict[str, str]:
        """Returns liveness without requiring a database operation."""
        return {"status": "ok"}

    @app.get("/readyz")
    def readyz() -> dict[str, str]:
        """Checks that PostgreSQL is available for durable consumption."""
        try:
            with configured_session_factory() as session:
                session.execute(text("SELECT 1"))
        except Exception as error:
            raise HTTPException(
                status_code=503, detail="Database is unavailable."
            ) from error
        return {"status": "ready"}

    @app.get("/users/{user_id}/windows", response_model=list[WindowStatisticResponse])
    def get_user_windows(
        user_id: str,
        start: datetime,
        end: datetime,
        window_seconds: Annotated[int, Query(ge=1)] = (
            configured_settings.default_window_seconds
        ),
    ) -> list[WindowStatisticResponse]:
        """Gets one user's complete aggregate buckets for [start, end)."""
        _validate_range_and_window(configured_settings, start, end, window_seconds)
        return [
            WindowStatisticResponse(
                window_start=statistic.window_start,
                event_count=statistic.event_count,
                total_amount=statistic.total_amount,
            )
            for statistic in query_service.user_windows(
                user_id, start, end, window_seconds
            )
        ]

    @app.get("/top-users", response_model=list[TopUserResponse])
    def get_top_users(
        start: datetime,
        end: datetime,
        k: Annotated[int, Query(ge=1, le=100)] = 10,
        window_seconds: Annotated[int, Query(ge=1)] = (
            configured_settings.default_window_seconds
        ),
    ) -> list[TopUserResponse]:
        """Gets the top users by summed amount over complete buckets in [start, end)."""
        _validate_range_and_window(configured_settings, start, end, window_seconds)
        return [
            TopUserResponse(
                user_id=user.user_id,
                event_count=user.event_count,
                total_amount=user.total_amount,
            )
            for user in query_service.top_users(start, end, window_seconds, k)
        ]

    return app


def _validate_range_and_window(
    settings: Settings, start: datetime, end: datetime, window_seconds: int
) -> None:
    if start.tzinfo is None or end.tzinfo is None:
        raise HTTPException(
            status_code=422, detail="start and end must include timezones."
        )
    if start >= end:
        raise HTTPException(status_code=422, detail="start must be before end.")
    try:
        settings.validate_window_size(window_seconds)
    except ValueError as error:
        raise HTTPException(status_code=422, detail=str(error)) from error


app = create_app()