"""PostgreSQL integration-test fixtures."""

import os
from collections.abc import Generator

import pytest
from alembic import command
from alembic.config import Config
from sqlalchemy import delete
from sqlalchemy.orm import Session, sessionmaker

from event_log_aggregator.database import create_session_factory
from event_log_aggregator.models import (
    ConsumerCheckpoint,
    Event,
    IngestionError,
    UserWindowAggregate,
)


@pytest.fixture(scope="session")
def database_url() -> str:
    """Provides the PostgreSQL URL supplied by Docker Compose or CI."""
    value = os.environ.get("TEST_DATABASE_URL")
    if value is None:
        pytest.skip("Set TEST_DATABASE_URL to run PostgreSQL integration tests.")
    return value


@pytest.fixture(scope="session")
def migrated_session_factory(database_url: str) -> sessionmaker[Session]:
    """Applies the deployed Alembic migrations before integration tests run."""
    alembic_config = Config("alembic.ini")
    alembic_config.set_main_option("sqlalchemy.url", database_url)
    command.upgrade(alembic_config, "head")
    return create_session_factory(database_url)


@pytest.fixture
def session_factory(
    migrated_session_factory: sessionmaker[Session],
) -> Generator[sessionmaker[Session], None, None]:
    """Returns an empty migrated database to every integration test."""
    with migrated_session_factory() as session, session.begin():
        session.execute(delete(IngestionError))
        session.execute(delete(ConsumerCheckpoint))
        session.execute(delete(UserWindowAggregate))
        session.execute(delete(Event))
    yield migrated_session_factory