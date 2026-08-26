"""Database engine and transaction utilities."""

from collections.abc import Generator

from sqlalchemy import Engine, create_engine
from sqlalchemy.orm import DeclarativeBase, Session, sessionmaker


class Base(DeclarativeBase):
    """Base class for database models."""


def create_database_engine(database_url: str) -> Engine:
    """Creates a pooled synchronous PostgreSQL engine."""
    return create_engine(database_url, pool_pre_ping=True)


def create_session_factory(database_url: str) -> sessionmaker[Session]:
    """Creates sessions bound to the configured database."""
    return sessionmaker(
        bind=create_database_engine(database_url), expire_on_commit=False
    )


def session_scope(
    session_factory: sessionmaker[Session],
) -> Generator[Session, None, None]:
    """Yields one session and commits or rolls back its transaction."""
    session = session_factory()
    try:
        yield session
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()