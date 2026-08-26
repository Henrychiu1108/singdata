"""Create durable event aggregation tables.

Revision ID: 0001_initial_schema
Revises:
Create Date: 2026-08-26
"""

import sqlalchemy as sa
from alembic import op

revision = "0001_initial_schema"
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "events",
        sa.Column("event_id", sa.String(length=128), primary_key=True),
        sa.Column("user_id", sa.String(length=128), nullable=False),
        sa.Column("event_time", sa.DateTime(timezone=True), nullable=False),
        sa.Column("amount", sa.Numeric(precision=18, scale=2), nullable=False),
        sa.Column("ingested_at", sa.DateTime(timezone=True), nullable=False),
    )
    op.create_index("ix_events_user_id", "events", ["user_id"])
    op.create_index("ix_events_event_time", "events", ["event_time"])
    op.create_table(
        "user_window_aggregates",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("user_id", sa.String(length=128), nullable=False),
        sa.Column("window_start", sa.DateTime(timezone=True), nullable=False),
        sa.Column("window_seconds", sa.Integer(), nullable=False),
        sa.Column("event_count", sa.BigInteger(), nullable=False),
        sa.Column("total_amount", sa.Numeric(precision=18, scale=2), nullable=False),
        sa.UniqueConstraint(
            "user_id",
            "window_start",
            "window_seconds",
            name="user_window_aggregates_user_id_window_start_window_seconds_key",
        ),
    )
    op.create_index(
        "ix_user_window_aggregates_user_id",
        "user_window_aggregates",
        ["user_id"],
    )
    op.create_index(
        "ix_user_window_aggregates_window_start",
        "user_window_aggregates",
        ["window_start"],
    )
    op.create_table(
        "consumer_checkpoints",
        sa.Column("source_name", sa.String(length=256), primary_key=True),
        sa.Column("byte_offset", sa.BigInteger(), nullable=False),
        sa.Column("file_device", sa.BigInteger(), nullable=True),
        sa.Column("file_inode", sa.BigInteger(), nullable=True),
    )
    op.create_table(
        "ingestion_errors",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("source_name", sa.String(length=256), nullable=False),
        sa.Column("byte_offset", sa.BigInteger(), nullable=False),
        sa.Column("raw_line", sa.Text(), nullable=False),
        sa.Column("reason", sa.Text(), nullable=False),
        sa.Column("recorded_at", sa.DateTime(timezone=True), nullable=False),
    )
    op.create_index(
        "ix_ingestion_errors_source_name",
        "ingestion_errors",
        ["source_name"],
    )


def downgrade() -> None:
    op.drop_index("ix_ingestion_errors_source_name", table_name="ingestion_errors")
    op.drop_table("ingestion_errors")
    op.drop_table("consumer_checkpoints")
    op.drop_index(
        "ix_user_window_aggregates_window_start", table_name="user_window_aggregates"
    )
    op.drop_index(
        "ix_user_window_aggregates_user_id", table_name="user_window_aggregates"
    )
    op.drop_table("user_window_aggregates")
    op.drop_index("ix_events_event_time", table_name="events")
    op.drop_index("ix_events_user_id", table_name="events")
    op.drop_table("events")