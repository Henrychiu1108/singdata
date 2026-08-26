# Event Log Aggregator

A durable, single-node service that incrementally consumes an append-only
`events.log`, stores validated events in PostgreSQL, and exposes event-time
fixed-window statistics and Top K users over HTTP.

## Architecture

```text
events.log -> EventLogConsumer -> PostgreSQL -> FastAPI query endpoints
                  |                  |
                  +-> errors         +-> checkpoint + event-time aggregates
```

The application is separated into HTTP (`api.py`), application services
(`services/`), and persistence (`models.py`, `database.py`, Alembic) layers.
PostgreSQL is the source of truth: the process never depends on an in-memory
aggregate cache for correctness.

## Guarantees and Assumptions

- `eventId` is globally idempotent. The first valid event with an ID is stored;
  later copies do not update aggregates.
- Each newline-terminated physical record is processed in one database
  transaction: event insert, aggregate upsert or ingestion-error audit, and
  checkpoint update commit together.
- The checkpoint is a byte offset plus file identity. A restart resumes at the
  committed offset. A truncated or replaced file restarts at byte zero; event
  ID idempotency prevents historical events from being double-counted.
- An unterminated final line is not consumed until its terminating newline is
  appended.
- Invalid lines are written to `ingestion_errors`, then consumption continues.
- Naive ISO-8601 times are interpreted as UTC. Offset-aware times are converted
  to UTC.
- Windows are fixed/tumbling and default to 60 seconds. Buckets use
  `[window_start, window_start + window_seconds)`. Late and out-of-order events
  are accepted and update the bucket determined by their own event time.
- Query time ranges are `[start, end)` and aggregate complete persisted buckets
  whose `window_start` falls in that range. Use boundaries aligned to the
  configured window to avoid partial-window ambiguity.
- This project targets one local consumer. Multi-process consumer coordination,
  authentication, and distributed file-system rotation semantics are outside
  its scope.

## Run with Docker

Docker Desktop must be running.

PowerShell:

```powershell
Copy-Item examples\events.log data\events.log
docker compose up --build
```

Git Bash:

```bash
cp examples/events.log data/events.log
docker compose up --build
```

The API starts at `http://localhost:8000`; the app runs Alembic migrations
before starting the consumer. The sample log deliberately includes invalid
records, which are recorded in `ingestion_errors` while valid events continue
to be processed. Append a valid event while it is running:

PowerShell:

```powershell
Add-Content data\events.log 'e1004,u03,2026-08-26T10:00:20,42'
```

Git Bash:

```bash
printf '%s\n' 'e1004,u03,2026-08-26T10:00:20,42' >> data/events.log
```

Useful endpoints:

```text
GET /healthz
GET /readyz
GET /users/u01/windows?start=2026-08-26T09:59:00Z&end=2026-08-26T10:02:00Z&window_seconds=60
GET /top-users?start=2026-08-26T09:59:00Z&end=2026-08-26T10:02:00Z&k=3&window_seconds=60
```

Top K ordering is deterministic: `total_amount` descending, then `user_id`
ascending. Decimal amounts are rendered as JSON strings to preserve precision.

To stop containers while retaining PostgreSQL state:

```powershell
docker compose down
```

To remove all local state:

```powershell
docker compose down --volumes
```

## Local Development and Tests

Install Python 3.12 or later, then:

```powershell
python -m pip install -e '.[dev]'
ruff check .
mypy src
pytest
```

Without `TEST_DATABASE_URL`, `pytest` runs the pure parser/window tests and
skips the PostgreSQL integration tests. GitHub Actions runs the full suite
against its dedicated `events_test` database. Do not point integration tests at
the Compose application's `events` database: the test fixture clears its tables
before each test. To run integration tests locally, provision a separate
PostgreSQL database and set `TEST_DATABASE_URL`, for example:

```powershell
$env:TEST_DATABASE_URL = 'postgresql+psycopg://events:events@localhost:5432/events_test'
pytest
```

## GitHub Publication

```powershell
git init
git add .
git commit -m 'Initial event log aggregator'
git branch -M main
git remote add origin https://github.com/USERNAME/event-log-aggregator.git
git push -u origin main
```

Do not commit `.env`, PostgreSQL volumes, or real production logs. The included
GitHub Actions workflow runs Ruff, mypy, and pytest against PostgreSQL on every
push and pull request.