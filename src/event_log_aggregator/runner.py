"""Supervised polling runner for the event-log consumer."""

import logging
from threading import Event, Thread

from event_log_aggregator.services.ingestion import EventLogConsumer

logger = logging.getLogger(__name__)


class ConsumerRunner:
    """Runs log polling in a daemon thread and logs unexpected failures."""

    def __init__(
        self, consumer: EventLogConsumer, poll_interval_seconds: float
    ) -> None:
        self._consumer = consumer
        self._poll_interval_seconds = poll_interval_seconds
        self._stop_event = Event()
        self._thread = Thread(target=self._run, name="event-log-consumer", daemon=True)

    def start(self) -> None:
        """Starts polling once."""
        self._thread.start()

    def stop(self) -> None:
        """Requests shutdown and waits briefly for the current poll to finish."""
        self._stop_event.set()
        self._thread.join(timeout=self._poll_interval_seconds * 2 + 1)

    def _run(self) -> None:
        while not self._stop_event.is_set():
            try:
                self._consumer.process_available_records()
            except Exception:
                logger.exception(
                    "Event-log polling failed; retrying on the next interval."
                )
            self._stop_event.wait(self._poll_interval_seconds)