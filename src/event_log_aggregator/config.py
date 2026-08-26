"""Application configuration."""

from pathlib import Path

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Configuration read from environment variables or an optional .env file."""

    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    database_url: str = "postgresql+psycopg://events:events@localhost:5432/events"
    events_log_path: Path = Path("events.log")
    poll_interval_seconds: float = 0.25
    default_window_seconds: int = 60

    def validate_window_size(self, window_seconds: int) -> int:
        """Ensures a requested window matches the service's fixed aggregation size."""
        if window_seconds != self.default_window_seconds:
            raise ValueError(
                "Only the configured fixed window size is supported: "
                f"{self.default_window_seconds} seconds."
            )
        return window_seconds