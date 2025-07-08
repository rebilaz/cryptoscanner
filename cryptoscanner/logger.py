import logging
import os

_LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()

logging.basicConfig(
    level=_LOG_LEVEL,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)


def get_logger(name: str) -> logging.Logger:
    """Return a logger configured with the global level."""
    return logging.getLogger(name)
