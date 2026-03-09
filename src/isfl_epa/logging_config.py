"""Centralized logging configuration for the ISFL EPA package.

Provides consistent log formatting across CLI and API modes with
configurable verbosity levels.
"""

from __future__ import annotations

import logging


def setup_logging(level: str = "INFO", *, rich_handler: bool = False) -> None:
    """Configure the root ``isfl_epa`` logger.

    Args:
        level: Log level name (DEBUG, INFO, WARNING, ERROR).
        rich_handler: If True, use ``rich.logging.RichHandler`` for
            pretty terminal output; otherwise use a plain StreamHandler.
    """
    root = logging.getLogger("isfl_epa")
    root.setLevel(getattr(logging, level.upper(), logging.INFO))

    # Avoid adding duplicate handlers on repeated calls
    if root.handlers:
        return

    if rich_handler:
        try:
            from rich.logging import RichHandler

            handler = RichHandler(
                show_time=True,
                show_path=False,
                markup=True,
                rich_tracebacks=True,
            )
            fmt = "%(message)s"
        except ImportError:
            handler = logging.StreamHandler()
            fmt = "%(asctime)s %(name)s %(levelname)s %(message)s"
    else:
        handler = logging.StreamHandler()
        fmt = "%(asctime)s %(name)s %(levelname)s %(message)s"

    handler.setFormatter(logging.Formatter(fmt))
    root.addHandler(handler)


def get_logger(name: str) -> logging.Logger:
    """Return a logger under the ``isfl_epa`` namespace."""
    return logging.getLogger(f"isfl_epa.{name}")
