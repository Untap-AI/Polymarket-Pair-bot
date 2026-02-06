"""Structured logging configuration for file + console output.

When the Rich dashboard is active, console output is suppressed so that
``rich.live.Live`` can own stdout.  All log output still goes to the
log file.
"""

import logging
from pathlib import Path


def setup_logging(
    level: str = "INFO",
    log_file: str | None = None,
    enable_console: bool = True,
) -> None:
    """Configure structured logging.

    Args:
        level: Logging level string (DEBUG, INFO, WARNING, ERROR).
        log_file: Path to log file.  ``None`` → no file output.
        enable_console: If ``False``, suppress the console handler
            (used when the rich dashboard takes over stdout).
    """
    root_logger = logging.getLogger()
    root_logger.setLevel(getattr(logging, level.upper(), logging.INFO))

    # Clear existing handlers to avoid duplicates on re-init
    root_logger.handlers.clear()

    # --- Console handler (optional) ---
    if enable_console:
        console_fmt = logging.Formatter(
            "%(asctime)s | %(levelname)-8s | %(name)-28s | %(message)s",
            datefmt="%H:%M:%S",
        )
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(console_fmt)
        root_logger.addHandler(console_handler)

    # --- File handler (if specified) ---
    if log_file:
        log_path = Path(log_file)
        log_path.parent.mkdir(parents=True, exist_ok=True)

        file_fmt = logging.Formatter(
            "%(asctime)s | %(levelname)-8s | %(name)-28s | %(funcName)s:%(lineno)d | %(message)s",
            datefmt="%Y-%m-%dT%H:%M:%S",
        )
        file_handler = logging.FileHandler(log_file, encoding="utf-8")
        file_handler.setFormatter(file_fmt)
        root_logger.addHandler(file_handler)

    # Suppress noisy third-party loggers
    logging.getLogger("websockets").setLevel(logging.WARNING)
    logging.getLogger("aiohttp").setLevel(logging.WARNING)

    logging.getLogger(__name__).info(
        "Logging initialized — level=%s, file=%s, console=%s",
        level, log_file or "none", enable_console,
    )
