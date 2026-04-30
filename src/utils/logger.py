"""Structured logging utility used across all pipeline components."""
from __future__ import annotations

import logging
import sys


def get_logger(name: str, level: str = "INFO") -> logging.Logger:
    """
    Return a configured logger. Call once per module:
        logger = get_logger(__name__, cfg.log_level)
    """
    logger = logging.getLogger(name)

    if not logger.handlers:
        handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(
            logging.Formatter(
                fmt="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
                datefmt="%Y-%m-%dT%H:%M:%S",
            )
        )
        logger.addHandler(handler)

    logger.setLevel(getattr(logging, level.upper(), logging.INFO))
    logger.propagate = False
    return logger
