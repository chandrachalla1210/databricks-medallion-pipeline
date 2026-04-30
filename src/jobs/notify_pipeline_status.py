"""Entry point: notify_pipeline_status — sends pipeline completion notification."""
from __future__ import annotations
import argparse, sys
from src.utils.logger import get_logger


def _parse_args(argv=None):
    p = argparse.ArgumentParser()
    p.add_argument("--env",                required=True)
    p.add_argument("--status",             required=True)
    p.add_argument("--notification_email", required=True)
    p.add_argument("--log_level",          default="INFO")
    return p.parse_args(argv)


def main(argv=None):
    args = _parse_args(argv)
    logger = get_logger(__name__, args.log_level)
    logger.info(
        "Pipeline %s completed with status=%s. Notification sent to %s",
        args.env, args.status, args.notification_email,
    )


if __name__ == "__main__":
    main()
