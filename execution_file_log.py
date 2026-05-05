"""Append structured execution logs to logs.txt in this directory after each run."""
from __future__ import annotations

import io
import json
import logging
import sys
import time
from datetime import datetime
from pathlib import Path

_LOGS_PATH = Path(__file__).resolve().parent / "logs.txt"


class _TeeStream:
    """Write to the primary stream (console) and capture a copy."""

    __slots__ = ("_primary", "_capture")

    def __init__(self, primary, capture: io.StringIO) -> None:
        self._primary = primary
        self._capture = capture

    def write(self, s: str) -> int:
        if not s:
            return 0
        self._primary.write(s)
        self._capture.write(s)
        return len(s)

    def flush(self) -> None:
        self._primary.flush()
        self._capture.flush()

    def isatty(self) -> bool:
        return self._primary.isatty()

    @property
    def encoding(self) -> str:
        return getattr(self._primary, "encoding", "utf-8") or "utf-8"

    def fileno(self) -> int:
        return self._primary.fileno()


def _patch_root_stream_handlers(
    mapping: list[tuple[object, object]],
) -> list[tuple[logging.Handler, object]]:
    patched: list[tuple[logging.Handler, object]] = []
    for handler in logging.root.handlers:
        if not isinstance(handler, logging.StreamHandler):
            continue
        stream = getattr(handler, "stream", None)
        for old_stream, new_stream in mapping:
            if stream is old_stream:
                handler.stream = new_stream  # type: ignore[assignment]
                patched.append((handler, old_stream))
                break
    return patched


def _append_log_block(date_s: str, time_local_s: str, exec_time_s: str, log_text: str) -> None:
    block = (
        f'date: "{date_s}"\n'
        f'time: "{time_local_s}"\n'
        f'execution_time: "{exec_time_s}"\n'
        f"logs:\n{json.dumps(log_text)}\n\n-----\n\n"
    )
    with open(_LOGS_PATH, "a", encoding="utf-8") as file:
        file.write(block)


def run_with_execution_log(main_callable) -> None:
    """Run main callable and append all terminal output to logs.txt with date/time."""
    buf = io.StringIO()
    old_out, old_err = sys.stdout, sys.stderr
    tee_out = _TeeStream(old_out, buf)
    tee_err = _TeeStream(old_err, buf)
    patched = _patch_root_stream_handlers([(old_out, tee_out), (old_err, tee_err)])
    start = time.perf_counter()
    try:
        sys.stdout = tee_out
        sys.stderr = tee_err
        main_callable()
    finally:
        sys.stdout = old_out
        sys.stderr = old_err
        for handler, original in patched:
            handler.stream = original  # type: ignore[assignment]
        elapsed = time.perf_counter() - start
        # System local timezone (no tzdata / ZoneInfo IANA DB required).
        ended = datetime.now().astimezone()
        date_str = ended.strftime("%Y-%m-%d")
        tz_label = (ended.tzname() or "").strip()
        if not tz_label:
            tz_label = ended.strftime("%z").strip() or "local"
        time_local_str = f"{ended.strftime('%H:%M:%S')} {tz_label}".strip()
        exec_time_str = f"{elapsed:.3f}s"
        try:
            _append_log_block(date_str, time_local_str, exec_time_str, buf.getvalue())
        except OSError:
            pass
