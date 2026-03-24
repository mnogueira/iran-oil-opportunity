"""Monitoring helpers for the paper-trading service."""

from __future__ import annotations

import json
import os
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any


@dataclass(frozen=True, slots=True)
class PaperServicePaths:
    """Well-known service files."""

    output_dir: Path
    event_log: Path
    status_path: Path
    heartbeat_path: Path
    service_dir: Path
    service_state_path: Path
    stdout_log: Path
    stderr_log: Path
    kill_switch_path: Path


def build_paper_service_paths(output_dir: Path, *, kill_switch_path: Path | None = None) -> PaperServicePaths:
    service_dir = output_dir / "service"
    return PaperServicePaths(
        output_dir=output_dir,
        event_log=output_dir / "events.jsonl",
        status_path=output_dir / "status.json",
        heartbeat_path=output_dir / "heartbeat.json",
        service_dir=service_dir,
        service_state_path=service_dir / "service_state.json",
        stdout_log=service_dir / "stdout.log",
        stderr_log=service_dir / "stderr.log",
        kill_switch_path=output_dir / "KILL_SWITCH" if kill_switch_path is None else kill_switch_path,
    )


def append_jsonl(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(dict(payload), default=str) + "\n")


def write_json_atomic(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f"{path.name}.tmp")
    temporary.write_text(
        json.dumps(dict(payload), indent=2, sort_keys=True, default=str),
        encoding="utf-8",
    )
    temporary.replace(path)


def load_json_file(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return payload if isinstance(payload, dict) else {}


def tail_text_lines(path: Path, *, lines: int = 20) -> list[str]:
    if not path.exists():
        return []
    return path.read_text(encoding="utf-8", errors="replace").splitlines()[-max(1, lines) :]


def is_process_alive(pid: int | None) -> bool:
    if pid is None or pid <= 0:
        return False
    if os.name == "nt":
        import ctypes

        query_limited_information = 0x1000
        still_active = 259
        handle = ctypes.windll.kernel32.OpenProcess(query_limited_information, False, int(pid))
        if handle == 0:
            return False
        try:
            exit_code = ctypes.c_ulong()
            result = ctypes.windll.kernel32.GetExitCodeProcess(handle, ctypes.byref(exit_code))
            if result == 0:
                return True
            return int(exit_code.value) == still_active
        finally:
            ctypes.windll.kernel32.CloseHandle(handle)
    try:
        os.kill(int(pid), 0)
    except OSError:
        return False
    return True


def summarize_runtime_health(
    *,
    status_payload: Mapping[str, Any] | None,
    heartbeat_payload: Mapping[str, Any] | None,
    process_alive: bool,
    stale_after_seconds: int = 60,
    now: datetime | None = None,
) -> dict[str, Any]:
    current_status = dict(status_payload or {})
    current_heartbeat = dict(heartbeat_payload or {})
    current_time = datetime.now() if now is None else now

    raw_pid = current_status.get("pid", current_heartbeat.get("pid"))
    try:
        pid = None if raw_pid in (None, "") else int(raw_pid)
    except (TypeError, ValueError):
        pid = None

    runner_state = str(current_status.get("runner_state", current_heartbeat.get("runner_state", "unknown")))
    heartbeat_timestamp = _coerce_timestamp(current_heartbeat.get("timestamp", current_status.get("last_heartbeat_at")))
    heartbeat_age = _age_seconds(current_time, heartbeat_timestamp)
    is_stale = heartbeat_age is None or heartbeat_age > stale_after_seconds

    if runner_state == "failed":
        health = "failed"
    elif runner_state == "stopped":
        health = "stopped" if not process_alive else "stopping"
    elif process_alive and not is_stale:
        health = "running"
    elif process_alive and is_stale:
        health = "stale"
    elif runner_state in {"started", "running"}:
        health = "down"
    else:
        health = "unknown"

    return {
        "health": health,
        "is_stale": is_stale,
        "process_alive": bool(process_alive),
        "pid": pid,
        "runner_state": runner_state,
        "last_heartbeat_at": None if heartbeat_timestamp is None else heartbeat_timestamp.isoformat(),
        "heartbeat_age_seconds": heartbeat_age,
        "last_action": current_status.get("last_action", current_heartbeat.get("last_action")),
        "last_reason": current_status.get("last_reason", current_heartbeat.get("last_reason")),
        "last_signal": current_status.get("last_signal", current_heartbeat.get("last_signal")),
        "failure": current_status.get("failure"),
        "stop_reason": current_status.get("stop_reason"),
    }


def _coerce_timestamp(value: object) -> datetime | None:
    if value in (None, ""):
        return None
    if isinstance(value, datetime):
        return value
    if not isinstance(value, str):
        return None
    try:
        return datetime.fromisoformat(value)
    except ValueError:
        return None


def _age_seconds(now: datetime, value: datetime | None) -> float | None:
    if value is None:
        return None
    return round(max((now - value).total_seconds(), 0.0), 3)
