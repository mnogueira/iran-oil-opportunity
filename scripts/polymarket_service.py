"""Service wrapper for the continuous Polymarket monitor."""

from __future__ import annotations

import argparse
import json
import os
import signal
import subprocess
import sys
import time
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[1]
SRC_ROOT = REPO_ROOT / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from iran_oil_opportunity.monitoring import (
    build_paper_service_paths,
    is_process_alive,
    load_json_file,
    summarize_runtime_health,
    tail_text_lines,
    write_json_atomic,
)


@dataclass(frozen=True, slots=True)
class _LaunchedProcess:
    pid: int


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Manage the Polymarket monitor service.")
    subparsers = parser.add_subparsers(dest="command", required=True)

    start = subparsers.add_parser("start", help="Launch the continuous Polymarket monitor.")
    _add_common_args(start)
    start.add_argument("--start-timeout-seconds", type=int, default=30)
    start.set_defaults(handler=handle_start)

    stop = subparsers.add_parser("stop", help="Stop the running Polymarket monitor.")
    _add_common_args(stop)
    stop.add_argument("--timeout-seconds", type=int, default=20)
    stop.add_argument("--force", action="store_true")
    stop.set_defaults(handler=handle_stop)

    status = subparsers.add_parser("status", help="Show Polymarket monitor health.")
    _add_common_args(status)
    status.add_argument("--stale-after-seconds", type=int, default=600)
    status.add_argument("--json", action="store_true")
    status.set_defaults(handler=handle_status)

    tail = subparsers.add_parser("tail", help="Tail Polymarket logs or events.")
    _add_common_args(tail)
    tail.add_argument("--source", choices=("events", "stdout", "stderr"), default="events")
    tail.add_argument("--lines", type=int, default=20)
    tail.set_defaults(handler=handle_tail)
    return parser


def _add_common_args(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--output-dir", default=".tradebot/polymarket_monitor")
    parser.add_argument("--markets-output", default="data/processed/polymarket_markets.csv")
    parser.add_argument("--scores-output", default="data/processed/polymarket_scores.csv")
    parser.add_argument("--poll-seconds", type=int, default=300)
    parser.add_argument("--kill-switch-path")


def resolve_paths(args: argparse.Namespace):
    output_dir = Path(args.output_dir)
    kill_switch_path = output_dir / "KILL_SWITCH" if args.kill_switch_path is None else Path(args.kill_switch_path)
    return build_paper_service_paths(output_dir, kill_switch_path=kill_switch_path)


def discover_pid(paths) -> int | None:
    service_state = load_json_file(paths.service_state_path)
    status_payload = load_json_file(paths.status_path)
    for payload in (service_state, status_payload):
        raw_pid = payload.get("pid")
        try:
            if raw_pid in (None, ""):
                continue
            return int(raw_pid)
        except (TypeError, ValueError):
            continue
    return None


def build_runner_command(args: argparse.Namespace, paths) -> list[str]:
    return [
        sys.executable,
        "-u",
        str(REPO_ROOT / "scripts" / "polymarket_monitor.py"),
        "--markets-output",
        str(args.markets_output),
        "--scores-output",
        str(args.scores_output),
        "--poll-seconds",
        str(args.poll_seconds),
        "--status-output",
        str(paths.status_path),
        "--heartbeat-output",
        str(paths.heartbeat_path),
        "--event-log-output",
        str(paths.event_log),
        "--kill-switch-path",
        str(paths.kill_switch_path),
    ]


def launch_runner(command: list[str], *, paths) -> _LaunchedProcess | subprocess.Popen[bytes]:
    paths.output_dir.mkdir(parents=True, exist_ok=True)
    paths.service_dir.mkdir(parents=True, exist_ok=True)
    if os.name == "nt":
        wrapper_path = paths.service_dir / "polymarket_wrapper.ps1"
        wrapper_path.write_text(_build_windows_wrapper(command, paths), encoding="utf-8")
        start_command = (
            f"$p = Start-Process -FilePath 'powershell' "
            f"-ArgumentList '-NoProfile','-ExecutionPolicy','Bypass','-File',{_quote_powershell(str(wrapper_path))} "
            f"-WindowStyle Hidden -PassThru; "
            "Write-Output $p.Id"
        )
        completed = subprocess.run(
            ["powershell", "-NoProfile", "-Command", start_command],
            cwd=str(REPO_ROOT),
            check=True,
            capture_output=True,
            text=True,
        )
        raw_pid = next((line.strip() for line in reversed(completed.stdout.splitlines()) if line.strip()), "")
        try:
            return _LaunchedProcess(pid=int(raw_pid))
        except ValueError as exc:
            raise RuntimeError(f"Unable to determine wrapper pid from Start-Process output: {completed.stdout!r}") from exc

    stdout_handle = paths.stdout_log.open("ab", buffering=0)
    stderr_handle = paths.stderr_log.open("ab", buffering=0)
    try:
        process = subprocess.Popen(
            command,
            cwd=str(REPO_ROOT),
            stdin=subprocess.DEVNULL,
            stdout=stdout_handle,
            stderr=stderr_handle,
            close_fds=True,
            start_new_session=True,
        )
    finally:
        stdout_handle.close()
        stderr_handle.close()
    return process


def _build_windows_wrapper(command: list[str], paths) -> str:
    quoted_command = " ".join(_quote_powershell(argument) for argument in command + ["--once"])
    working_dir = _quote_powershell(str(REPO_ROOT))
    kill_switch = _quote_powershell(str(paths.kill_switch_path))
    stdout_log = _quote_powershell(str(paths.stdout_log))
    stderr_log = _quote_powershell(str(paths.stderr_log))
    poll_seconds = 300
    for index, token in enumerate(command):
        if token == "--poll-seconds" and index + 1 < len(command):
            try:
                poll_seconds = max(30, int(command[index + 1]))
            except ValueError:
                poll_seconds = 300
            break
    return "\n".join(
        [
            f"Set-Location {working_dir}",
            "while ($true) {",
            f"  if (Test-Path {kill_switch}) {{ exit 0 }}",
            f"  & {quoted_command} >> {stdout_log} 2>> {stderr_log}",
            f"  Start-Sleep -Seconds {poll_seconds}",
            "}",
            "",
        ]
    )


def _quote_powershell(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def wait_for_startup(*, paths, pid: int, timeout_seconds: int) -> dict[str, Any]:
    deadline = time.monotonic() + max(1, timeout_seconds)
    while time.monotonic() < deadline:
        if not is_process_alive(pid):
            stderr_tail = "\n".join(tail_text_lines(paths.stderr_log, lines=20))
            raise RuntimeError(
                "Runner exited before becoming healthy."
                + ("" if not stderr_tail else f"\nRecent stderr:\n{stderr_tail}")
            )
        status_payload = load_json_file(paths.status_path)
        if status_payload.get("runner_state") in {"started", "running"}:
            heartbeat_payload = load_json_file(paths.heartbeat_path)
            return summarize_runtime_health(
                status_payload=status_payload,
                heartbeat_payload=heartbeat_payload,
                process_alive=True,
                stale_after_seconds=max(60, timeout_seconds * 2),
            )
        time.sleep(1)
    raise RuntimeError("Timed out waiting for the Polymarket monitor to publish startup status.")


def terminate_process(pid: int) -> None:
    if os.name == "nt":
        subprocess.run(["taskkill", "/PID", str(pid), "/T", "/F"], check=False, capture_output=True, text=True)
        return
    os.kill(pid, signal.SIGTERM)


def handle_start(args: argparse.Namespace) -> int:
    paths = resolve_paths(args)
    existing_pid = discover_pid(paths)
    if is_process_alive(existing_pid):
        raise RuntimeError(f"Polymarket monitor already appears to be running with pid={existing_pid}.")
    if paths.kill_switch_path.exists():
        paths.kill_switch_path.unlink()
    command = build_runner_command(args, paths)
    process = launch_runner(command, paths=paths)
    write_json_atomic(
        paths.service_state_path,
        {
            "pid": process.pid,
            "started_at": datetime.now(tz=UTC).isoformat(),
            "command": command,
            "event_log_path": str(paths.event_log),
            "status_path": str(paths.status_path),
            "heartbeat_path": str(paths.heartbeat_path),
            "stdout_log_path": str(paths.stdout_log),
            "stderr_log_path": str(paths.stderr_log),
            "kill_switch_path": str(paths.kill_switch_path),
        },
    )
    summary = wait_for_startup(paths=paths, pid=process.pid, timeout_seconds=args.start_timeout_seconds)
    print(json.dumps(summary, indent=2, sort_keys=True))
    return 0


def handle_stop(args: argparse.Namespace) -> int:
    paths = resolve_paths(args)
    pid = discover_pid(paths)
    paths.kill_switch_path.parent.mkdir(parents=True, exist_ok=True)
    paths.kill_switch_path.write_text("stop\n", encoding="utf-8")
    if not is_process_alive(pid):
        print(json.dumps({"stopped": True, "pid": pid, "reason": "not_running"}, indent=2))
        return 0

    deadline = time.monotonic() + max(1, args.timeout_seconds)
    while time.monotonic() < deadline and is_process_alive(pid):
        time.sleep(1)
    force_terminated = False
    if is_process_alive(pid) and args.force:
        terminate_process(pid)
        force_terminated = True
        time.sleep(1)
    print(
        json.dumps(
            {
                "stopped": not is_process_alive(pid),
                "pid": pid,
                "force_terminated": force_terminated,
                "kill_switch_path": str(paths.kill_switch_path),
            },
            indent=2,
            sort_keys=True,
        )
    )
    return 0 if not is_process_alive(pid) else 1


def handle_status(args: argparse.Namespace) -> int:
    paths = resolve_paths(args)
    status_payload = load_json_file(paths.status_path)
    heartbeat_payload = load_json_file(paths.heartbeat_path)
    pid = discover_pid(paths)
    summary = summarize_runtime_health(
        status_payload=status_payload,
        heartbeat_payload=heartbeat_payload,
        process_alive=is_process_alive(pid),
        stale_after_seconds=args.stale_after_seconds,
    )
    if args.json:
        print(json.dumps(summary, indent=2, sort_keys=True))
        return 0
    for key, value in summary.items():
        print(f"{key}: {value}")
    return 0


def handle_tail(args: argparse.Namespace) -> int:
    paths = resolve_paths(args)
    target = {"events": paths.event_log, "stdout": paths.stdout_log, "stderr": paths.stderr_log}[args.source]
    for line in tail_text_lines(target, lines=args.lines):
        print(line)
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    return args.handler(args)


if __name__ == "__main__":
    raise SystemExit(main())
