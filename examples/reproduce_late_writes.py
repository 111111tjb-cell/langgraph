"""Verify completed-node recovery across a real SQLite/process restart.

Run with the locally installed fork:
    python examples/reproduce_late_writes.py
"""

from __future__ import annotations

import argparse
import json
import sqlite3
import subprocess
import sys
import tempfile
import threading
import time
from pathlib import Path

from langgraph.checkpoint.sqlite import SqliteSaver
from langgraph.graph import END, START, StateGraph
from typing_extensions import TypedDict


class State(TypedDict, total=False):
    a: int
    b: int


def run_stage(directory: Path, stage: str) -> dict:
    directory.mkdir(parents=True, exist_ok=True)
    effects = directory / "effects.sqlite3"
    if stage == "fail" and effects.exists():
        raise ValueError("The fail stage needs a fresh directory.")
    with sqlite3.connect(effects) as connection:
        connection.execute(
            "CREATE TABLE IF NOT EXISTS calls (node TEXT PRIMARY KEY, count INTEGER)"
        )

    def record(node: str) -> int:
        with sqlite3.connect(effects, timeout=10) as connection:
            connection.execute(
                "INSERT INTO calls VALUES (?, 1) ON CONFLICT(node) "
                "DO UPDATE SET count = count + 1",
                (node,),
            )
            return connection.execute(
                "SELECT count FROM calls WHERE node = ?", (node,)
            ).fetchone()[0]

    started = threading.Event()

    def flaky(state: State) -> dict:
        if record("flaky") == 1:
            if not started.wait(10):
                raise TimeoutError("Sibling did not start")
            raise ValueError("expected first-attempt failure")
        return {"a": 1}

    def slow(state: State) -> dict:
        record("slow")
        started.set()
        time.sleep(0.1)
        return {"b": 1}

    builder = StateGraph(State)
    builder.add_node("flaky", flaky)
    builder.add_node("slow", slow)
    builder.add_edge(START, "flaky")
    builder.add_edge(START, "slow")
    builder.add_edge(["flaky", "slow"], END)
    config = {"configurable": {"thread_id": "process-restart"}}
    with SqliteSaver.from_conn_string(str(directory / "checkpoints.sqlite3")) as saver:
        graph = builder.compile(checkpointer=saver)
        if stage == "fail":
            try:
                graph.invoke({}, config, durability="exit")
            except ValueError as error:
                if str(error) != "expected first-attempt failure":
                    raise
            else:
                raise AssertionError("The first attempt should fail")
            checkpoint = saver.get_tuple(config)
            assert checkpoint is not None
            result = {
                "stage": stage,
                "saved_channels": sorted(
                    {key for _, key, _ in checkpoint.pending_writes}
                ),
            }
        else:
            result = {
                "stage": stage,
                "state": graph.invoke(None, config, durability="exit"),
            }
    with sqlite3.connect(effects) as connection:
        result["calls"] = dict(connection.execute("SELECT node, count FROM calls"))
    return result


def run_restart(directory: Path) -> dict:
    reports = []
    for stage in ("fail", "resume"):
        process = subprocess.run(
            [
                sys.executable,
                str(Path(__file__).resolve()),
                stage,
                "--directory",
                str(directory),
            ],
            check=True,
            capture_output=True,
            text=True,
            encoding="utf-8",
            timeout=30,
        )
        reports.append(json.loads(process.stdout))
    passed = (
        "b" in reports[0]["saved_channels"]
        and reports[1]["calls"] == {"flaky": 2, "slow": 1}
        and reports[1]["state"] == {"a": 1, "b": 1}
    )
    return {"passed": passed, "separate_processes": True, "reports": reports}


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "stage", nargs="?", choices=("all", "fail", "resume"), default="all"
    )
    parser.add_argument("--directory", type=Path)
    args = parser.parse_args()
    if args.stage == "all":
        if args.directory is not None:
            result = run_restart(args.directory)
        else:
            with tempfile.TemporaryDirectory(prefix="langgraph-recovery-") as temporary:
                result = run_restart(Path(temporary))
        print(json.dumps(result, indent=2))
        if not result["passed"]:
            raise SystemExit(1)
    elif args.directory is None:
        parser.error("--directory is required for a single stage")
    else:
        print(json.dumps(run_stage(args.directory, args.stage)))


if __name__ == "__main__":
    main()
