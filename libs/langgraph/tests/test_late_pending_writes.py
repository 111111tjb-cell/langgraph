"""Completed sibling writes must survive a failed parallel superstep."""

import asyncio
import json
import subprocess
import sys
import threading
import time
from collections import Counter
from pathlib import Path

import pytest
from langgraph.checkpoint.base import BaseCheckpointSaver
from typing_extensions import TypedDict

from langgraph._internal._constants import CONFIG_KEY_NODE_FINISHED
from langgraph.graph import END, START, StateGraph
from langgraph.types import Durability

pytestmark = pytest.mark.anyio


class State(TypedDict, total=False):
    a: int
    b: int


@pytest.mark.parametrize("nested", [False, True])
def test_late_sibling_writes_survive_failure(
    sync_checkpointer: BaseCheckpointSaver, durability: Durability, nested: bool
) -> None:
    started = threading.Event()
    failing = threading.Event()
    runs: Counter[str] = Counter()

    def flaky(state: State) -> dict:
        runs["flaky"] += 1
        assert started.wait(5)
        if runs["flaky"] == 1:
            failing.set()
            raise ValueError("transient failure")
        return {"a": 1}

    def slow(state: State) -> dict:
        runs["slow"] += 1
        started.set()
        assert failing.wait(5)
        # Finish after the failing task has woken the runner's early-stop path.
        time.sleep(0.05)
        return {"b": 1}

    builder = StateGraph(State)
    builder.add_node("flaky", flaky)
    if nested:
        child = StateGraph(State)
        child.add_node("work", slow)
        child.add_edge(START, "work")
        child.add_edge("work", END)
        builder.add_node("slow", child.compile())
    else:
        builder.add_node("slow", slow)
    builder.add_edge(START, "flaky")
    builder.add_edge(START, "slow")
    builder.add_edge(["flaky", "slow"], END)
    config = {"configurable": {"thread_id": "late-sibling"}}
    graph = builder.compile(checkpointer=sync_checkpointer)

    with pytest.raises(ValueError, match="transient failure"):
        graph.invoke({}, config, durability=durability)

    checkpoint = sync_checkpointer.get_tuple(config)
    assert checkpoint is not None
    saved = list(checkpoint.pending_writes)
    # Recompile so resumption can only rely on the checkpointer's saved state.
    resumed = builder.compile(checkpointer=sync_checkpointer)
    assert resumed.invoke(None, config, durability=durability) == {"a": 1, "b": 1}
    assert runs == {"flaky": 2, "slow": 1}
    assert any(channel == "b" for _, channel, _ in saved)


def test_inflight_commit_finishes_before_shutdown(
    sync_checkpointer: BaseCheckpointSaver, durability: Durability
) -> None:
    committing = threading.Event()
    failing = threading.Event()
    runs: Counter[str] = Counter()

    def flaky(state: State) -> dict:
        runs["flaky"] += 1
        assert committing.wait(5)
        if runs["flaky"] == 1:
            failing.set()
            raise ValueError("transient failure")
        return {"a": 1}

    def slow(state: State) -> dict:
        runs["slow"] += 1
        return {"b": 1}

    def node_finished(name: str) -> None:
        if name == "slow":
            committing.set()
            assert failing.wait(5)
            # The node future is already done, but its commit has not finished.
            time.sleep(0.05)

    builder = StateGraph(State)
    builder.add_node("flaky", flaky)
    builder.add_node("slow", slow)
    builder.add_edge(START, "flaky")
    builder.add_edge(START, "slow")
    builder.add_edge(["flaky", "slow"], END)
    graph = builder.compile(checkpointer=sync_checkpointer)
    config = {
        "configurable": {
            "thread_id": "inflight-commit",
            CONFIG_KEY_NODE_FINISHED: node_finished,
        }
    }

    with pytest.raises(ValueError, match="transient failure"):
        graph.invoke({}, config, durability=durability)
    assert graph.invoke(None, config, durability=durability) == {"a": 1, "b": 1}
    assert runs == {"flaky": 2, "slow": 1}


@pytest.mark.parametrize("nested", [False, True])
@pytest.mark.parametrize("finish_after_cancel", [False, True])
async def test_late_async_sibling_writes_survive_failure(
    async_checkpointer: BaseCheckpointSaver,
    durability: Durability,
    nested: bool,
    finish_after_cancel: bool,
) -> None:
    started = asyncio.Event()
    failing = asyncio.Event()
    runs: Counter[str] = Counter()

    async def flaky(state: State) -> dict:
        runs["flaky"] += 1
        await asyncio.wait_for(started.wait(), 5)
        if runs["flaky"] == 1:
            failing.set()
            raise ValueError("transient failure")
        return {"a": 1}

    async def slow(state: State) -> dict:
        runs["slow"] += 1
        started.set()
        while True:
            try:
                await asyncio.wait_for(failing.wait(), 5)
                await asyncio.sleep(0.05)
                break
            except asyncio.CancelledError:
                if not finish_after_cancel:
                    raise
                # Finish despite cancellation, as a non-cancellable thread or
                # a coroutine completing a committed operation can do.
                continue
        return {"b": 1}

    builder = StateGraph(State)
    builder.add_node("flaky", flaky)
    if nested:
        child = StateGraph(State)
        child.add_node("work", slow)
        child.add_edge(START, "work")
        child.add_edge("work", END)
        builder.add_node("slow", child.compile())
    else:
        builder.add_node("slow", slow)
    builder.add_edge(START, "flaky")
    builder.add_edge(START, "slow")
    builder.add_edge(["flaky", "slow"], END)
    config = {"configurable": {"thread_id": "late-async-sibling"}}
    graph = builder.compile(checkpointer=async_checkpointer)

    with pytest.raises(ValueError, match="transient failure"):
        await graph.ainvoke({}, config, durability=durability)

    checkpoint = await async_checkpointer.aget_tuple(config)
    assert checkpoint is not None
    saved = list(checkpoint.pending_writes)
    resumed = builder.compile(checkpointer=async_checkpointer)
    assert await resumed.ainvoke(None, config, durability=durability) == {
        "a": 1,
        "b": 1,
    }
    assert runs == {"flaky": 2, "slow": 1 if finish_after_cancel else 2}
    assert any(channel == "b" for _, channel, _ in saved) == finish_after_cancel


def test_sqlite_recovery_in_a_new_process(tmp_path: Path) -> None:
    script = (
        Path(__file__).resolve().parents[3] / "examples" / "reproduce_late_writes.py"
    )
    process = subprocess.run(
        [sys.executable, str(script), "--directory", str(tmp_path)],
        check=True,
        capture_output=True,
        text=True,
        encoding="utf-8",
        timeout=60,
    )
    report = json.loads(process.stdout)
    assert report["passed"] is True
    assert report["separate_processes"] is True
    assert report["reports"][1]["calls"] == {"flaky": 2, "slow": 1}
