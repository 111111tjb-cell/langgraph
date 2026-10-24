# Preserve completed work after parallel failure

这个 fork 针对 [langchain-ai/langgraph#9397](https://github.com/langchain-ai/langgraph/issues/9397)：并行节点中一个先失败，另一个稍后成功时，`durability="exit"` 可能丢失成功节点的待提交写入。恢复执行后，已经完成的节点会再次运行。

## What changes

`Future.done()` does not imply that its commit callback has finished. The loop could flush its exit checkpoint before a late sibling committed, or close its executor while a callback was still submitting checkpoint writes.

The runner now tracks completion of **commit callbacks** separately from the early-stop event. Loop shutdown waits for those callbacks before flushing checkpoints and closing the executor. Async shutdown cancels outstanding work once, then waits for its callbacks. Work that actually finishes despite cancellation is preserved; cancelled work without a result runs again on resume.

The patch changes two production files, `pregel/_runner.py` and `pregel/main.py`, with no public API changes. It does not guarantee exactly-once external side effects or recovery from a process killed before its checkpoint is saved. A sync node that never returns can still block shutdown, as with executor shutdown previously.

## Try the process restart demo

Python 3.12 was used for local validation. From the repository root, create and activate a virtual environment, then install the local packages:

```sh
python -m pip install -e libs/checkpoint -e libs/checkpoint-sqlite -e libs/sdk-py -e libs/prebuilt -e libs/langgraph
python examples/reproduce_late_writes.py
```

No model, API key, or external database is required. The demo launches two independent Python processes against a temporary SQLite database:

1. `flaky` fails while its parallel sibling `slow` finishes and commits.
2. A new process resumes the graph. Only `flaky` is retried.

Expected output includes `"passed": true`, `"separate_processes": true`, a final state of `{"a": 1, "b": 1}`, and call counts `{"flaky": 2, "slow": 1}`. To inspect the database, use a fresh directory with `--directory ./recovery-demo`.

## Regression coverage

`libs/langgraph/tests/test_late_pending_writes.py` covers synchronous and asynchronous execution, all three durability modes, nested graphs, in-memory and SQLite checkpoint savers, encrypted SQLite, in-flight commit callbacks, cancellation, and recovery in a new process. It contains 52 parametrized cases without network calls.

For the test dependencies used by this file and the shared fixtures:

```sh
python -m pip install "langchain-core==1.6.5" pytest syrupy pytest-mock pytest-cov pytest-dotenv pytest-xdist pytest-repeat anyio httpx pycryptodome redis "psycopg[binary,pool]"
python -m pip install -e libs/checkpoint-postgres
cd libs/langgraph
NO_DOCKER=true python -m pytest tests/test_late_pending_writes.py -q -o addopts=
```

On PowerShell, set `$env:NO_DOCKER='true'` first and omit `NO_DOCKER=true` before `python`.

## Validation record

Validated on Windows with Python 3.12.4. Local packages were installed in editable mode. Test dependencies were installed with pip because the repository test group includes `uvloop`, which is unavailable on Windows; this was not a fully locked `uv sync` environment. Ruff 0.16.9 and ty 0.0.84 match the repository lock.

- Initial regression matrix: 39 passed; repeated ten times: 390 passed.
- Expanded matrix including cancellation and real process restart: 52 passed.
- Replacing only the two production files with their original upstream versions made the regression matrix fail, confirming the tests exercise the fix.
- `make format` and `make lint` passed, including package-wide type checking.
- Full non-Docker suite at original base `9d68e5b3eb1a63027b05e9f0152149ea0284dcdb`: **2808 passed, 43 skipped, 3 failed**. The failures were two parametrizations of `test_imp_nested` (stream ordering) and `test_async_streaming_with_functional_api` (timing). Both test functions also fail on unmodified upstream in this Windows environment: the nested test failed 31 of 60 repeated cases; the streaming test failed 10 of 10 repeated cases. These failures have not been hidden or changed.

The fork was subsequently fast-forwarded to upstream `4c52e12` (1.2.17); neither patched production file changed between these bases. The final full suite produced **2816 passed, 43 skipped, 3 failed**: one nested ordering case, the streaming timing case, and `test_replay_creates_branch_preserving_old_checkpoints[sqlite_aio]`. The replay test also failed on unmodified upstream in 15 of 40 repeated cases. The final regression matrix passed all 52 cases after moving the demo to its final path. Machine-readable details are in [VALIDATION_RESULTS.json](VALIDATION_RESULTS.json).

Docker-backed Postgres/Redis tests and other operating systems were not exercised locally. This is a focused recovery fix with explicit validation limits, not an upstream release.

## Attribution

LangGraph is maintained by LangChain and contributors under its existing MIT license. This fork preserves that license and upstream history. The patch, tests, and reproduction were developed with Codex assistance. No upstream pull request is implied.
