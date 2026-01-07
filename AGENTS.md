# Repository Guidelines

## Project Structure & Module Organization
- Root-level scripts are the entry points: `api.py` starts the FastAPI layer, `demo.py` exercises ingestion/query flows, and `tests.py` holds the unittest surface.
- Persistence lives in `tsdb_fdb.py` with access helpers and the FoundationDB wiring; `query_engine.py` wraps the SQL layer plus `ts_bucket`. Shared settings live under `config/`.
- Container resources (`Dockerfile`, `docker/`) mirror the same services for reproducible development.
- Keep supporting modules cohesive—add helpers near the most relevant entry point (e.g., FastAPI utilities beside `api.py`), and avoid scattering shared state across unrelated folders.

## Build, Test, and Development Commands
- Use `uv sync --dev --no-install-package datafusion` (followed by `uv run --no-project maturin develop --uv` when rebuilding the binding) to bootstrap dependencies with better native-toolchain caching.
- `python3 -m venv .venv` / `.venv/bin/pip install -r requirements.txt` remain available but only until `uv` aligns the virtual environment.
- `.venv/bin/python demo.py`: runs the synthetic workload, registers the `ts_bucket` UDF, and prints both raw and aggregated rows against a FoundationDB cluster.
- `FDB_CLUSTER_FILE=/etc/foundationdb/fdb.cluster .venv/bin/uvicorn api:app --reload --host 0.0.0.0 --port 8000`: launches the HTTP API once the cluster is reachable.
- `.venv/bin/python tests.py`: runs `unittest` cases for ring buffer behavior and SQL queries; the script skips cleanly if FoundationDB is unavailable.
- `podman build -t tsdb-proto .` and the corresponding `podman run` snippet from `README.md` bring up a container with embedded FoundationDB plus the FastAPI server for end-to-end experimentation.

## Coding Style & Naming Conventions
- Follow general Python best practices (PEP 8, 4-space indentation, short imports).
- Prefer `snake_case` for functions, variables, and filenames (e.g., `query_engine.py`); use descriptive names that hint at the ring buffer or SQL intent.
- Keep modules small: each file’s primary responsibility should be obvious from its name (e.g., `tsdb_fdb.py` owns FoundationDB wiring, `query_engine.py` owns SQL orchestration).
- Document non-obvious behaviors with docstrings or inline comments; avoid redundant comments that restate the code.

## Testing Guidelines
- Tests are located in `tests.py` and implemented with `unittest`. Name new tests `test_*` so the runner discovers them automatically.
- The suite touches FoundationDB directly—confirm `fdb.cluster` is readable via the default path or `FDB_CLUSTER_FILE` before running.
- Add one test case per scenario (ring round-trip, rate detection, SQL query) and verify they pass locally before pushing changes.

## Commit & Pull Request Guidelines
- Commit messages should follow a concise, imperative style (e.g., `feat: add query helpers`, `fix: guard counter reset`). No explicit template exists in history, so default to the conventional format.
- PRs need a descriptive summary, mention any linked issue IDs, and note whether the FoundationDB dependency is required. Attach screenshots only if the change affects observability (logs/metrics).
- Run the demo, API, or tests locally as applicable and list the commands you executed in the PR description; highlight any manual verification steps.

## Environment & Configuration Tips
- Store cluster metadata as `fdb.cluster` in the repo root or set `FDB_CLUSTER_FILE` to the desired location before running scripts.
- The `docker/` directory contains service definitions used by the containerized image; the provided `Dockerfile` targets the dev workflow only (runs `runit` services, enables `uvicorn` APIs, and now installs `git` for convenience).
- The runit-managed FoundationDB service rewrites `/etc/foundationdb/fdb.cluster` to `127.0.0.1:${FDB_PORT:-4501}`, configures `single memory`, and writes logs to `/var/log/foundationdb`. Point `FDB_CLUSTER_FILE` at that path when running tests or the API inside the container.
