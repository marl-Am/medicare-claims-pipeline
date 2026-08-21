# ProviderPulse — Project Checklist

A data pipeline and query API for Medicare provider payment records. DuckDB
processes the raw CSV into aggregated tables, PostgreSQL stores them, and
FastAPI serves them. Everything runs under Docker Compose.

## Setup

- [X] Install Python 3.11+, Docker Desktop, and Git
- [X] Create project folder, open it in VS Code
- [X] Create a virtual environment — VS Code Integrated Terminal, project root:
  ```powershell
  python -m venv .venv
  ```
- [X] Activate it — same terminal:
  ```powershell
  .venv\Scripts\Activate.ps1
  ```
- [X] Install core packages inside the activated venv:
  ```powershell
  pip install fastapi uvicorn sqlalchemy[asyncio] asyncpg duckdb pydantic mypy pytest
  ```
- [X] Initialize git: `git init`

## Stage 1 — Get and process the data

- [X] Download the dataset: [Medicare Physician & Other Practitioners by Provider and Service](https://data.cms.gov/provider-summary-by-type-of-service/medicare-physician-other-practitioners/medicare-physician-other-practitioners-by-provider-and-service)
- [X] Inspect the columns in DuckDB before writing any cleaning logic
- [X] Write the ingestion script: typed functions, mypy-checked
- [X] Write cleaning logic: shape raw rows into `cleaned_claims` with a `claim_year` column
- [X] Write aggregation logic: totals by state and by procedure code, per year
- [X] Write output step: save cleaned results to Parquet locally
- [X] Test the pipeline against a small subset before running the full file

## Stage 2 — API and local database

- [X] Write `docker-compose.yml` for a local Postgres container
- [X] Define SQLAlchemy models matching the aggregated schema, fully typed
- [X] Write FastAPI query endpoints (filter by state, procedure code, year)
- [X] Run locally to test — Integrated Terminal, project root:
  ```powershell
  uvicorn app.main:app --reload
  ```
- [X] Load the processed output into local Postgres, confirm queries return correct results

## Stage 3 — Tests

- [X] Refactor `pipeline.py` into typed functions so the SQL logic is testable
- [X] Write `tests/test_pipeline.py` against an in-memory DuckDB database
- [X] Confirm `mypy --strict` passes across every source file
- [X] Write `tests/test_api.py` covering the two summary endpoints and `/health`
- [X] Add `pytest-asyncio` and `httpx` for async endpoint tests:
  ```powershell
  pip install pytest-asyncio httpx
  ```

## Stage 4 — Multi-year data

- [X] Check for schema drift between years before loading a second. Confirmed:
      2023 and 2024 carry the same 28 columns in the same order, so a `SELECT *`
      load works unchanged.
- [X] Switch `cleaned_claims` from `CREATE OR REPLACE TABLE` to delete-then-insert
      on `claim_year`, so a second year appends and a re-run stays idempotent.
      (Qualified in Stage 7: idempotent with respect to the Parquet, not the
      CSV, until `convert_csv_to_parquet` was fixed.)
      `raw_claims` was dropped rather than made appendable — nothing queried it,
      and it doubled storage for no benefit once `cleaned_claims` reads straight
      from Parquet.
- [X] Rebuild the summary tables from the full `cleaned_claims` after each load
- [X] Load 2023 and confirm `claim_year` separates the results correctly —
      verified via `GET /claims/summary/by-state?state=CA`, which returned one
      row per year with different totals
- [X] Reload both years under the current schema (no `is_suppressed`/
      `suppressed_count`, `ANALYZE` after load). Row counts match the prior
      run exactly: 19,442,320 combined, 124 states, 12,876 HCPCS codes.

## Stage 5 — Containerize

- [X] Rewrite `ingestion/export_cleaned.py` as a wrapper around
      `export_cleaned_parquet`, with an optional year argument
- [X] Write a Dockerfile for the API (pinned base image, non-root user).
      Confirmed via `docker compose up --build`: two-stage build on
      `python:3.12-slim-bookworm`, runs as non-root `appuser`, Docker's own
      `HEALTHCHECK` passing repeatedly against `/health`
- [X] Write a Dockerfile for the pipeline. Confirmed via
      `docker compose run --rm pipeline` against a real 2023 reload: image
      built, the DuckDB extension pre-fetch worked, and final counts matched
      exactly (`cleaned_claims` 19,442,320; `summary_by_state` 124;
      `summary_by_hcpcs` 12,876). Very slow, about 3h19m total, for a reason
      unrelated to the container itself; see the new Stage 6 item and
      CHANGELOG.md
- [X] Build locally. `docker compose up --build` (Integrated Terminal,
      project root, Docker Desktop running) built the API image
      successfully as part of bringing up the full stack
- [X] Add both services to `docker-compose.yml` alongside Postgres
- [X] Bring the whole stack up and confirm the API answers from inside a
      container. Confirmed via `GET /claims/summary/by-state?state=CA`
      returning 200 against the already-loaded 2023/2024 data, from inside
      the `providerpulse-api` container

## Stage 6 — Hardening and write-up

- [X] Add server-side validation on every query parameter (year range, two-letter
      state codes, HCPCS code format) so bad input returns 422 rather than an empty list
- [X] Investigate why `suppressed_count` read 0 across every row of real data.
      Confirmed against CMS's own methodology documentation, and directly
      against the 2024 CSV, that CMS excludes low-volume provider-service
      records from the file entirely rather than including them with blanked
      fields. Removed `is_suppressed` and `suppressed_count` from the
      pipeline, models, and schemas.
- [X] Add `ANALYZE` after the bulk load, so Postgres's query planner has
      current statistics before the summary rebuild runs against it
- [X] Fix `main()`'s call order. `analyze_cleaned_claims()` was running
      after both summary rebuilds instead of before them. Confirmed fixed
      by a real timed reload: state summary 4m21s (was 91m34s, benchmark
      4m54s), HCPCS summary 3m51s (was 80m49s, benchmark 4m55s). Both now
      land at or better than benchmark. `ANALYZE` logs between the load and
      the first summary rebuild, exactly where it belongs.
- [X] Add structured logging to the API. `main.py` now logs on startup and
      shutdown, and after every query, the filters applied and rows
      returned. Verified with `mypy --strict`, though see Stage 7: that
      check was resolving packages from an unrelated project's venv at the
      time, so it was re-run against a clean environment before this was
      considered settled.
- [X] Add a `/health` check that confirms the database connection, not just
      the process. Runs a real `SELECT 1` through `get_db()`; verified live
      against an unreachable database, correctly returns 503 with a generic
      body while the full error goes to the log, not the response.
- [X] Push the repository to GitHub. `.gitignore` had two stray lines
      (`data.txt`, `requirements-dev.txt`) silently excluding real project
      files; fixed, `docs/data.txt` restored, `git status --ignored` came
      back clean before the push
- [X] Write the README: architecture, data source, setup steps, the CMS
      suppression-by-omission finding, the `"XX"` state code, plus record
      counts and query timings. One placeholder to fill in yourself:
      `<repo-url>` in the clone command.

## Stage 7 — Audit against an LLM-code checklist

Reviewed every source file against `docs/signs-of-llm-generated-code.md`.
The checklist's main tells were absent: no placeholders, no `cast`/
`type: ignore`/`Any` in source, no invented APIs, no silent error
swallowing. What it did surface were two data-correctness defects and a
broken local environment.

- [X] Fix `convert_csv_to_parquet` reusing a stale Parquet. Replacing a CSV
      and re-running was a silent no-op: the conversion skipped because the
      Parquet existed, so delete-then-insert reloaded the previous file's
      rows and logged an ordinary success. Now compares mtimes and rebuilds
      when the Parquet predates its CSV. Reproduced before the fix: a
      1-row load, CSV replaced with 3 rows, re-run left the database at 1.
- [X] Switch `build_insert_cleaned_claims_sql` to `INSERT ... BY NAME`.
      The positional insert loaded values by column position against a
      table created from the first year's file. Reproduced: two years with
      identical columns in a different order put the HCPCS description into
      the `state` column with no error, because both are VARCHAR. A
      differing type does fail loudly, which is why the real 2023/2024 load
      was unaffected. `BY NAME` fixes the reorder case and still rejects a
      genuinely new column.
- [X] Add `await engine.dispose()` to the API's `lifespan` shutdown. The
      connection pool was left open on exit.
- [X] Add `app/__init__.py`. Without it mypy mapped `app/database.py` to
      both `database` and `app.database` and refused to check anything.
      `mypy --strict .` now reports 12 source files.
- [X] Replace `test_hcpcs_filter_uppercases_the_code`, which tested nothing.
      Every fixture code was all digits and therefore case-invariant, so
      the endpoint's `.upper()` had no coverage. Added a `J1885` row and
      parametrized over both casings. Also removed
      `test_max_claim_year_is_a_sane_upper_bound`, which asserted
      `2100 > 2024` and could not fail.
- [X] Pin `MIN_CLAIM_YEAR`/`MAX_CLAIM_YEAR` in both test files. The
      constants are deliberately duplicated across `app/` and `ingestion/`,
      but nothing guarded against the two copies drifting apart.
- [X] Consolidate the requirements files from four to three. `requirements.txt`
      only combined the two split lists, which `requirements-dev.txt` can do
      directly. Deleted `numpy`, `pandas`, and `polars`: no module in the
      project imports any of them.
- [X] Fix two dependency pins that could never have resolved.
      `httpx==0.28.2` does not exist on PyPI, and `pytest-asyncio==0.22.0`
      is incompatible with the `pytest==9.1.1` pinned three lines above it
      (pytest 9 removed `FixtureDef.unittest`, which that version reads).
      Both went unnoticed because the packages were already present.
- [X] Remove `docker-rag-assistant\.venv\Scripts` from the user `PATH`.
      A sibling project's venv was on `PATH` permanently, so bare `pytest`
      and `mypy` calls in this project silently resolved packages from it.
      The evidence: the suite reported `pytest-asyncio 1.4.0` while this
      project pinned 0.22.0, meaning it had never once run against its own
      pinned version.
- [X] Rebuild `.venv` from scratch and re-verify. 91 tests pass and
      `mypy --strict .` reports no issues across 12 source files, with
      `sqlalchemy`, `duckdb`, and `asyncpg` all confirmed importing from
      this project's own site-packages.
- [X] Document the external `pg-medicare-data` volume in the README.
      Declared `external: true` so `docker compose down -v` cannot destroy
      a loaded database, which also means Compose will not create it and a
      fresh clone failed before Postgres started.
- [X] Strip conversational artifacts from the requirements comments
      ("see the note in chat", "going by what's been shared this session"),
      and correct one that described two pinned lines as unpinned.

## Deferred

- Kubernetes deployment. Dropped from this project to keep the scope tight.
  Worth revisiting on a smaller service where the orchestration is the point
  rather than an addition.