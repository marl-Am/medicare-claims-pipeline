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
- [ ] Fix `main()`'s call order. `analyze_cleaned_claims()` currently runs
      after both summary rebuilds instead of before them, so the statistics
      refresh never helps the run it's part of. Confirmed cost on the last
      real run: HCPCS summary took 80m49s instead of a 4m55s benchmark.
      Corrected `main()` written and passes `mypy --strict`; still needs to
      be copied into the project and confirmed with a fresh timed run before
      this is checked off for real.
- [ ] Add structured logging to the API. The pipeline already uses `logging`;
      the API logs nothing.
- [ ] Add a `/health` check that confirms the database connection, not just the process
- [ ] Push the repository to GitHub. Confirm `.gitignore` excludes `data/`,
      `*.parquet`, `.env`, `.venv/`, and the cache folders before the first commit.
- [X] Write the README: architecture, data source, setup steps, the CMS
      suppression-by-omission finding, the `"XX"` state code, plus record
      counts and query timings. One placeholder to fill in yourself:
      `<repo-url>` in the clone command.

## Deferred

- Kubernetes deployment. Dropped from this project to keep the scope tight.
  Worth revisiting on a smaller service where the orchestration is the point
  rather than an addition.