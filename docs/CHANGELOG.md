# CHANGELOG

## 1. Core Topic & Goal

Build ProviderPulse, a data pipeline and query API for Medicare provider payment
records, using DuckDB, PostgreSQL, FastAPI, and Docker. The API serves
pre-aggregated summaries that anyone can query over HTTP.

Both years are loaded under the current schema and queried successfully: 2024
(9,781,673 rows) and 2023 (9,660,647 rows), 19,442,320 combined. CMS publishes
2019 through 2024; the remaining years are optional and add nothing beyond
volume.

## 2. Key Decisions & Agreements

- Use DuckDB as the processing engine to convert CSV to Parquet and load data into PostgreSQL, rather than pandas.
- PostgreSQL runs in Docker (containerized), not the native Windows service, to simplify local dev.
- Postgres data persists via a named Docker volume (`pg-medicare-data`) rather than inside the container's writable layer.
- `claim_year` is derived from the CSV's parent folder name (e.g. `data/2024/`), assuming one folder per year.
- API queries hit pre-aggregated summary tables (`summary_by_state`, `summary_by_hcpcs`), not the raw multi-million-row `cleaned_claims` table, for performance.
- Credentials managed via `.env` (git-ignored), loaded with `python-dotenv` in Python and natively by Docker Compose.
- `total_services` typed as `float` (not `int`) in both the SQLAlchemy model and Pydantic schema, since CMS data contains fractional service counts.
- `"XX"` state codes (CMS placeholder for unknown/foreign/territory records) are kept in the data rather than filtered out, documented in the README's data-quality section.

### Revised decisions

- **Kubernetes dropped from the project scope.** It was included to practice
  orchestration, not because the workload needed it. A read-only API over
  pre-aggregated tables runs comfortably under Docker Compose. Kubernetes moves
  to a separate, smaller project where the orchestration itself is the subject.
- **`pipeline.py` restructured from a top-level script into typed functions.** The
  original version executed every stage at import time, which made it untestable
  and hardcoded absolute Windows paths into the source. Each stage is now a
  function taking a DuckDB connection and a schema name.
- **Ingestion scripts moved into an `ingestion/` package**, separate from `app/`.
  The two have opposite lifecycles: the pipeline runs once and exits, while the
  API runs continuously. Keeping them apart lets each Dockerfile install only
  what it needs, and keeps DuckDB out of the API image.
- **`raw_claims` removed entirely.** It was a second full copy of the source
  data in Postgres that nothing queried, costing roughly 3 GB and a full
  9.78M-row write per year. `cleaned_claims` now reads directly from the
  Parquet file, which DuckDB scans natively.
- **`cleaned_claims` loads per year via delete-then-insert**, replacing the
  original `CREATE OR REPLACE TABLE` (which wiped every prior year on each
  run). A stored year is deleted before its rows are re-inserted, so a re-run
  is idempotent and a second year appends rather than overwrites. Both summary
  tables are rebuilt in full from the complete `cleaned_claims` after every
  load, so they stay consistent regardless of load order.
- **`ANALYZE` runs against `cleaned_claims` after each load**, via DuckDB's
  `postgres_execute` passthrough. Added after the 2023 load's summary rebuild
  took roughly 28x longer than 2024's for 2x the data — a jump consistent with
  Postgres's query planner working from statistics that predated the bulk
  insert. On the next reload, the equivalent step dropped from 50m06s to
  4m54s (roughly 10x) for the state summary and from 13m22s to 4m55s (roughly
  2.7x) for the HCPCS summary. Not a fully isolated result: about 11 hours
  elapsed between the two runs, long enough for Postgres's own autovacuum to
  have analyzed the table independently of this call. Either way the
  diagnosis holds, and the explicit call means fresh statistics don't depend
  on autovacuum's timing.
- **File paths moved from hardcoded strings to `.env`** (`DATA_ROOT`,
  `CLAIM_YEAR_DIR`, `CSV_FILENAME`). Absolute paths under `C:\Users\marlo\` would
  not resolve inside a container. Values are left unquoted so Docker Compose
  reads the same file correctly, since it does not strip quotes the way
  `python-dotenv` does.
- **`print` replaced with the `logging` module** in the pipeline, and
  `PostgresSettings.__repr__` now redacts the password so credentials cannot
  reach a traceback.
- **`app/database.py` percent-encodes the Postgres user and password**
  (`urllib.parse.quote_plus`) before building the connection URL. A generated
  password containing `@`, `/`, `:`, or `#` would otherwise be parsed as URL
  structure and fail with a misleading error.
- **`ingestion/export_cleaned.py` rewritten** as a thin wrapper around
  `export_cleaned_parquet`, dropping the hardcoded path and the credentials
  interpolated into the ATTACH string. Takes an optional year argument;
  defaults to exporting every loaded year.
- **HCPCS summary groups by code alone**, not code plus description, using
  `ANY_VALUE` for the description. Grouping on both would let a reworded CMS
  description in a later year split one code into two summary rows, breaking
  the composite primary key.
- **State and HCPCS code columns are cast to VARCHAR** in both summary tables.
  `read_csv_auto` infers column types from the data it sees; a year whose
  codes happened to be entirely numeric would otherwise produce an integer
  column, mismatched against the `str`-typed SQLAlchemy models.
- **SQL identifiers and file paths are guarded.** DuckDB accepts bind parameters
  for values but not for schema names, table names, or COPY targets. Identifiers
  pass through a strict allowlist, paths are escaped as SQL literals, and
  `ensure_within` confirms a path sits under the configured data root.
- **`conftest.py` kept at the project root.** Its presence there puts the root on
  `sys.path`, which lets the test modules import `ingestion.pipeline` and
  `app.main`. Bare `pytest` fails without it unless invoked as `python -m pytest`.
  It also sets placeholder database credentials, because `app/database.py` reads
  `PG_HOST` and friends at import time and would raise `KeyError` on a machine
  without a `.env` file.
- **API tests run against in-memory SQLite**, with `get_db` replaced through
  `app.dependency_overrides`. The summary tables use only integers, floats, and
  text, so nothing in the query layer depends on a PostgreSQL feature. This
  assumption breaks if the API ever queries `cleaned_claims` directly.
- **Query parameters are validated server-side.** Year is constrained to
  1990-2100, state to two letters, and HCPCS codes to five alphanumeric
  characters. Malformed input now returns 422 instead of an empty 200, so a
  caller can tell an impossible question from one with no data behind it.
- **HCPCS codes are uppercased on the way in**, matching what the state filter
  already did. Level II codes carry a letter prefix (`G0008`, `J1885`) and are
  uppercase in the source data.
- **`MIN_CLAIM_YEAR` and `MAX_CLAIM_YEAR` are defined separately in
  `app/main.py` and `ingestion/pipeline.py`** rather than shared. Importing
  across that boundary would pull DuckDB into an API image that never calls it.
- **Suppression tracking removed entirely** (`is_suppressed` from
  `cleaned_claims`; `suppressed_count` from both summary tables, models, and
  schemas). The original design assumed CMS suppresses small-count records by
  blanking specific fields within a row. What CMS's own methodology
  documentation states, and what a direct query against the real 2024 CSV
  confirmed (zero NULLs across `Tot_Benes`, `Tot_Srvcs`, and
  `Tot_Bene_Day_Srvcs` in 9.78M rows), is that records derived from 10 or
  fewer beneficiaries are excluded from the published file entirely. There is
  nothing left in the data for a per-row flag to detect. This did not affect
  any served totals: every row present was already unsuppressed, so
  `FILTER (WHERE NOT is_suppressed)` matching 100% of rows was already the
  correct result, not a symptom of a bug. Reference:
  https://data.cms.gov/resources/medicare-physician-other-practitioners-methodology-2022

## 3. Main Takeaways / Output

**Files created:**
- `ingestion/pipeline.py` — typed functions covering each stage: CSV to Parquet, per-year load into `cleaned_claims` via delete-then-insert, `ANALYZE`, and aggregation into `summary_by_state` and `summary_by_hcpcs`.
- `ingestion/inspect_csv.py` — reports row counts and column names for one or more CSVs without loading them, and compares columns across files to detect schema drift.
- `ingestion/export_cleaned.py` — exports `cleaned_claims` to local Parquet without re-running the pipeline; takes an optional year argument.
- `app/models.py` — SQLAlchemy models (`SummaryByState`, `SummaryByHcpcs`) with composite primary keys (`claim_year` + `state`/`hcpcs_code`).
- `app/schemas.py` — Pydantic response schemas (`StateSummary`, `HcpcsSummary`) using `ConfigDict(from_attributes=True)`.
- `app/database.py` — async SQLAlchemy engine/session setup using `asyncpg`, with a `get_db()` FastAPI dependency.
- `app/main.py` — FastAPI app with `GET /claims/summary/by-state`, `GET /claims/summary/by-hcpcs` (both filterable and validated), and `GET /health`.
- `conftest.py` — project-root file that fixes the import path and supplies placeholder database credentials for tests.
- `tests/test_pipeline.py` — 54 tests covering year derivation, multi-year append/idempotency, aggregation arithmetic, injection guards, type-inference guards, and a full pipeline run.
- `tests/test_api.py` — 35 tests covering both endpoints, filter combinations, null handling, input rejection, and the OpenAPI surface.
- `docker-compose.yml` — defines the Postgres 16 container (`pg-medicare`) using `.env` values and the external `pg-medicare-data` volume.

**Confirmed working end to end:**
- `mypy --strict` passes cleanly across all source files.
- All 89 tests pass in roughly 2-16 seconds depending on machine load.
- Multi-year loading validated against real data: 2024 and 2023 both loaded,
  and `GET /claims/summary/by-state?state=CA` returned one row per year with
  distinct, correct totals — confirming the full DuckDB → Postgres →
  SQLAlchemy → FastAPI chain across more than one loaded year.
- Reloaded both years under the current schema (suppression columns removed,
  `ANALYZE` in place). Row counts match the earlier run exactly:
  `cleaned_claims` at 19,442,320, `summary_by_state` at 124 rows (62 states ×
  2 years), `summary_by_hcpcs` at 12,876 rows — matching counts under a
  changed schema is good evidence the column removal didn't disturb the
  underlying grouping.

**Real pipeline timings (Windows, local Docker Postgres):**
- 2024, first load (pre-cleanup): 22m15s total — CSV→Parquet 1m03s, Postgres
  load 11m29s, state summary 1m48s, HCPCS summary 2m21s, export 4m29s.
- 2023, second load (pre-cleanup, append path): 1h21m43s total — CSV→Parquet
  37s, Postgres load 9m47s, state summary 50m06s, HCPCS summary 13m22s,
  export 5m22s. The state summary step took roughly 28x longer for 2x the
  data, which motivated adding `ANALYZE`.
- 2024, reload (post-cleanup, with `ANALYZE`): load 11m25s, state summary
  4m03s, HCPCS summary 3m45s, `ANALYZE` 1m50s. Slower on the summary steps
  than the original 2024-only run despite identical row counts — ordinary
  run-to-run variance, a reminder not to read a single timing too literally.
- 2023, reload (post-cleanup, with `ANALYZE`): load 7m36s, state summary
  4m54s, HCPCS summary 4m55s, `ANALYZE` 55s. Roughly 10x and 2.7x faster than
  the pre-`ANALYZE` equivalents above, though about 11 hours separated the
  two runs, long enough for autovacuum to have contributed independently.

**Key data-quality notes surfaced:**
- CMS excludes any provider-service record derived from 10 or fewer
  beneficiaries from this file before publishing it, rather than including it
  with blanked fields. See the suppression-tracking entry above.
- CMS uses `"XX"` as a placeholder state code for unknown/foreign/territory provider records.
- `total_medicare_payment` is calculated as `Avg_Mdcr_Pymt_Amt * Tot_Srvcs`, since the raw file only provides per-service averages, not row-level totals.
- **No schema drift between 2023 and 2024.** Both files carry the same 28
  columns in the same order, verified with `inspect_csv.py`.
- **CMS serves the same file under two names.** `PHY_R26_P05_V10_D24_Prov_Svc.csv`
  and `Medicare_Physician_Other_Practitioners_by_Provider_and_Service_2024.csv`
  hold identical row counts and identical columns. The byte sizes differ
  slightly (3.03 vs 3.11 GiB), most likely from line-ending or quoting
  conventions applied by the different download paths.

## 4. Action Items & Next Steps

- Push the repository to GitHub. Verify `.gitignore` excludes `data/`,
  `*.parquet`, `.env`, `.venv/`, and the cache folders before the first commit.
- Write a Dockerfile for the FastAPI app (pinned base image, non-root user).
- Write a Dockerfile for the pipeline.
- Add both services to `docker-compose.yml` and verify the stack together.
- Add structured logging to the API. The pipeline already uses `logging`; the
  API does not log anything yet.
- Extend `/health` to confirm the database connection rather than only reporting
  that the process is alive.
- Write the README, including the CMS suppression-by-omission finding, the
  `"XX"` state code, plus record counts and query timings.