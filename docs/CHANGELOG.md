# CHANGELOG

## 1. Core Topic & Goal

Build ProviderPulse, a data pipeline and query API for Medicare provider payment records (~9.78M rows), using DuckDB, PostgreSQL, FastAPI, and Docker, with Kubernetes deployment planned as a later phase.

## 2. Key Decisions & Agreements

- Use DuckDB as the processing engine to convert CSV to Parquet and load data into PostgreSQL, rather than pandas.
- PostgreSQL runs in Docker (containerized), not the native Windows service, to simplify local dev.
- Postgres data persists via a named Docker volume (`pg-medicare-data`) rather than inside the container's writable layer.
- Suppressed/redacted rows (CMS-nulled fields) are kept and flagged with an `is_suppressed` boolean column, not dropped, so the API can report data-quality stats.
- Aggregate sums (`total_beneficiaries`, `total_services`, `total_medicare_payment`) exclude suppressed rows via `FILTER (WHERE NOT is_suppressed)`.
- `claim_year` is derived from the CSV's parent folder name (e.g. `data/2024/`), assuming one folder per year.
- API queries hit pre-aggregated summary tables (`summary_by_state`, `summary_by_hcpcs`), not the raw 9.78M-row `cleaned_claims` table, for performance.
- Project structure simplified from the original proposal: single `pipeline.py` instead of a `pipeline/` package, routes folded into `app/main.py` instead of a `routers/` folder, to be split later only if complexity grows.
- Credentials managed via `.env` (git-ignored), loaded with `python-dotenv` in Python and natively by Docker Compose.
- `docker-compose.yml` placed at project root (not in `k8s/`), since Kubernetes manifests and local dev config serve different purposes.
- `total_services` typed as `float` (not `int`) in both the SQLAlchemy model and Pydantic schema, since CMS data contains fractional service counts.
- `"XX"` state codes (CMS placeholder for unknown/foreign/territory records) are kept in the data rather than filtered out, to be documented in the README rather than silently dropped.

## 3. Main Takeaways / Output

**Files created:**
- `pipeline.py` — loads CSV, converts to Parquet, loads raw data into Postgres (`raw_claims`), cleans and flags suppressed rows (`cleaned_claims`), aggregates into `summary_by_state` and `summary_by_hcpcs`.
- `export_cleaned.py` — standalone script to export `cleaned_claims` to a local Parquet file without re-running the full pipeline.
- `app/models.py` — SQLAlchemy models (`SummaryByState`, `SummaryByHcpcs`) with composite primary keys (`claim_year` + `state`/`hcpcs_code`).
- `app/schemas.py` — Pydantic response schemas (`StateSummary`, `HcpcsSummary`) using `ConfigDict(from_attributes=True)`.
- `app/database.py` — async SQLAlchemy engine/session setup using `asyncpg`, with a `get_db()` FastAPI dependency.
- `app/main.py` — FastAPI app with `GET /claims/summary/by-state`, `GET /claims/summary/by-hcpcs` (both filterable by year/state/hcpcs_code), and `GET /health`.
- `docker-compose.yml` — defines the Postgres 16 container (`pg-medicare`) using `.env` values and the external `pg-medicare-data` volume.

**Confirmed working end to end:**
- Full pipeline run: 9,781,673 rows processed into `raw_claims` and `cleaned_claims`; 62 rows in `summary_by_state`; 6,471 rows in `summary_by_hcpcs`.
- `mypy .` passes cleanly across all source files.
- FastAPI endpoints return correct JSON data when queried via Swagger UI (`/docs`), confirming the full DuckDB → Postgres → SQLAlchemy → FastAPI chain.

**Key data-quality notes surfaced:**
- CMS suppresses `Tot_Benes`, `Tot_Srvcs`, `Tot_Bene_Day_Srvcs` for small patient counts (privacy) — handled via `is_suppressed` flag.
- CMS uses `"XX"` as a placeholder state code for unknown/foreign/territory provider records.
- `total_medicare_payment` is calculated as `Avg_Mdcr_Pymt_Amt * Tot_Srvcs`, since the raw file only provides per-service averages, not row-level totals.

## 4. Action Items & Next Steps

- Document the `"XX"` state code and suppression handling in the README's data-quality section.
- Write a Dockerfile for the FastAPI app (pinned base image, non-root user) — next unchecked checklist item.
- Write a Dockerfile or entrypoint for the pipeline script.
- Test both containers together via `docker-compose`.
- Proceed to Day 2: cloud account/cluster setup, container registry, managed Postgres, and Kubernetes manifests (Secrets, Job, Deployment, Service, Ingress).
- Add automated tests (`tests/test_api.py`, and cleaning-logic tests) once pipeline logic is touched again.
- Decide on multi-year CSV handling (looping vs. one run per year) before adding a second year's data.