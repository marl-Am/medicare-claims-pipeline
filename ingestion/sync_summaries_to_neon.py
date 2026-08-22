"""
ingestion/sync_summaries_to_neon.py

Copies summary_by_state and summary_by_hcpcs from the local Postgres
instance to a remote target (Neon), leaving cleaned_claims untouched on
both sides. cleaned_claims never needs to leave this machine; only the
two small, already-correct summary tables need to reach the deployed API.

Run this after a local pipeline run whose results should be reflected in
the deployed API. Reads local connection details from PG_HOST/PG_PORT/
PG_DB/PG_USER/PG_PASSWORD, the same variables pipeline.py itself uses,
and remote connection details from a separate set, NEON_HOST/NEON_PORT/
NEON_DB/NEON_USER/NEON_PASSWORD, so the two never collide in one .env.
"""

from __future__ import annotations

import logging
import os

import duckdb
from dotenv import load_dotenv

from ingestion.pipeline import PostgresSettings, attach_postgres, validate_identifier

logger = logging.getLogger(__name__)

# Deliberately narrower than pipeline.py's PIPELINE_TABLES, which also
# includes cleaned_claims. That table is not part of this sync.
SUMMARY_TABLES: tuple[str, ...] = ("summary_by_state", "summary_by_hcpcs")


def neon_settings_from_env() -> PostgresSettings:
    """Build Neon's connection settings from NEON_* variables, SSL required."""
    return PostgresSettings(
        host=os.environ["NEON_HOST"],
        port=os.environ["NEON_PORT"],
        database=os.environ["NEON_DB"],
        user=os.environ["NEON_USER"],
        password=os.environ["NEON_PASSWORD"],
        sslmode="require",
    )


def sync_summary_tables(
    con: duckdb.DuckDBPyConnection,
    local_alias: str = "pg",
    remote_alias: str = "neon",
) -> None:
    """Copy each summary table from local_alias to remote_alias, as-is."""
    validate_identifier(local_alias)
    validate_identifier(remote_alias)
    for table in SUMMARY_TABLES:
        validate_identifier(table)
        con.execute(
            f"CREATE OR REPLACE TABLE {remote_alias}.{table} AS "
            f"SELECT * FROM {local_alias}.{table}"
        )
        logger.info("Synced %s to %s", table, remote_alias)


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    load_dotenv()

    local_settings = PostgresSettings.from_env()
    remote_settings = neon_settings_from_env()

    con: duckdb.DuckDBPyConnection = duckdb.connect()
    try:
        attach_postgres(con, local_settings, alias="pg")
        attach_postgres(con, remote_settings, alias="neon")
        sync_summary_tables(con, local_alias="pg", remote_alias="neon")
    finally:
        con.close()


if __name__ == "__main__":
    main()
