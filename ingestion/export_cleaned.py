"""
ingestion/export_cleaned.py

Exports the already-cleaned rows from PostgreSQL to a local Parquet file
without re-running the pipeline. Useful after a load, or when the Parquet has
been deleted but the database still holds the data.

Usage, from the project root with the venv active:

    python -m ingestion.export_cleaned              # every loaded year
    python -m ingestion.export_cleaned 2023         # one year only

The output path comes from the same .env settings the pipeline uses, so a
year-limited export lands in that year's data folder.
"""

from __future__ import annotations

import logging
import sys
from pathlib import Path
from typing import Final

import duckdb
from dotenv import load_dotenv

from ingestion.pipeline import (
    PipelineConfig,
    PostgresSettings,
    attach_postgres,
    export_cleaned_parquet,
    rows_by_claim_year,
    validate_claim_year,
)

logger: Final[logging.Logger] = logging.getLogger(__name__)


def resolve_output_path(config: PipelineConfig, claim_year: int | None) -> Path:
    """Pick where the export lands: the year's folder, or the data root for all years."""
    if claim_year is None:
        return config.data_root / "cleaned_medicare_claims.parquet"
    return config.data_root / str(claim_year) / "cleaned_medicare_claims.parquet"


def parse_year_argument(argv: list[str]) -> int | None:
    """Read an optional four-digit year from the command line."""
    if not argv:
        return None
    if len(argv) > 1:
        raise ValueError("Pass at most one claim year")
    try:
        requested = int(argv[0])
    except ValueError as error:
        raise ValueError(f"Not a year: {argv[0]!r}") from error
    return validate_claim_year(requested)


def main(argv: list[str]) -> int:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    load_dotenv()

    try:
        claim_year = parse_year_argument(argv)
        config = PipelineConfig.from_env()
        settings = PostgresSettings.from_env()
    except (ValueError, KeyError) as error:
        logger.error("Configuration problem: %s", error)
        return 1

    output_path = resolve_output_path(config, claim_year)

    con: duckdb.DuckDBPyConnection = duckdb.connect()
    try:
        attach_postgres(con, settings, config.schema)
        available = rows_by_claim_year(con, config.schema)

        if claim_year is not None and claim_year not in available:
            logger.error(
                "No rows loaded for %d. Available years: %s",
                claim_year,
                ", ".join(str(year) for year in available) or "none",
            )
            return 1

        written = export_cleaned_parquet(
            con, output_path, config.schema, claim_year=claim_year, overwrite=True
        )
    finally:
        con.close()

    if written:
        logger.info("Wrote %s", output_path)
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))