"""
ingestion/pipeline.py

Ingests raw Medicare provider CSV data: converts it to Parquet for efficient
storage, then aggregates totals by state and by procedure code and loads the
results into PostgreSQL.

CMS excludes any provider-service combination derived from 10 or fewer
beneficiaries from this file before publishing it, so every row here is
already unsuppressed data. There is no per-row suppression to detect or flag.

Loading is per year and idempotent. Running the same year twice replaces that
year's rows rather than duplicating them, and running a second year appends to
what is already there.

Every step is a typed function that accepts a DuckDB connection and a target
schema name. That lets the SQL logic run against an in-memory DuckDB database
in tests, with no PostgreSQL instance required.
"""

from __future__ import annotations

import logging
import os
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Final

import duckdb
from dotenv import load_dotenv

logger: Final[logging.Logger] = logging.getLogger(__name__)

MIN_CLAIM_YEAR: Final[int] = 1990
MAX_CLAIM_YEAR: Final[int] = 2100

PIPELINE_TABLES: Final[tuple[str, ...]] = (
    "cleaned_claims",
    "summary_by_state",
    "summary_by_hcpcs",
)

_IDENTIFIER_PATTERN: Final[re.Pattern[str]] = re.compile(
    r"^[A-Za-z_][A-Za-z0-9_]{0,62}$"
)
_YEAR_FOLDER_PATTERN: Final[re.Pattern[str]] = re.compile(r"^[0-9]{4}$")


# ---------------------------------------------------------------------------
# Safety helpers (A03: Injection)
#
# DuckDB accepts bind parameters for values, but not for schema names, table
# names, or COPY targets. Those positions are guarded here instead: identifiers
# pass through a strict allowlist, and file paths are escaped as SQL literals
# after being confirmed to sit under the configured data root.
# ---------------------------------------------------------------------------


def validate_identifier(name: str) -> str:
    """Return `name` if it is a plain SQL identifier, otherwise raise ValueError."""
    if _IDENTIFIER_PATTERN.match(name) is None:
        raise ValueError(f"Unsafe SQL identifier: {name!r}")
    return name


def quote_literal(value: str) -> str:
    """Escape `value` for use as a single-quoted SQL string literal."""
    return "'" + value.replace("'", "''") + "'"


def quote_dsn_value(value: str) -> str:
    """Escape `value` for use inside a libpq keyword/value connection string."""
    escaped = value.replace("\\", "\\\\").replace("'", "\\'")
    return f"'{escaped}'"


def ensure_within(base: Path, candidate: Path) -> Path:
    """Return the resolved `candidate` if it sits under `base`, else raise ValueError."""
    resolved_base = base.resolve()
    resolved_candidate = candidate.resolve()
    if (
        resolved_candidate != resolved_base
        and resolved_base not in resolved_candidate.parents
    ):
        raise ValueError(f"Path {candidate!s} escapes the data root {base!s}")
    return resolved_candidate


def claim_year_from_path(csv_path: Path) -> int:
    """Derive the claim year from the CSV's parent folder name, e.g. data/2024/file.csv."""
    folder_name = csv_path.parent.name
    if _YEAR_FOLDER_PATTERN.match(folder_name) is None:
        raise ValueError(
            f"Expected a four-digit year folder above {csv_path.name!r}, found {folder_name!r}"
        )
    return validate_claim_year(int(folder_name))


def validate_claim_year(claim_year: int) -> int:
    """Return `claim_year` if it falls in the supported range, otherwise raise ValueError."""
    if not MIN_CLAIM_YEAR <= claim_year <= MAX_CLAIM_YEAR:
        raise ValueError(
            f"Claim year {claim_year} is outside {MIN_CLAIM_YEAR}-{MAX_CLAIM_YEAR}"
        )
    return claim_year


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------


@dataclass(frozen=True, repr=False)
class PostgresSettings:
    """Connection settings for the target PostgreSQL database."""

    host: str
    port: str
    database: str
    user: str
    password: str

    @classmethod
    def from_env(cls) -> PostgresSettings:
        return cls(
            host=os.environ["PG_HOST"],
            port=os.environ["PG_PORT"],
            database=os.environ["PG_DB"],
            user=os.environ["PG_USER"],
            password=os.environ["PG_PASSWORD"],
        )

    def attach_dsn(self) -> str:
        """Build a libpq keyword/value connection string with each value escaped."""
        fields: list[tuple[str, str]] = [
            ("dbname", self.database),
            ("user", self.user),
            ("password", self.password),
            ("host", self.host),
            ("port", self.port),
        ]
        return " ".join(f"{key}={quote_dsn_value(value)}" for key, value in fields)

    def __repr__(self) -> str:
        """Redact the password so it never reaches logs or tracebacks (A09)."""
        return (
            f"PostgresSettings(host={self.host!r}, port={self.port!r}, "
            f"database={self.database!r}, user={self.user!r}, password='***')"
        )


@dataclass(frozen=True)
class PipelineConfig:
    """Filesystem paths and target schema for a single pipeline run."""

    data_root: Path
    csv_path: Path
    parquet_path: Path
    cleaned_parquet_path: Path
    schema: str = "pg"

    @classmethod
    def from_env(cls) -> PipelineConfig:
        data_root = Path(os.environ.get("DATA_ROOT", "data"))
        year_folder = os.environ.get("CLAIM_YEAR_DIR", "2024")
        csv_filename = os.environ["CSV_FILENAME"]

        csv_path = ensure_within(data_root, data_root / year_folder / csv_filename)
        year_directory = csv_path.parent
        return cls(
            data_root=data_root.resolve(),
            csv_path=csv_path,
            parquet_path=year_directory / "medicare_claims.parquet",
            cleaned_parquet_path=year_directory / "cleaned_medicare_claims.parquet",
            schema=validate_identifier(os.environ.get("PG_SCHEMA", "pg")),
        )


# ---------------------------------------------------------------------------
# SQL builders (pure functions, directly testable)
#
# The cleaned rows are read straight from Parquet. An intermediate raw_claims
# table in PostgreSQL would be a second full copy of the source data that
# nothing queries, costing roughly 3 GB and a full table write per year.
# ---------------------------------------------------------------------------


def _cleaned_select_sql(parquet_path: Path, claim_year: int) -> str:
    """The SELECT that shapes one year of Parquet rows into cleaned_claims form.

    CMS protects patient privacy by excluding any provider-service combination
    derived from 10 or fewer beneficiaries from the published file entirely,
    rather than including the row with blanked fields. There is no per-row
    suppression flag to compute: every row present already cleared that
    threshold before the file was published. Verified directly against the
    2024 source file (zero NULLs in Tot_Benes, Tot_Srvcs, or
    Tot_Bene_Day_Srvcs across 9.78M rows), matching CMS's own methodology
    documentation.
    """
    return f"""
        SELECT
            *,
            {claim_year} AS claim_year
        FROM read_parquet({quote_literal(str(parquet_path))})
    """


def build_create_cleaned_claims_sql(
    parquet_path: Path, claim_year: int, schema: str = "pg"
) -> str:
    """SQL creating an empty cleaned_claims table if it does not already exist.

    `WHERE FALSE` borrows the column types from the Parquet file without
    reading any rows, so the table shape always matches what will be inserted.
    """
    validate_identifier(schema)
    validate_claim_year(claim_year)
    return f"""
        CREATE TABLE IF NOT EXISTS {schema}.cleaned_claims AS
        {_cleaned_select_sql(parquet_path, claim_year)}
        WHERE FALSE
    """


def build_delete_claim_year_sql(claim_year: int, schema: str = "pg") -> str:
    """SQL removing one year's rows, so a repeated run replaces rather than duplicates."""
    validate_identifier(schema)
    validate_claim_year(claim_year)
    return f"DELETE FROM {schema}.cleaned_claims WHERE claim_year = {claim_year}"


def build_insert_cleaned_claims_sql(
    parquet_path: Path, claim_year: int, schema: str = "pg"
) -> str:
    """SQL appending one year of cleaned rows to cleaned_claims.

    `BY NAME` matches source columns to target columns by name rather than
    position. The table is created from the first year loaded and reused via
    IF NOT EXISTS, so a later year listing the same columns in a different
    order would otherwise load each value into the wrong column. That fails
    loudly only when the swapped columns differ in type; two VARCHAR columns
    swap silently and corrupt the summaries built on top of them.
    """
    validate_identifier(schema)
    validate_claim_year(claim_year)
    return f"""
        INSERT INTO {schema}.cleaned_claims BY NAME
        {_cleaned_select_sql(parquet_path, claim_year)}
    """


def build_summary_by_state_sql(schema: str = "pg") -> str:
    """SQL that aggregates cleaned_claims by year and rendering-provider state.

    Rebuilt in full from every loaded year, so the summaries stay consistent
    with cleaned_claims no matter which year was loaded last.

    No suppression FILTER: every row in cleaned_claims is real, unsuppressed
    data (see _cleaned_select_sql), so a plain SUM is correct. SUM still skips
    a NULL in an individual column on its own, standard SQL behavior, which is
    enough defensive coverage without a dedicated flag.

    State and code columns are cast to VARCHAR because read_csv_auto infers
    types from the data it sees. A year whose codes happened to be all digits
    would otherwise produce an integer column, which would not match the
    SQLAlchemy models the API queries through.
    """
    validate_identifier(schema)
    return f"""
        CREATE OR REPLACE TABLE {schema}.summary_by_state AS
        SELECT
            claim_year,
            CAST(Rndrng_Prvdr_State_Abrvtn AS VARCHAR) AS state,
            COUNT(*) AS row_count,
            SUM(Tot_Benes) AS total_beneficiaries,
            SUM(Tot_Srvcs) AS total_services,
            SUM(Avg_Mdcr_Pymt_Amt * Tot_Srvcs) AS total_medicare_payment
        FROM {schema}.cleaned_claims
        GROUP BY claim_year, Rndrng_Prvdr_State_Abrvtn
    """


def build_summary_by_hcpcs_sql(schema: str = "pg") -> str:
    """SQL that aggregates cleaned_claims by year and HCPCS procedure code.

    No suppression FILTER; see build_summary_by_state_sql for why.
    """
    validate_identifier(schema)
    return f"""
        CREATE OR REPLACE TABLE {schema}.summary_by_hcpcs AS
        SELECT
            claim_year,
            CAST(HCPCS_Cd AS VARCHAR) AS hcpcs_code,
            ANY_VALUE(CAST(HCPCS_Desc AS VARCHAR)) AS hcpcs_description,
            COUNT(*) AS row_count,
            SUM(Tot_Benes) AS total_beneficiaries,
            SUM(Tot_Srvcs) AS total_services,
            SUM(Avg_Mdcr_Pymt_Amt * Tot_Srvcs) AS total_medicare_payment
        FROM {schema}.cleaned_claims
        GROUP BY claim_year, CAST(HCPCS_Cd AS VARCHAR)
    """


# ---------------------------------------------------------------------------
# Pipeline steps
# ---------------------------------------------------------------------------


def attach_postgres(
    con: duckdb.DuckDBPyConnection,
    settings: PostgresSettings,
    alias: str = "pg",
) -> None:
    """Attach the PostgreSQL database to the DuckDB connection under `alias`."""
    validate_identifier(alias)
    con.execute("INSTALL postgres; LOAD postgres;")
    con.execute(
        f"ATTACH {quote_literal(settings.attach_dsn())} AS {alias} (TYPE POSTGRES)"
    )
    logger.info("Attached PostgreSQL as %s using %r", alias, settings)


def convert_csv_to_parquet(
    con: duckdb.DuckDBPyConnection,
    csv_path: Path,
    parquet_path: Path,
    *,
    overwrite: bool = False,
) -> bool:
    """Convert the source CSV to Parquet. Returns False when the conversion was skipped.

    A Parquet older than its CSV means the source was replaced since the last
    run, so it is rebuilt rather than reused. Without this check, replacing a
    CSV and re-running reloads the previous file's rows and logs a normal
    success.
    """
    if parquet_path.exists() and not overwrite:
        if parquet_path.stat().st_mtime >= csv_path.stat().st_mtime:
            logger.info("Parquet at %s is current, skipping conversion", parquet_path)
            return False
        logger.warning(
            "Parquet at %s predates %s, rebuilding it", parquet_path, csv_path
        )

    parquet_path.parent.mkdir(parents=True, exist_ok=True)
    con.execute(
        f"COPY (SELECT * FROM read_csv_auto({quote_literal(str(csv_path))})) "
        f"TO {quote_literal(str(parquet_path))} (FORMAT PARQUET)"
    )
    logger.info("Converted %s to %s", csv_path, parquet_path)
    return True


def load_cleaned_claims(
    con: duckdb.DuckDBPyConnection,
    parquet_path: Path,
    claim_year: int,
    schema: str = "pg",
) -> int:
    """Load one year into cleaned_claims, replacing any rows already held for it.

    Returns the number of rows now stored for that year.
    """
    con.execute(build_create_cleaned_claims_sql(parquet_path, claim_year, schema))
    con.execute(build_delete_claim_year_sql(claim_year, schema))
    con.execute(build_insert_cleaned_claims_sql(parquet_path, claim_year, schema))

    loaded = rows_for_claim_year(con, claim_year, schema)
    logger.info("Loaded %d rows for claim year %d", loaded, claim_year)
    return loaded


def create_summary_by_state(con: duckdb.DuckDBPyConnection, schema: str = "pg") -> None:
    """Build `<schema>.summary_by_state` from every year in cleaned_claims."""
    con.execute(build_summary_by_state_sql(schema))
    logger.info("Rebuilt %s.summary_by_state", schema)


def create_summary_by_hcpcs(con: duckdb.DuckDBPyConnection, schema: str = "pg") -> None:
    """Build `<schema>.summary_by_hcpcs` from every year in cleaned_claims."""
    con.execute(build_summary_by_hcpcs_sql(schema))
    logger.info("Rebuilt %s.summary_by_hcpcs", schema)


def export_cleaned_parquet(
    con: duckdb.DuckDBPyConnection,
    cleaned_parquet_path: Path,
    schema: str = "pg",
    *,
    claim_year: int | None = None,
    overwrite: bool = False,
) -> bool:
    """Write cleaned_claims to local Parquet, optionally limited to one year.

    Returns False when the file already exists and `overwrite` is not set.
    """
    validate_identifier(schema)
    if cleaned_parquet_path.exists() and not overwrite:
        logger.info(
            "Cleaned Parquet already present at %s, skipping", cleaned_parquet_path
        )
        return False

    where_clause = ""
    if claim_year is not None:
        validate_claim_year(claim_year)
        where_clause = f" WHERE claim_year = {claim_year}"

    cleaned_parquet_path.parent.mkdir(parents=True, exist_ok=True)
    con.execute(
        f"COPY (SELECT * FROM {schema}.cleaned_claims{where_clause}) "
        f"TO {quote_literal(str(cleaned_parquet_path))} (FORMAT PARQUET)"
    )
    logger.info("Exported cleaned claims to %s", cleaned_parquet_path)
    return True


def rows_for_claim_year(
    con: duckdb.DuckDBPyConnection, claim_year: int, schema: str = "pg"
) -> int:
    """Count the cleaned rows currently stored for one year."""
    validate_identifier(schema)
    validate_claim_year(claim_year)
    result: tuple[object, ...] | None = con.execute(
        f"SELECT COUNT(*) FROM {schema}.cleaned_claims WHERE claim_year = {claim_year}"
    ).fetchone()
    if result is None:
        raise RuntimeError(f"Count query returned no row for claim year {claim_year}")
    return int(str(result[0]))


def rows_by_claim_year(
    con: duckdb.DuckDBPyConnection, schema: str = "pg"
) -> dict[int, int]:
    """Return the cleaned row count for every year currently loaded."""
    validate_identifier(schema)
    rows = con.execute(
        f"SELECT claim_year, COUNT(*) FROM {schema}.cleaned_claims "
        "GROUP BY claim_year ORDER BY claim_year"
    ).fetchall()
    return {int(str(year)): int(str(count)) for year, count in rows}


def build_analyze_sql(table: str = "cleaned_claims") -> str:
    """The Postgres-native ANALYZE statement, run through postgres_execute.

    This runs directly against Postgres using Postgres's own object names, not
    the DuckDB catalog alias, so no schema/alias prefix belongs here.
    """
    validate_identifier(table)
    return f"ANALYZE {table}"


def analyze_cleaned_claims(con: duckdb.DuckDBPyConnection, alias: str = "pg") -> None:
    """Refresh Postgres's planner statistics for cleaned_claims after a bulk load.

    A multi-million-row INSERT leaves the query planner working from stale
    row-count and distribution estimates for that table. Without this, the
    next aggregation over cleaned_claims can pick a poor plan, and the gap
    widens as more years accumulate in the table. Requires a live attached
    Postgres catalog, via the postgres_execute passthrough function, so this
    is not exercised by the in-memory pipeline tests and is called only from
    main() after a real run.
    """
    validate_identifier(alias)
    con.execute(
        f"CALL postgres_execute({quote_literal(alias)}, {quote_literal(build_analyze_sql())})"
    )
    logger.info("Ran ANALYZE on %s.cleaned_claims", alias)


def table_row_counts(
    con: duckdb.DuckDBPyConnection,
    tables: tuple[str, ...] = PIPELINE_TABLES,
    schema: str = "pg",
) -> dict[str, int]:
    """Return a row count per table, used as a post-run sanity check."""
    validate_identifier(schema)
    counts: dict[str, int] = {}
    for table in tables:
        validate_identifier(table)
        result: tuple[object, ...] | None = con.execute(
            f"SELECT COUNT(*) FROM {schema}.{table}"
        ).fetchone()
        if result is None:
            raise RuntimeError(f"Count query returned no row for {schema}.{table}")
        counts[table] = int(str(result[0]))
    return counts


def run_pipeline(
    con: duckdb.DuckDBPyConnection, config: PipelineConfig
) -> dict[str, int]:
    """Load the configured year and rebuild the summaries. Returns table row counts.

    Counts cover every loaded year, not only the year just processed.

    Note: main() does not call this directly. It calls the same stage
    functions itself, in the same order, with analyze_cleaned_claims()
    inserted between the load and the summary rebuilds, which this function
    does not do. This function stays as written for the test suite's
    full-pipeline exercise, which runs against an in-memory DuckDB with no
    live Postgres attached, so analyze_cleaned_claims() (Postgres-only) was
    never callable from inside it anyway.
    """
    claim_year = claim_year_from_path(config.csv_path)

    convert_csv_to_parquet(con, config.csv_path, config.parquet_path)
    load_cleaned_claims(con, config.parquet_path, claim_year, config.schema)
    create_summary_by_state(con, config.schema)
    create_summary_by_hcpcs(con, config.schema)
    export_cleaned_parquet(
        con,
        config.cleaned_parquet_path,
        config.schema,
        claim_year=claim_year,
        overwrite=True,
    )

    return table_row_counts(con, PIPELINE_TABLES, config.schema)


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    load_dotenv()

    config = PipelineConfig.from_env()
    settings = PostgresSettings.from_env()
    claim_year = claim_year_from_path(config.csv_path)

    con: duckdb.DuckDBPyConnection = duckdb.connect()
    try:
        attach_postgres(con, settings, config.schema)
        convert_csv_to_parquet(con, config.csv_path, config.parquet_path)
        load_cleaned_claims(con, config.parquet_path, claim_year, config.schema)
        analyze_cleaned_claims(con, config.schema)
        create_summary_by_state(con, config.schema)
        create_summary_by_hcpcs(con, config.schema)
        export_cleaned_parquet(
            con,
            config.cleaned_parquet_path,
            config.schema,
            claim_year=claim_year,
            overwrite=True,
        )
        counts = table_row_counts(con, PIPELINE_TABLES, config.schema)
        per_year = rows_by_claim_year(con, config.schema)
    finally:
        con.close()

    for table, count in counts.items():
        logger.info("%s: %d rows", table, count)
    for year, count in per_year.items():
        logger.info("claim year %d: %d cleaned rows", year, count)


if __name__ == "__main__":
    main()
