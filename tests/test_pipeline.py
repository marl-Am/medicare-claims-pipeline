"""
tests/test_pipeline.py

Exercises the pipeline's cleaning and aggregation logic against an in-memory
DuckDB database. A schema named `pg` stands in for the attached PostgreSQL
catalog, so the same SQL that runs in production runs here with no database
container and no multi-gigabyte CSV.
"""

from __future__ import annotations

from collections.abc import Iterator
from pathlib import Path
from typing import Any

import duckdb
import pytest

from ingestion.pipeline import (
    MAX_CLAIM_YEAR,
    PipelineConfig,
    PostgresSettings,
    build_analyze_sql,
    build_delete_claim_year_sql,
    build_insert_cleaned_claims_sql,
    build_summary_by_hcpcs_sql,
    build_summary_by_state_sql,
    claim_year_from_path,
    convert_csv_to_parquet,
    create_summary_by_hcpcs,
    create_summary_by_state,
    ensure_within,
    export_cleaned_parquet,
    load_cleaned_claims,
    quote_dsn_value,
    quote_literal,
    rows_by_claim_year,
    rows_for_claim_year,
    run_pipeline,
    table_row_counts,
    validate_identifier,
)

TEST_SCHEMA: str = "pg"
TEST_YEAR: int = 2024
PRIOR_YEAR: int = 2023

CSV_HEADER: str = (
    "Rndrng_NPI,Rndrng_Prvdr_State_Abrvtn,HCPCS_Cd,HCPCS_Desc,"
    "Tot_Benes,Tot_Srvcs,Tot_Bene_Day_Srvcs,Avg_Mdcr_Pymt_Amt"
)

# Every row here is complete. CMS excludes low-volume provider-service
# combinations from this file before publishing it (verified directly against
# the real 2024 CSV: zero NULLs across Tot_Benes, Tot_Srvcs, and
# Tot_Bene_Day_Srvcs in 9.78M rows), so a realistic sample has no suppression
# to model. NPI 4 carries the "XX" placeholder state, which must survive
# cleaning unchanged.
RAW_ROWS: list[tuple[Any, ...]] = [
    (1, "CA", "99213", "Office visit", 30, 100.0, 90, 50.0),
    (2, "CA", "99213", "Office visit", 20, 50.0, 45, 40.0),
    (3, "TX", "99214", "Longer visit", 12, 45.0, 40, 60.0),
    (4, "XX", "99213", "Office visit", 10, 25.0, 20, 20.0),
    (5, "TX", "99214", "Longer visit", 15, 40.0, 35, 30.0),
    (6, "TX", "99214", "Longer visit", 99, 200.0, 180, 10.0),
]

# A smaller prior year, used to prove that loading a second year appends.
PRIOR_YEAR_ROWS: list[tuple[Any, ...]] = [
    (7, "CA", "99213", "Office visit", 12, 60.0, 55, 35.0),
    (8, "NY", "99215", "Complex visit", 8, 30.0, 25, 70.0),
]

# A single row with an incomplete field, used only to prove SUM's ordinary
# NULL-skipping still works without a dedicated suppression flag. This
# scenario is not expected in real CMS data, since a row that exists in the
# file has already cleared their disclosure threshold, but the aggregation
# should not break if some other data-quality issue ever produced a gap.
INCOMPLETE_FIELD_ROWS: list[tuple[Any, ...]] = [
    (9, "CA", "99213", "Office visit", 30, 100.0, 90, 50.0),
    (10, "CA", "99213", "Office visit", None, 50.0, 45, 40.0),
]


def _csv_cell(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, str):
        return f'"{value}"'
    return str(value)


def write_year_csv(root: Path, year: int, rows: list[tuple[Any, ...]]) -> Path:
    """Write a CSV into <root>/<year>/, matching the real folder layout."""
    year_directory = root / str(year)
    year_directory.mkdir(parents=True, exist_ok=True)
    csv_path = year_directory / "claims.csv"
    lines = [CSV_HEADER]
    lines.extend(",".join(_csv_cell(cell) for cell in row) for row in rows)
    csv_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return csv_path


def config_for(root: Path, csv_path: Path) -> PipelineConfig:
    year_directory = csv_path.parent
    return PipelineConfig(
        data_root=root,
        csv_path=csv_path,
        parquet_path=year_directory / "medicare_claims.parquet",
        cleaned_parquet_path=year_directory / "cleaned_medicare_claims.parquet",
        schema=TEST_SCHEMA,
    )


@pytest.fixture()
def con() -> Iterator[duckdb.DuckDBPyConnection]:
    """An in-memory DuckDB connection with an empty `pg` schema."""
    connection = duckdb.connect(":memory:")
    try:
        connection.execute(f"CREATE SCHEMA {TEST_SCHEMA}")
        yield connection
    finally:
        connection.close()


@pytest.fixture()
def sample_csv(tmp_path: Path) -> Path:
    return write_year_csv(tmp_path / "data", TEST_YEAR, RAW_ROWS)


@pytest.fixture()
def sample_parquet(
    con: duckdb.DuckDBPyConnection, sample_csv: Path, tmp_path: Path
) -> Path:
    parquet_path = tmp_path / "claims.parquet"
    convert_csv_to_parquet(con, sample_csv, parquet_path)
    return parquet_path


@pytest.fixture()
def loaded_con(
    con: duckdb.DuckDBPyConnection, sample_parquet: Path
) -> duckdb.DuckDBPyConnection:
    """A connection with one year of cleaned rows loaded."""
    load_cleaned_claims(con, sample_parquet, TEST_YEAR, TEST_SCHEMA)
    return con


@pytest.fixture()
def aggregated_con(loaded_con: duckdb.DuckDBPyConnection) -> duckdb.DuckDBPyConnection:
    create_summary_by_state(loaded_con, TEST_SCHEMA)
    create_summary_by_hcpcs(loaded_con, TEST_SCHEMA)
    return loaded_con


def fetch_all(con: duckdb.DuckDBPyConnection, sql: str) -> list[tuple[Any, ...]]:
    return con.execute(sql).fetchall()


def fetch_one(con: duckdb.DuckDBPyConnection, sql: str) -> tuple[Any, ...]:
    row = con.execute(sql).fetchone()
    assert row is not None, f"Expected a row from: {sql}"
    return row


# ---------------------------------------------------------------------------
# Year derivation
# ---------------------------------------------------------------------------


def test_claim_year_comes_from_parent_folder() -> None:
    assert claim_year_from_path(Path("data") / "2024" / "claims.csv") == 2024


@pytest.mark.parametrize("folder", ["twenty24", "24", "2024a", ""])
def test_claim_year_rejects_non_year_folders(folder: str) -> None:
    with pytest.raises(ValueError):
        claim_year_from_path(Path("data") / folder / "claims.csv")


def test_claim_year_rejects_out_of_range_folder() -> None:
    with pytest.raises(ValueError):
        claim_year_from_path(Path("data") / "1899" / "claims.csv")


# ---------------------------------------------------------------------------
# Injection and path guards
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "identifier",
    ["pg; DROP TABLE cleaned_claims", "pg.public", "1_schema", "", "pg-schema", "pg'"],
)
def test_validate_identifier_rejects_unsafe_names(identifier: str) -> None:
    with pytest.raises(ValueError):
        validate_identifier(identifier)


def test_validate_identifier_accepts_plain_names() -> None:
    assert validate_identifier("summary_by_state") == "summary_by_state"


def test_quote_literal_doubles_single_quotes() -> None:
    assert quote_literal("O'Brien") == "'O''Brien'"


def test_quote_dsn_value_escapes_quotes_and_backslashes() -> None:
    assert quote_dsn_value(r"pa's\word") == r"'pa\'s\\word'"


def test_attach_dsn_escapes_the_password() -> None:
    settings = PostgresSettings(
        host="localhost",
        port="5432",
        database="medicare",
        user="app",
        password="p a's",
    )
    assert r"password='p a\'s'" in settings.attach_dsn()


def test_settings_repr_hides_password() -> None:
    settings = PostgresSettings("localhost", "5432", "medicare", "app", "hunter2")
    assert "hunter2" not in repr(settings)


def test_sql_builders_reject_unsafe_schema() -> None:
    unsafe = "pg; DROP TABLE users"
    with pytest.raises(ValueError):
        build_delete_claim_year_sql(TEST_YEAR, unsafe)
    with pytest.raises(ValueError):
        build_insert_cleaned_claims_sql(Path("x.parquet"), TEST_YEAR, unsafe)
    with pytest.raises(ValueError):
        build_summary_by_state_sql(unsafe)
    with pytest.raises(ValueError):
        build_summary_by_hcpcs_sql(unsafe)


def test_sql_builders_reject_out_of_range_year() -> None:
    with pytest.raises(ValueError):
        build_delete_claim_year_sql(99999, TEST_SCHEMA)


def test_ensure_within_rejects_traversal(tmp_path: Path) -> None:
    base = tmp_path / "data"
    base.mkdir()
    with pytest.raises(ValueError):
        ensure_within(base, base / ".." / "secrets.env")


def test_ensure_within_accepts_nested_path(tmp_path: Path) -> None:
    base = tmp_path / "data"
    base.mkdir()
    assert ensure_within(base, base / "2024" / "claims.csv") == (
        base / "2024" / "claims.csv"
    ).resolve()


# ---------------------------------------------------------------------------
# Cleaning logic
# ---------------------------------------------------------------------------


def test_cleaning_keeps_every_source_row(loaded_con: duckdb.DuckDBPyConnection) -> None:
    count = fetch_one(loaded_con, f"SELECT COUNT(*) FROM {TEST_SCHEMA}.cleaned_claims")
    assert count[0] == len(RAW_ROWS)


def test_cleaning_stamps_claim_year(loaded_con: duckdb.DuckDBPyConnection) -> None:
    years = fetch_all(
        loaded_con, f"SELECT DISTINCT claim_year FROM {TEST_SCHEMA}.cleaned_claims"
    )
    assert years == [(TEST_YEAR,)]


def test_cleaning_keeps_xx_placeholder_state(
    loaded_con: duckdb.DuckDBPyConnection,
) -> None:
    count = fetch_one(
        loaded_con,
        f"SELECT COUNT(*) FROM {TEST_SCHEMA}.cleaned_claims "
        "WHERE Rndrng_Prvdr_State_Abrvtn = 'XX'",
    )
    assert count[0] == 1


def test_cleaned_claims_has_no_suppression_column(
    loaded_con: duckdb.DuckDBPyConnection,
) -> None:
    """CMS excludes low-volume rows from the file before publishing it, so
    there is nothing for a per-row suppression flag to detect. Guards against
    the column quietly reappearing in a future edit."""
    described = fetch_all(loaded_con, f"DESCRIBE {TEST_SCHEMA}.cleaned_claims")
    columns = {str(row[0]) for row in described}
    assert "is_suppressed" not in columns


# ---------------------------------------------------------------------------
# Multi-year loading
# ---------------------------------------------------------------------------


def test_second_year_appends_rather_than_replacing(
    con: duckdb.DuckDBPyConnection, tmp_path: Path
) -> None:
    root = tmp_path / "data"
    current = write_year_csv(root, TEST_YEAR, RAW_ROWS)
    prior = write_year_csv(root, PRIOR_YEAR, PRIOR_YEAR_ROWS)

    run_pipeline(con, config_for(root, current))
    run_pipeline(con, config_for(root, prior))

    assert rows_by_claim_year(con, TEST_SCHEMA) == {
        PRIOR_YEAR: len(PRIOR_YEAR_ROWS),
        TEST_YEAR: len(RAW_ROWS),
    }


def test_reloading_a_year_replaces_rather_than_duplicates(
    con: duckdb.DuckDBPyConnection, tmp_path: Path
) -> None:
    """A re-run has to be safe: containers get retried, and a duplicated year
    would silently double every total in the summaries."""
    root = tmp_path / "data"
    current = write_year_csv(root, TEST_YEAR, RAW_ROWS)
    config = config_for(root, current)

    run_pipeline(con, config)
    run_pipeline(con, config)
    run_pipeline(con, config)

    assert rows_for_claim_year(con, TEST_YEAR, TEST_SCHEMA) == len(RAW_ROWS)


def test_summaries_rebuild_across_every_loaded_year(
    con: duckdb.DuckDBPyConnection, tmp_path: Path
) -> None:
    """The summary tables are rebuilt in full, so loading a second year must not
    drop the first year's aggregates."""
    root = tmp_path / "data"
    current = write_year_csv(root, TEST_YEAR, RAW_ROWS)
    prior = write_year_csv(root, PRIOR_YEAR, PRIOR_YEAR_ROWS)

    run_pipeline(con, config_for(root, current))
    run_pipeline(con, config_for(root, prior))

    years = fetch_all(
        con,
        f"SELECT DISTINCT claim_year FROM {TEST_SCHEMA}.summary_by_state "
        "ORDER BY claim_year",
    )
    assert years == [(PRIOR_YEAR,), (TEST_YEAR,)]


def test_same_state_in_two_years_stays_separate(
    con: duckdb.DuckDBPyConnection, tmp_path: Path
) -> None:
    """California appears in both years and must produce two summary rows, not
    one merged row."""
    root = tmp_path / "data"
    current = write_year_csv(root, TEST_YEAR, RAW_ROWS)
    prior = write_year_csv(root, PRIOR_YEAR, PRIOR_YEAR_ROWS)

    run_pipeline(con, config_for(root, current))
    run_pipeline(con, config_for(root, prior))

    rows = fetch_all(
        con,
        f"SELECT claim_year, row_count FROM {TEST_SCHEMA}.summary_by_state "
        "WHERE state = 'CA' ORDER BY claim_year",
    )
    assert rows == [(PRIOR_YEAR, 1), (TEST_YEAR, 2)]


def test_loader_reports_the_row_count_it_stored(
    con: duckdb.DuckDBPyConnection, sample_parquet: Path
) -> None:
    stored = load_cleaned_claims(con, sample_parquet, TEST_YEAR, TEST_SCHEMA)
    assert stored == len(RAW_ROWS)


# ---------------------------------------------------------------------------
# Aggregation by state
# ---------------------------------------------------------------------------


def test_state_summary_has_one_row_per_state(
    aggregated_con: duckdb.DuckDBPyConnection,
) -> None:
    states = fetch_all(
        aggregated_con, f"SELECT state FROM {TEST_SCHEMA}.summary_by_state ORDER BY state"
    )
    assert states == [("CA",), ("TX",), ("XX",)]


def test_state_summary_row_count_matches_group_size(
    aggregated_con: duckdb.DuckDBPyConnection,
) -> None:
    row = fetch_one(
        aggregated_con,
        f"SELECT row_count FROM {TEST_SCHEMA}.summary_by_state WHERE state = 'TX'",
    )
    assert row[0] == 3


def test_state_summary_sums_every_row_in_the_group(
    aggregated_con: duckdb.DuckDBPyConnection,
) -> None:
    row = fetch_one(
        aggregated_con,
        f"SELECT total_beneficiaries, total_services FROM {TEST_SCHEMA}.summary_by_state "
        "WHERE state = 'TX'",
    )
    assert int(row[0]) == 12 + 15 + 99
    assert float(row[1]) == pytest.approx(45.0 + 40.0 + 200.0)


def test_state_summary_payment_multiplies_average_by_services(
    aggregated_con: duckdb.DuckDBPyConnection,
) -> None:
    row = fetch_one(
        aggregated_con,
        f"SELECT total_medicare_payment FROM {TEST_SCHEMA}.summary_by_state "
        "WHERE state = 'CA'",
    )
    assert float(row[0]) == pytest.approx(100.0 * 50.0 + 50.0 * 40.0)


def test_state_summary_totals_the_xx_rows_normally(
    aggregated_con: duckdb.DuckDBPyConnection,
) -> None:
    row = fetch_one(
        aggregated_con,
        f"SELECT total_medicare_payment FROM {TEST_SCHEMA}.summary_by_state "
        "WHERE state = 'XX'",
    )
    assert float(row[0]) == pytest.approx(500.0)


def test_a_null_in_one_column_does_not_break_aggregation(
    con: duckdb.DuckDBPyConnection, tmp_path: Path
) -> None:
    """Not a suppression scenario (see module docstring); this only confirms
    SUM's ordinary NULL-skipping still holds without a dedicated flag."""
    root = tmp_path / "data"
    csv_path = write_year_csv(root, TEST_YEAR, INCOMPLETE_FIELD_ROWS)
    run_pipeline(con, config_for(root, csv_path))

    row = fetch_one(
        con,
        f"SELECT row_count, total_beneficiaries FROM {TEST_SCHEMA}.summary_by_state "
        "WHERE state = 'CA'",
    )
    assert row[0] == 2
    assert int(row[1]) == 30


# ---------------------------------------------------------------------------
# Aggregation by HCPCS code
# ---------------------------------------------------------------------------


def test_hcpcs_summary_groups_by_code(aggregated_con: duckdb.DuckDBPyConnection) -> None:
    codes = fetch_all(
        aggregated_con,
        f"SELECT hcpcs_code FROM {TEST_SCHEMA}.summary_by_hcpcs ORDER BY hcpcs_code",
    )
    assert codes == [("99213",), ("99214",)]


def test_hcpcs_summary_totals_across_states(
    aggregated_con: duckdb.DuckDBPyConnection,
) -> None:
    row = fetch_one(
        aggregated_con,
        "SELECT row_count, total_beneficiaries, total_services, total_medicare_payment "
        f"FROM {TEST_SCHEMA}.summary_by_hcpcs WHERE hcpcs_code = '99213'",
    )
    assert row[0] == 3
    assert int(row[1]) == 60
    assert float(row[2]) == pytest.approx(175.0)
    assert float(row[3]) == pytest.approx(7500.0)


def test_hcpcs_summary_carries_the_description(
    aggregated_con: duckdb.DuckDBPyConnection,
) -> None:
    row = fetch_one(
        aggregated_con,
        f"SELECT hcpcs_description FROM {TEST_SCHEMA}.summary_by_hcpcs "
        "WHERE hcpcs_code = '99214'",
    )
    assert row[0] == "Longer visit"


def test_hcpcs_code_yields_one_row_per_year(
    con: duckdb.DuckDBPyConnection, tmp_path: Path
) -> None:
    """Grouping is on code alone, not code plus description, so a reworded CMS
    description in a later year cannot split one code into two summary rows."""
    root = tmp_path / "data"
    current = write_year_csv(root, TEST_YEAR, RAW_ROWS)
    reworded = [(9, "CA", "99213", "Office visit, established", 5, 10.0, 8, 25.0)]
    prior = write_year_csv(root, PRIOR_YEAR, reworded)

    run_pipeline(con, config_for(root, current))
    run_pipeline(con, config_for(root, prior))

    rows = fetch_all(
        con,
        f"SELECT claim_year FROM {TEST_SCHEMA}.summary_by_hcpcs "
        "WHERE hcpcs_code = '99213' ORDER BY claim_year",
    )
    assert rows == [(PRIOR_YEAR,), (TEST_YEAR,)]


def test_summary_key_columns_are_text_regardless_of_inference(
    aggregated_con: duckdb.DuckDBPyConnection,
) -> None:
    """The sample file's codes are all digits, so read_csv_auto infers an integer
    column. The summary tables must still expose text, since the SQLAlchemy
    models and Pydantic schemas the API uses declare `str`."""
    described = fetch_all(
        aggregated_con, f"DESCRIBE {TEST_SCHEMA}.summary_by_hcpcs"
    )
    column_types = {str(row[0]): str(row[1]) for row in described}
    assert column_types["hcpcs_code"] == "VARCHAR"

    described_states = fetch_all(
        aggregated_con, f"DESCRIBE {TEST_SCHEMA}.summary_by_state"
    )
    state_types = {str(row[0]): str(row[1]) for row in described_states}
    assert state_types["state"] == "VARCHAR"


def test_letter_prefixed_hcpcs_codes_survive(
    con: duckdb.DuckDBPyConnection, tmp_path: Path
) -> None:
    """HCPCS Level II codes carry a letter prefix. They must group and return
    unchanged rather than being coerced or dropped."""
    root = tmp_path / "data"
    rows: list[tuple[Any, ...]] = [
        (10, "CA", "J1885", "Ketorolac injection", 12, 40.0, 35, 5.0),
        (11, "CA", "G0008", "Flu shot administration", 20, 60.0, 55, 8.0),
    ]
    csv_path = write_year_csv(root, TEST_YEAR, rows)
    run_pipeline(con, config_for(root, csv_path))

    codes = fetch_all(
        con,
        f"SELECT hcpcs_code FROM {TEST_SCHEMA}.summary_by_hcpcs ORDER BY hcpcs_code",
    )
    assert codes == [("G0008",), ("J1885",)]


# ---------------------------------------------------------------------------
# File conversion steps
# ---------------------------------------------------------------------------


def test_csv_converts_to_parquet(
    con: duckdb.DuckDBPyConnection, sample_csv: Path, tmp_path: Path
) -> None:
    parquet_path = tmp_path / "out" / "claims.parquet"
    assert convert_csv_to_parquet(con, sample_csv, parquet_path) is True
    assert parquet_path.exists()

    count = fetch_one(con, f"SELECT COUNT(*) FROM read_parquet('{parquet_path.as_posix()}')")
    assert count[0] == len(RAW_ROWS)


def test_conversion_skips_an_existing_parquet(
    con: duckdb.DuckDBPyConnection, sample_csv: Path, tmp_path: Path
) -> None:
    parquet_path = tmp_path / "claims.parquet"
    parquet_path.write_bytes(b"placeholder")
    assert convert_csv_to_parquet(con, sample_csv, parquet_path) is False
    assert parquet_path.read_bytes() == b"placeholder"


def test_conversion_overwrites_when_asked(
    con: duckdb.DuckDBPyConnection, sample_csv: Path, tmp_path: Path
) -> None:
    parquet_path = tmp_path / "claims.parquet"
    parquet_path.write_bytes(b"placeholder")
    assert convert_csv_to_parquet(con, sample_csv, parquet_path, overwrite=True) is True
    assert parquet_path.read_bytes() != b"placeholder"


def test_cleaned_export_writes_a_readable_parquet(
    loaded_con: duckdb.DuckDBPyConnection, tmp_path: Path
) -> None:
    export_path = tmp_path / "cleaned.parquet"

    assert export_cleaned_parquet(loaded_con, export_path, TEST_SCHEMA) is True
    count = fetch_one(
        loaded_con, f"SELECT COUNT(*) FROM read_parquet('{export_path.as_posix()}')"
    )
    assert count[0] == len(RAW_ROWS)


def test_cleaned_export_can_limit_to_one_year(
    con: duckdb.DuckDBPyConnection, tmp_path: Path
) -> None:
    root = tmp_path / "data"
    current = write_year_csv(root, TEST_YEAR, RAW_ROWS)
    prior = write_year_csv(root, PRIOR_YEAR, PRIOR_YEAR_ROWS)
    run_pipeline(con, config_for(root, current))
    run_pipeline(con, config_for(root, prior))

    export_path = tmp_path / "one_year.parquet"
    export_cleaned_parquet(con, export_path, TEST_SCHEMA, claim_year=PRIOR_YEAR)

    count = fetch_one(
        con, f"SELECT COUNT(*) FROM read_parquet('{export_path.as_posix()}')"
    )
    assert count[0] == len(PRIOR_YEAR_ROWS)


def test_cleaned_export_skips_an_existing_file(
    loaded_con: duckdb.DuckDBPyConnection, tmp_path: Path
) -> None:
    export_path = tmp_path / "cleaned.parquet"
    export_path.write_bytes(b"placeholder")

    assert export_cleaned_parquet(loaded_con, export_path, TEST_SCHEMA) is False


# ---------------------------------------------------------------------------
# Row counts and full run
# ---------------------------------------------------------------------------


def test_row_counts_reject_an_unsafe_table_name(
    loaded_con: duckdb.DuckDBPyConnection,
) -> None:
    with pytest.raises(ValueError):
        table_row_counts(loaded_con, ("cleaned_claims; DROP TABLE x",), TEST_SCHEMA)


def test_full_run_produces_expected_row_counts(
    con: duckdb.DuckDBPyConnection, tmp_path: Path
) -> None:
    root = tmp_path / "data"
    csv_path = write_year_csv(root, TEST_YEAR, RAW_ROWS)
    config = config_for(root, csv_path)

    counts = run_pipeline(con, config)

    assert counts == {
        "cleaned_claims": 6,
        "summary_by_state": 3,
        "summary_by_hcpcs": 2,
    }
    assert config.parquet_path.exists()
    assert config.cleaned_parquet_path.exists()


def test_full_run_rejects_a_year_folder_it_cannot_parse(
    con: duckdb.DuckDBPyConnection, tmp_path: Path
) -> None:
    root = tmp_path / "data"
    bad_directory = root / "latest"
    bad_directory.mkdir(parents=True)
    csv_path = bad_directory / "claims.csv"
    csv_path.write_text(CSV_HEADER + "\n", encoding="utf-8")

    with pytest.raises(ValueError):
        run_pipeline(con, config_for(root, csv_path))


def test_max_claim_year_is_a_sane_upper_bound() -> None:
    assert MAX_CLAIM_YEAR > TEST_YEAR


# ---------------------------------------------------------------------------
# ANALYZE statement
# ---------------------------------------------------------------------------


def test_analyze_sql_targets_cleaned_claims_unqualified() -> None:
    """No catalog alias or schema prefix: this runs through postgres_execute
    directly against Postgres, using Postgres's own table name."""
    assert build_analyze_sql() == "ANALYZE cleaned_claims"


def test_analyze_sql_rejects_an_unsafe_table_name() -> None:
    with pytest.raises(ValueError):
        build_analyze_sql("cleaned_claims; DROP TABLE users")