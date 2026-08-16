"""
pipeline.py

Ingests raw Medicare provider CSV data: converts it to Parquet for efficient
storage, cleans and flags suppressed rows, aggregates totals by state and by
procedure code, then loads raw + cleaned + aggregated results into PostgreSQL.
"""
from typing import Any

import os
from pathlib import Path

import duckdb
from dotenv import load_dotenv

load_dotenv()

pg_host: str = os.environ["PG_HOST"]
pg_port: str = os.environ["PG_PORT"]
pg_db: str = os.environ["PG_DB"]
pg_user: str = os.environ["PG_USER"]
pg_password: str = os.environ["PG_PASSWORD"]

con: duckdb.DuckDBPyConnection = duckdb.connect()

csv_path: str = (
    r"C:\Users\marlo\Documents\Github\medicare-claims-pipeline\data\2024\PHY_R26_P05_V10_D24_Prov_Svc.csv"
)
parquet_path: str = (
    r"C:\Users\marlo\Documents\Github\medicare-claims-pipeline\data\2024\medicare_claims.parquet"
)
cleaned_parquet_path: str = (
    r"C:\Users\marlo\Documents\Github\medicare-claims-pipeline\data\2024\cleaned_medicare_claims.parquet"
)

# Year is derived from the parent folder name (e.g. "data\2024\...").
# This assumes each year's CSV lives in a folder named after that year.
data_year: int = int(Path(csv_path).parent.name)

# CSV -> Parquet (skip if already converted)
if not Path(parquet_path).exists():
    print("---------- CSV -> Parquet Starting ----------\n")
    con.execute(
        f"COPY (SELECT * FROM read_csv_auto('{csv_path}')) TO '{parquet_path}' (FORMAT PARQUET)"
    )
    print("---------- CSV -> Parquet Finished ----------\n")
else:
    print("---------- CSV -> Parquet Skipped (already exists) ----------\n")

# Parquet -> PostgreSQL (raw load, unchanged)
print("---------- Parquet -> PostgreSQL (raw) Starting ----------\n")
con.execute("INSTALL postgres; LOAD postgres;")
con.execute(
    f"ATTACH 'dbname={pg_db} user={pg_user} password={pg_password} host={pg_host} port={pg_port}' "
    "AS pg (TYPE POSTGRES)"
)
con.execute(
    f"CREATE OR REPLACE TABLE pg.raw_claims AS SELECT * FROM read_parquet('{parquet_path}')"
)
print("---------- Parquet -> PostgreSQL (raw) Finished ----------\n")

# Clean: flag suppressed rows instead of dropping them.
# CMS redacts Tot_Benes (and related counts) when the value is small enough
# to risk identifying an individual patient — those rows come through as NULL.
print("---------- Cleaning Starting ----------\n")
con.execute(f"""
    CREATE OR REPLACE TABLE pg.cleaned_claims AS
    SELECT
        *,
        {data_year} AS claim_year,
        (Tot_Benes IS NULL OR Tot_Srvcs IS NULL OR Tot_Bene_Day_Srvcs IS NULL) AS is_suppressed
    FROM pg.raw_claims
    """)
print("---------- Cleaning Finished ----------\n")

# Aggregate: totals by state, excluding suppressed rows from sums
# (suppressed row counts are reported separately for data-quality visibility).
print("---------- Aggregation Starting ----------\n")
con.execute("""
    CREATE OR REPLACE TABLE pg.summary_by_state AS
    SELECT
        claim_year,
        Rndrng_Prvdr_State_Abrvtn AS state,
        COUNT(*) FILTER (WHERE NOT is_suppressed) AS row_count,
        COUNT(*) FILTER (WHERE is_suppressed) AS suppressed_count,
        SUM(Tot_Benes) FILTER (WHERE NOT is_suppressed) AS total_beneficiaries,
        SUM(Tot_Srvcs) FILTER (WHERE NOT is_suppressed) AS total_services,
        SUM(Avg_Mdcr_Pymt_Amt * Tot_Srvcs) FILTER (WHERE NOT is_suppressed) AS total_medicare_payment
    FROM pg.cleaned_claims
    GROUP BY claim_year, Rndrng_Prvdr_State_Abrvtn
    """)

con.execute("""
    CREATE OR REPLACE TABLE pg.summary_by_hcpcs AS
    SELECT
        claim_year,
        HCPCS_Cd AS hcpcs_code,
        HCPCS_Desc AS hcpcs_description,
        COUNT(*) FILTER (WHERE NOT is_suppressed) AS row_count,
        COUNT(*) FILTER (WHERE is_suppressed) AS suppressed_count,
        SUM(Tot_Benes) FILTER (WHERE NOT is_suppressed) AS total_beneficiaries,
        SUM(Tot_Srvcs) FILTER (WHERE NOT is_suppressed) AS total_services,
        SUM(Avg_Mdcr_Pymt_Amt * Tot_Srvcs) FILTER (WHERE NOT is_suppressed) AS total_medicare_payment
    FROM pg.cleaned_claims
    GROUP BY claim_year, HCPCS_Cd, HCPCS_Desc
    """)
print("---------- Aggregation Finished ----------\n")

# Sanity check: print row counts for each table
for table in ["raw_claims", "cleaned_claims", "summary_by_state", "summary_by_hcpcs"]:
    result: tuple[int] | None = con.execute(f"SELECT COUNT(*) FROM pg.{table}").fetchone()
    if result is not None:
        print(f"{table}: {result[0]} rows")


#
# Save cleaned results to Parquet locally
if not Path(cleaned_parquet_path).exists():
  print("---------- Saving Cleaned Parquet Starting ----------\n")
  con.execute(f"COPY pg.cleaned_claims TO '{cleaned_parquet_path}' (FORMAT PARQUET)")
  print("---------- Saving Cleaned Parquet Finished ----------\n")
else:
  print("---------- Cleaned Parquet already exists, skipping ----------\n")
