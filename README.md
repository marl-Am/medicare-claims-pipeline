# medicare-claims-pipeline
A Kubernetes-deployed pipeline that cleans and aggregates 10M+ Medicare provider payment records, served through a FastAPI query API.

2023 dataset from: "https://data.cms.gov/provider-summary-by-type-of-service/medicare-physician-other-practitioners/medicare-physician-other-practitioners-by-provider-and-service/data/2023"

2024 dataset from: "https://data.cms.gov/provider-summary-by-type-of-service/medicare-physician-other-practitioners/medicare-physician-other-practitioners-by-provider-and-service/data/2024"

# Pipeline Run Timings

Real-data runs against the full Medicare Physician & Other Practitioners
dataset. Windows, local Docker Postgres.

## 2024 reload (post-cleanup, with ANALYZE)

| Stage | Duration |
| :--- | :--- |
| CSV to Parquet | skipped (already existed) |
| Load into PostgreSQL | 11m 25s |
| Rebuild state summary | 4m 03s |
| Rebuild HCPCS summary | 3m 45s |
| Export cleaned Parquet | skipped (already existed) |
| ANALYZE | 1m 50s |

**Total: 22m 06s for 9.78M rows**

## 2023 reload (append, post-cleanup, with ANALYZE)

| Stage | Duration |
| :--- | :--- |
| CSV to Parquet | skipped (already existed) |
| Load into PostgreSQL | 7m 36s |
| Rebuild state summary | 4m 54s |
| Rebuild HCPCS summary | 4m 55s |
| Export cleaned Parquet | skipped (already existed) |
| ANALYZE | 0m 55s |

**Total: 18m 52s for 19.44M rows combined**
