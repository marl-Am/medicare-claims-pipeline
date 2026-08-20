# medicare-claims-pipeline
A Kubernetes-deployed pipeline that cleans and aggregates 10M+ Medicare provider payment records, served through a FastAPI query API.

2024 dataset downloaded from "https://data.cms.gov/provider-summary-by-type-of-service/medicare-physician-other-practitioners/medicare-physician-other-practitioners-by-provider-and-service"

Timing for "python -m ingestion.pipeline"

| Stage | Duration |
| :--- | :--- |
| CSV to Parquet | 1m 03s |
| Load into PostgreSQL | 11m 29s |
| Rebuild state summary | 1m 48s |
| Rebuild HCPCS summary | 2m 21s |
| Export cleaned Parquet | 4m 29s |

Total: 22 minutes for 9.8M rows