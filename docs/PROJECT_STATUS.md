# Healthcare Data Pipeline — Project Checklist

A FastAPI + PostgreSQL pipeline for Medicare provider data, containerized with Docker and deployed on Kubernetes.

## Setup (before Day 1)

- [X] Install Python 3.11+, Docker Desktop, and Git — python.org, docker.com, git-scm.com
- [X] Create project folder, open it in VS Code
- [ ] Create a virtual environment — VS Code Integrated Terminal, project root:
  ```
  python -m venv .venv
  ```
- [X] Activate it — same terminal:
  ```
  .venv\Scripts\Activate.ps1
  ```
- [X] Install core packages inside the activated venv:
  ```
  pip install fastapi uvicorn sqlalchemy[asyncio] asyncpg duckdb pydantic mypy
  ```
- [X] Initialize git: `git init`

## Day 1 — Get and process the data

- [X] Download the dataset: [Medicare Physician & Other Practitioners by Provider and Service](https://data.cms.gov/provider-summary-by-type-of-service/medicare-physician-other-practitioners/medicare-physician-other-practitioners-by-provider-and-service) — grab 2-3 recent years for a schema-drift problem worth writing about
- [ ] Open a sample in DuckDB or Polars to inspect columns before writing any cleaning logic
- [ ] Write the ingestion script: typed functions, mypy-checked, load raw file(s)
- [ ] Write cleaning logic: handle nulls, redacted rows, inconsistent codes across years
- [ ] Write aggregation logic: totals by procedure code, by state, by year
- [ ] Write output step: save cleaned results to Parquet locally
- [ ] Test the pipeline against a small subset before running the full file

## Day 1 — API and local database

- [ ] Write `docker-compose.yml` for a local Postgres container
- [ ] Define SQLAlchemy models matching the cleaned schema, fully typed
- [ ] Write FastAPI query endpoints (filter by state, procedure code, year)
- [ ] Run locally to test — Integrated Terminal, project root:
  ```
  uvicorn app.main:app --reload
  ```
- [ ] Load the processed Parquet output into local Postgres, confirm queries return correct results

## Day 1 — Containerize

- [ ] Write a Dockerfile for the API (pinned base image, non-root user)
- [ ] Write a Dockerfile or separate entrypoint for the pipeline script
- [ ] Build locally — Integrated Terminal or PowerShell, project root, Docker Desktop running:
  ```
  docker build -t healthdata-api .
  ```
- [ ] Test both containers together via docker-compose

## Day 2 — Cloud account and cluster

- [ ] Sign up for a cloud provider if you don't have one — Azure (portal.azure.com), AWS (aws.amazon.com), or GCP (console.cloud.google.com)
- [ ] Install the matching CLI: Azure CLI, AWS CLI, or gcloud CLI
- [ ] Create a managed cluster (example for Azure):
  ```
  az aks create ...
  ```
- [ ] Point kubectl at it:
  ```
  az aks get-credentials ...
  ```
- [ ] Create a container registry, push your images to it
- [ ] Create a cloud storage bucket for raw and processed data (Blob/S3/GCS)
- [ ] Provision a managed Postgres instance (Azure Database for PostgreSQL, RDS, or Cloud SQL)

## Day 2 — Kubernetes manifests

- [ ] Create Secrets for DB credentials and storage keys:
  ```
  kubectl create secret ...
  ```
- [ ] Write a Job manifest for the pipeline (runs once, not a long-running service)
- [ ] Write Deployment and Service manifests for the FastAPI app
- [ ] Write an Ingress manifest with TLS
- [ ] Apply everything:
  ```
  kubectl apply -f .
  ```
- [ ] Verify:
  ```
  kubectl get pods
  kubectl logs <pod-name>
  ```

## Day 3 — Hardening

- [ ] Add resource requests/limits to the Job and Deployment
- [ ] Add readiness and liveness probes to the API Deployment
- [ ] Create a scoped service account with least-privilege RBAC
- [ ] Add server-side validation on every query parameter
- [ ] Add structured logging for pipeline failures and API errors
- [ ] Add a HorizontalPodAutoscaler for the API
- [ ] Write the README: architecture, data source, setup steps, and the record count / query speed numbers for your resume line