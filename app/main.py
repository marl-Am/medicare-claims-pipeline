"""
app/main.py

FastAPI entrypoint. Exposes query endpoints over the pre-aggregated
summary_by_state and summary_by_hcpcs tables.
"""

from fastapi import Depends, FastAPI
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.database import get_db
from app.models import SummaryByHcpcs, SummaryByState
from app.schemas import HcpcsSummary, StateSummary

app = FastAPI(title="ProviderPulse", description="Medicare provider payment query API")


@app.get("/claims/summary/by-state", response_model=list[StateSummary])
async def get_summary_by_state(
    year: int | None = None,
    state: str | None = None,
    db: AsyncSession = Depends(get_db),
) -> list[SummaryByState]:
    query = select(SummaryByState)
    if year is not None:
        query = query.where(SummaryByState.claim_year == year)
    if state is not None:
        query = query.where(SummaryByState.state == state.upper())

    result = await db.execute(query)
    return list(result.scalars().all())


@app.get("/claims/summary/by-hcpcs", response_model=list[HcpcsSummary])
async def get_summary_by_hcpcs(
    year: int | None = None,
    hcpcs_code: str | None = None,
    db: AsyncSession = Depends(get_db),
) -> list[SummaryByHcpcs]:
    query = select(SummaryByHcpcs)
    if year is not None:
        query = query.where(SummaryByHcpcs.claim_year == year)
    if hcpcs_code is not None:
        query = query.where(SummaryByHcpcs.hcpcs_code == hcpcs_code)

    result = await db.execute(query)
    return list(result.scalars().all())


@app.get("/health")
async def health_check() -> dict[str, str]:
    return {"status": "ok"}
