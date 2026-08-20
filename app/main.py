"""
app/main.py

FastAPI entrypoint. Exposes query endpoints over the pre-aggregated
summary_by_state and summary_by_hcpcs tables.
"""

from typing import Annotated, Final

from fastapi import Depends, FastAPI, Query
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.database import get_db
from app.models import SummaryByHcpcs, SummaryByState
from app.schemas import HcpcsSummary, StateSummary

# Deliberately duplicated from ingestion/pipeline.py rather than imported.
# The API container has no reason to depend on the ingestion package, and
# importing it would pull DuckDB into an image that never calls it.
MIN_CLAIM_YEAR: Final[int] = 1990
MAX_CLAIM_YEAR: Final[int] = 2100

# Two letters: the 50 states, territories, and the CMS "XX" placeholder.
STATE_PATTERN: Final[str] = r"^[A-Za-z]{2}$"

# HCPCS codes are five characters. CPT codes are all digits (99213), while
# HCPCS Level II codes start with a letter (J1885, G0008).
HCPCS_PATTERN: Final[str] = r"^[A-Za-z0-9]{5}$"

YearQuery = Annotated[
    int | None,
    Query(
        ge=MIN_CLAIM_YEAR,
        le=MAX_CLAIM_YEAR,
        description="Filter to a single claim year.",
    ),
]

app = FastAPI(title="ProviderPulse", description="Medicare provider payment query API")


@app.get("/claims/summary/by-state", response_model=list[StateSummary])
async def get_summary_by_state(
    year: YearQuery = None,
    state: Annotated[
        str | None,
        Query(
            pattern=STATE_PATTERN,
            description='Two-letter state abbreviation. "XX" covers unknown '
            "and foreign provider records.",
        ),
    ] = None,
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
    year: YearQuery = None,
    hcpcs_code: Annotated[
        str | None,
        Query(
            pattern=HCPCS_PATTERN,
            description="Five-character HCPCS or CPT procedure code.",
        ),
    ] = None,
    db: AsyncSession = Depends(get_db),
) -> list[SummaryByHcpcs]:
    query = select(SummaryByHcpcs)
    if year is not None:
        query = query.where(SummaryByHcpcs.claim_year == year)
    if hcpcs_code is not None:
        query = query.where(SummaryByHcpcs.hcpcs_code == hcpcs_code.upper())

    result = await db.execute(query)
    return list(result.scalars().all())


@app.get("/health")
async def health_check() -> dict[str, str]:
    return {"status": "ok"}
