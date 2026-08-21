"""
app/main.py

FastAPI entrypoint. Exposes query endpoints over the pre-aggregated
summary_by_state and summary_by_hcpcs tables.
"""

import logging
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import Annotated, Final

from fastapi import Depends, FastAPI, Query, Response
from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import AsyncSession

from app.database import get_db, pg_db, pg_host, pg_port
from app.models import SummaryByHcpcs, SummaryByState
from app.schemas import HcpcsSummary, StateSummary

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)
logger = logging.getLogger(__name__)

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


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    logger.info(
        "ProviderPulse API starting up, database target %s:%s/%s",
        pg_host,
        pg_port,
        pg_db,
    )
    yield
    logger.info("ProviderPulse API shutting down")


app = FastAPI(
    title="ProviderPulse",
    description="Medicare provider payment query API",
    lifespan=lifespan,
)


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
    rows = list(result.scalars().all())
    logger.info(
        "by-state query: year=%s state=%s -> %d rows",
        year,
        state.upper() if state else None,
        len(rows),
    )
    return rows


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
    rows = list(result.scalars().all())
    logger.info(
        "by-hcpcs query: year=%s hcpcs_code=%s -> %d rows",
        year,
        hcpcs_code.upper() if hcpcs_code else None,
        len(rows),
    )
    return rows


@app.get("/health")
async def health_check(
    response: Response, db: AsyncSession = Depends(get_db)
) -> dict[str, str]:
    try:
        await db.execute(text("SELECT 1"))
    except Exception:
        # Broad by design: a health probe's job is to report any failure
        # reaching the database as unhealthy, not to distinguish causes.
        # The cause goes to the log, not the response body.
        logger.exception("Health check failed: database unreachable")
        response.status_code = 503
        return {"status": "error", "detail": "database unreachable"}
    return {"status": "ok"}
