"""
tests/test_api.py

Exercises the FastAPI endpoints against an in-memory SQLite database. The
`get_db` dependency is overridden, so no PostgreSQL instance is needed and the
tests leave no state behind.

SQLite stands in for PostgreSQL because the summary tables use only integers,
floats, and text. Nothing in the query layer depends on a PostgreSQL-specific
feature, so the substitution is safe here.
"""

from __future__ import annotations

from collections.abc import AsyncIterator
from typing import Any

import pytest
import pytest_asyncio
from httpx import ASGITransport, AsyncClient
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.pool import StaticPool

from app.database import get_db
from app.main import MAX_CLAIM_YEAR, MIN_CLAIM_YEAR, app
from app.models import Base, SummaryByHcpcs, SummaryByState

# Two years of data so the year filter has something to separate. The "XX"
# row is the CMS placeholder for unknown or foreign provider records.
#
# No suppressed/null-total row here: CMS excludes low-volume provider-service
# combinations from this dataset before publishing it, rather than including
# them with blanked fields, so every real summary row carries real totals.
# See test_null_totals_still_serialize_correctly below for the one place a
# null total is still exercised.
STATE_ROWS: list[dict[str, Any]] = [
    {
        "claim_year": 2024,
        "state": "CA",
        "row_count": 320,
        "total_beneficiaries": 18400,
        "total_services": 41200.0,
        "total_medicare_payment": 2150340.50,
    },
    {
        "claim_year": 2024,
        "state": "TX",
        "row_count": 150,
        "total_beneficiaries": 4200,
        "total_services": 9100.5,
        "total_medicare_payment": 512340.75,
    },
    {
        "claim_year": 2024,
        "state": "XX",
        "row_count": 3,
        "total_beneficiaries": 55,
        "total_services": 120.0,
        "total_medicare_payment": 6100.0,
    },
    {
        "claim_year": 2023,
        "state": "TX",
        "row_count": 140,
        "total_beneficiaries": 3900,
        "total_services": 8800.0,
        "total_medicare_payment": 488210.25,
    },
    # Not a real scenario the pipeline produces: a state whose totals are null.
    # Present only so the serialization test below has something to exercise,
    # since the schema keeps these fields Optional. QQ is not a real or
    # already-used state code, so it stays isolated from every other test.
    {
        "claim_year": 2024,
        "state": "QQ",
        "row_count": 1,
        "total_beneficiaries": None,
        "total_services": None,
        "total_medicare_payment": None,
    },
]

HCPCS_ROWS: list[dict[str, Any]] = [
    {
        "claim_year": 2024,
        "hcpcs_code": "99213",
        "hcpcs_description": "Office visit, established patient",
        "row_count": 500,
        "total_beneficiaries": 15000,
        "total_services": 32000.0,
        "total_medicare_payment": 1750000.0,
    },
    {
        "claim_year": 2024,
        "hcpcs_code": "99214",
        "hcpcs_description": None,
        "row_count": 300,
        "total_beneficiaries": 9000,
        "total_services": 19500.5,
        "total_medicare_payment": 1120000.5,
    },
    {
        "claim_year": 2023,
        "hcpcs_code": "99213",
        "hcpcs_description": "Office visit, established patient",
        "row_count": 480,
        "total_beneficiaries": 14100,
        "total_services": 30500.0,
        "total_medicare_payment": 1690000.0,
    },
    # A Level II code, stored uppercase as CMS publishes it. The all-numeric
    # codes above cannot exercise the endpoint's .upper() call, since their
    # casing is already invariant.
    {
        "claim_year": 2024,
        "hcpcs_code": "J1885",
        "hcpcs_description": "Ketorolac tromethamine injection",
        "row_count": 210,
        "total_beneficiaries": 6200,
        "total_services": 14800.0,
        "total_medicare_payment": 88400.0,
    },
]

STATE_URL: str = "/claims/summary/by-state"
HCPCS_URL: str = "/claims/summary/by-hcpcs"


@pytest_asyncio.fixture
async def client() -> AsyncIterator[AsyncClient]:
    """An HTTP client wired to the app, backed by a seeded SQLite database.

    StaticPool keeps every session on the same in-memory connection. Without it,
    each new connection would open a fresh empty database.
    """
    engine = create_async_engine(
        "sqlite+aiosqlite:///:memory:",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    session_factory = async_sessionmaker(bind=engine, expire_on_commit=False)

    async with engine.begin() as connection:
        await connection.run_sync(Base.metadata.create_all)

    async with session_factory() as session:
        session.add_all(SummaryByState(**row) for row in STATE_ROWS)
        session.add_all(SummaryByHcpcs(**row) for row in HCPCS_ROWS)
        await session.commit()

    async def override_get_db() -> AsyncIterator[AsyncSession]:
        async with session_factory() as session:
            yield session

    app.dependency_overrides[get_db] = override_get_db
    transport = ASGITransport(app=app)
    try:
        async with AsyncClient(
            transport=transport, base_url="http://test"
        ) as http_client:
            yield http_client
    finally:
        app.dependency_overrides.clear()
        await engine.dispose()


# ---------------------------------------------------------------------------
# Health
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_health_reports_ok(client: AsyncClient) -> None:
    response = await client.get("/health")
    assert response.status_code == 200
    assert response.json() == {"status": "ok"}


# ---------------------------------------------------------------------------
# State summaries
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_state_endpoint_returns_every_row_when_unfiltered(
    client: AsyncClient,
) -> None:
    response = await client.get(STATE_URL)
    assert response.status_code == 200
    assert len(response.json()) == len(STATE_ROWS)


@pytest.mark.asyncio
async def test_state_response_carries_every_schema_field(client: AsyncClient) -> None:
    response = await client.get(STATE_URL, params={"year": 2024, "state": "TX"})
    payload = response.json()
    assert len(payload) == 1
    assert set(payload[0]) == {
        "claim_year",
        "state",
        "row_count",
        "total_beneficiaries",
        "total_services",
        "total_medicare_payment",
    }


@pytest.mark.asyncio
async def test_year_filter_separates_the_two_years(client: AsyncClient) -> None:
    response = await client.get(STATE_URL, params={"year": 2023})
    payload = response.json()
    assert len(payload) == 1
    assert payload[0]["claim_year"] == 2023
    assert payload[0]["state"] == "TX"


@pytest.mark.asyncio
async def test_state_filter_returns_both_years_for_that_state(
    client: AsyncClient,
) -> None:
    response = await client.get(STATE_URL, params={"state": "TX"})
    years = sorted(row["claim_year"] for row in response.json())
    assert years == [2023, 2024]


@pytest.mark.asyncio
@pytest.mark.parametrize("supplied_state", ["tx", "Tx", "TX"])
async def test_state_filter_is_case_insensitive(
    client: AsyncClient, supplied_state: str
) -> None:
    """The endpoint calls .upper() on the parameter, so casing should not matter."""
    response = await client.get(
        STATE_URL, params={"state": supplied_state, "year": 2024}
    )
    payload = response.json()
    assert len(payload) == 1
    assert payload[0]["state"] == "TX"


@pytest.mark.asyncio
async def test_state_and_year_filters_combine(client: AsyncClient) -> None:
    response = await client.get(STATE_URL, params={"state": "TX", "year": 2024})
    payload = response.json()
    assert len(payload) == 1
    assert (payload[0]["claim_year"], payload[0]["state"]) == (2024, "TX")


@pytest.mark.asyncio
async def test_null_totals_still_serialize_correctly(client: AsyncClient) -> None:
    """The schema keeps total_* fields Optional. This confirms that if a row
    ever did carry a null total, it reaches the client as JSON null rather
    than being coerced to 0, which would misleadingly read as 'no payments.'
    Not a scenario the real pipeline produces: CMS excludes low-volume
    provider-service combinations before publishing the file, rather than
    including them with blanked fields, so real summary rows always carry
    real totals."""
    response = await client.get(STATE_URL, params={"state": "QQ"})
    row = response.json()[0]
    assert row["total_beneficiaries"] is None
    assert row["total_services"] is None
    assert row["total_medicare_payment"] is None


@pytest.mark.asyncio
async def test_xx_placeholder_state_is_queryable(client: AsyncClient) -> None:
    response = await client.get(STATE_URL, params={"state": "XX"})
    assert len(response.json()) == 1


@pytest.mark.asyncio
async def test_unknown_state_returns_an_empty_list(client: AsyncClient) -> None:
    response = await client.get(STATE_URL, params={"state": "ZZ"})
    assert response.status_code == 200
    assert response.json() == []


@pytest.mark.asyncio
async def test_year_with_no_data_returns_an_empty_list(client: AsyncClient) -> None:
    response = await client.get(STATE_URL, params={"year": 1999})
    assert response.status_code == 200
    assert response.json() == []


# ---------------------------------------------------------------------------
# HCPCS summaries
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_hcpcs_endpoint_returns_every_row_when_unfiltered(
    client: AsyncClient,
) -> None:
    response = await client.get(HCPCS_URL)
    assert response.status_code == 200
    assert len(response.json()) == len(HCPCS_ROWS)


@pytest.mark.asyncio
async def test_hcpcs_code_filter_spans_years(client: AsyncClient) -> None:
    response = await client.get(HCPCS_URL, params={"hcpcs_code": "99213"})
    years = sorted(row["claim_year"] for row in response.json())
    assert years == [2023, 2024]


@pytest.mark.asyncio
async def test_hcpcs_code_and_year_filters_combine(client: AsyncClient) -> None:
    response = await client.get(HCPCS_URL, params={"hcpcs_code": "99213", "year": 2024})
    payload = response.json()
    assert len(payload) == 1
    assert payload[0]["total_beneficiaries"] == 15000


@pytest.mark.asyncio
@pytest.mark.parametrize("supplied_code", ["j1885", "J1885"])
async def test_hcpcs_filter_uppercases_the_code(
    client: AsyncClient, supplied_code: str
) -> None:
    """Level II codes carry a letter prefix and are uppercase in the source
    data, so a lowercase query still has to match."""
    response = await client.get(HCPCS_URL, params={"hcpcs_code": supplied_code})
    payload = response.json()
    assert len(payload) == 1
    assert payload[0]["hcpcs_code"] == "J1885"


@pytest.mark.asyncio
async def test_malformed_hcpcs_code_is_rejected(client: AsyncClient) -> None:
    """A trailing space used to produce an empty 200 response, which looked the
    same as a valid code with no data behind it."""
    response = await client.get(HCPCS_URL, params={"hcpcs_code": "99213 "})
    assert response.status_code == 422


@pytest.mark.asyncio
async def test_missing_hcpcs_description_serializes_as_null(
    client: AsyncClient,
) -> None:
    response = await client.get(HCPCS_URL, params={"hcpcs_code": "99214"})
    assert response.json()[0]["hcpcs_description"] is None


@pytest.mark.asyncio
async def test_hcpcs_totals_keep_their_decimal_component(client: AsyncClient) -> None:
    """total_services is a float because CMS reports fractional service counts."""
    response = await client.get(HCPCS_URL, params={"hcpcs_code": "99214"})
    assert response.json()[0]["total_services"] == pytest.approx(19500.5)


# ---------------------------------------------------------------------------
# Input handling
#
# Malformed input is rejected before any query runs, so a 200 response always
# means the question was answerable and an empty list always means no data.
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_non_numeric_year_is_rejected(client: AsyncClient) -> None:
    """FastAPI rejects this on the type annotation, with no code of ours involved."""
    response = await client.get(STATE_URL, params={"year": "last-year"})
    assert response.status_code == 422


@pytest.mark.asyncio
@pytest.mark.parametrize("bad_year", [1899, 12345, -2024, 0])
async def test_year_outside_the_supported_range_is_rejected(
    client: AsyncClient, bad_year: int
) -> None:
    response = await client.get(STATE_URL, params={"year": bad_year})
    assert response.status_code == 422


@pytest.mark.asyncio
@pytest.mark.parametrize("bad_state", ["CALIFORNIA", "C", "T3", "  ", "'; DROP TABLE"])
async def test_malformed_state_code_is_rejected(
    client: AsyncClient, bad_state: str
) -> None:
    response = await client.get(STATE_URL, params={"state": bad_state})
    assert response.status_code == 422


@pytest.mark.asyncio
async def test_rejection_names_the_offending_parameter(client: AsyncClient) -> None:
    """The 422 body should point at what was wrong, so a caller can correct it."""
    response = await client.get(STATE_URL, params={"state": "CALIFORNIA"})
    detail = response.json()["detail"]
    assert any("state" in entry["loc"] for entry in detail)


def test_year_bounds_are_pinned() -> None:
    """These constants are deliberately duplicated in ingestion/pipeline.py
    rather than imported, so the API image need not depend on the ingestion
    package. Pinning the literals on both sides makes a one-sided edit fail."""
    assert (MIN_CLAIM_YEAR, MAX_CLAIM_YEAR) == (1990, 2100)


@pytest.mark.asyncio
async def test_valid_boundary_years_are_accepted(client: AsyncClient) -> None:
    for boundary_year in (1990, 2100):
        response = await client.get(STATE_URL, params={"year": boundary_year})
        assert response.status_code == 200


@pytest.mark.asyncio
async def test_unknown_query_parameters_are_ignored(client: AsyncClient) -> None:
    response = await client.get(STATE_URL, params={"sort_by": "payment", "year": 2023})
    assert response.status_code == 200
    assert len(response.json()) == 1


@pytest.mark.asyncio
async def test_undefined_route_returns_404(client: AsyncClient) -> None:
    response = await client.get("/claims/summary/by-provider")
    assert response.status_code == 404


# ---------------------------------------------------------------------------
# Documentation surface
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_openapi_schema_lists_both_summary_routes(client: AsyncClient) -> None:
    response = await client.get("/openapi.json")
    assert response.status_code == 200
    assert {STATE_URL, HCPCS_URL, "/health"} <= set(response.json()["paths"])
