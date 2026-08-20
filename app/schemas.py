"""
app/schemas.py

Pydantic schemas defining the JSON shapes returned by the API. Separate from
models.py, which defines the database table structure.
"""

from pydantic import BaseModel, ConfigDict


class StateSummary(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    claim_year: int
    state: str
    row_count: int
    total_beneficiaries: int | None
    total_services: float | None
    total_medicare_payment: float | None


class HcpcsSummary(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    claim_year: int
    hcpcs_code: str
    hcpcs_description: str | None
    row_count: int
    total_beneficiaries: int | None
    total_services: float | None
    total_medicare_payment: float | None
