"""
app/models.py

SQLAlchemy models mapping to the pre-aggregated summary tables produced by
pipeline.py: summary_by_state and summary_by_hcpcs. These are the tables the
API queries directly, since they're small and fast compared to the 9.78M-row
cleaned_claims table.
"""

from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


class Base(DeclarativeBase):
    pass


class SummaryByState(Base):
    __tablename__ = "summary_by_state"

    claim_year: Mapped[int] = mapped_column(primary_key=True)
    state: Mapped[str] = mapped_column(primary_key=True)
    row_count: Mapped[int]
    suppressed_count: Mapped[int]
    total_beneficiaries: Mapped[int | None]
    total_services: Mapped[float | None]
    total_medicare_payment: Mapped[float | None]


class SummaryByHcpcs(Base):
    __tablename__ = "summary_by_hcpcs"

    claim_year: Mapped[int] = mapped_column(primary_key=True)
    hcpcs_code: Mapped[str] = mapped_column(primary_key=True)
    hcpcs_description: Mapped[str | None]
    row_count: Mapped[int]
    suppressed_count: Mapped[int]
    total_beneficiaries: Mapped[int | None]
    total_services: Mapped[float | None]
    total_medicare_payment: Mapped[float | None]
