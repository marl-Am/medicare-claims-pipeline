"""
app/models.py

SQLAlchemy models mapping to the pre-aggregated summary tables produced by
ingestion/pipeline.py: summary_by_state and summary_by_hcpcs. These are the
tables the API queries directly, since they hold a few thousand rows against
the millions in cleaned_claims.

Both carry a composite primary key on claim_year plus the grouping column,
so each loaded year contributes its own row per state and per procedure code.

No suppression tracking: CMS excludes low-volume provider-service records
from this dataset before publishing it, rather than including them with
blanked fields, so there is nothing in the source data for a per-row
suppression flag to detect.
"""

from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


class Base(DeclarativeBase):
    pass


class SummaryByState(Base):
    __tablename__ = "summary_by_state"

    claim_year: Mapped[int] = mapped_column(primary_key=True)
    state: Mapped[str] = mapped_column(primary_key=True)
    row_count: Mapped[int]
    total_beneficiaries: Mapped[int | None]
    total_services: Mapped[float | None]
    total_medicare_payment: Mapped[float | None]


class SummaryByHcpcs(Base):
    __tablename__ = "summary_by_hcpcs"

    claim_year: Mapped[int] = mapped_column(primary_key=True)
    hcpcs_code: Mapped[str] = mapped_column(primary_key=True)
    hcpcs_description: Mapped[str | None]
    row_count: Mapped[int]
    total_beneficiaries: Mapped[int | None]
    total_services: Mapped[float | None]
    total_medicare_payment: Mapped[float | None]
