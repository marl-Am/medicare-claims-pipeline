"""
app/database.py

Async SQLAlchemy engine and session setup for the FastAPI app. Reads
connection details from the same .env file used by pipeline.py.
"""

import os
from collections.abc import AsyncGenerator
from urllib.parse import quote_plus

from dotenv import load_dotenv
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)

load_dotenv()

pg_host: str = os.environ["PG_HOST"]
pg_port: str = os.environ["PG_PORT"]
pg_db: str = os.environ["PG_DB"]
pg_user: str = os.environ["PG_USER"]
pg_password: str = os.environ["PG_PASSWORD"]

# Off by default so local Docker Postgres, which isn't configured for TLS,
# is unaffected. Neon (and most hosted Postgres providers) require SSL on
# every connection; set PG_SSL=true wherever the target is Neon.
pg_ssl: bool = os.environ.get("PG_SSL", "false").strip().lower() == "true"

# The user and password are percent-encoded before going into the URL. A
# generated password containing @, /, :, or # would otherwise be parsed as URL
# structure and the connection would fail with a misleading error.
DATABASE_URL: str = (
    f"postgresql+asyncpg://{quote_plus(pg_user)}:{quote_plus(pg_password)}"
    f"@{pg_host}:{pg_port}/{pg_db}"
)

# asyncpg takes ssl as a direct connect argument rather than a URL query
# parameter; passing it this way sidesteps any ambiguity about which
# libpq-style query params (sslmode, channel_binding) asyncpg does or
# doesn't recognize through SQLAlchemy's URL parsing.
connect_args: dict[str, object] = {"ssl": True} if pg_ssl else {}

engine: AsyncEngine = create_async_engine(
    DATABASE_URL, echo=False, connect_args=connect_args
)

AsyncSessionLocal: async_sessionmaker[AsyncSession] = async_sessionmaker(
    bind=engine,
    expire_on_commit=False,
)


async def get_db() -> AsyncGenerator[AsyncSession, None]:
    """FastAPI dependency that yields a database session per request."""
    async with AsyncSessionLocal() as session:
        yield session