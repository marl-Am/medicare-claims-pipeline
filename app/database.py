"""
app/database.py

Async SQLAlchemy engine and session setup for the FastAPI app. Reads
connection details from the same .env file used by pipeline.py.
"""

import os
from collections.abc import AsyncGenerator

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

DATABASE_URL: str = (
    f"postgresql+asyncpg://{pg_user}:{pg_password}@{pg_host}:{pg_port}/{pg_db}"
)

engine: AsyncEngine = create_async_engine(DATABASE_URL, echo=False)

AsyncSessionLocal: async_sessionmaker[AsyncSession] = async_sessionmaker(
    bind=engine,
    expire_on_commit=False,
)


async def get_db() -> AsyncGenerator[AsyncSession, None]:
    """FastAPI dependency that yields a database session per request."""
    async with AsyncSessionLocal() as session:
        yield session
