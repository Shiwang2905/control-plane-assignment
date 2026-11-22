from __future__ import annotations

from collections.abc import AsyncIterator

import asyncpg
import pytest_asyncio


@pytest_asyncio.fixture(autouse=True)
async def _clean_db(pool: asyncpg.Pool) -> AsyncIterator[None]:
    """Every integration test starts from empty tables."""
    await pool.execute("TRUNCATE tenants, tasks, outbox, processed_messages CASCADE")
    yield
