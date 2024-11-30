
import asyncpg
import contextlib
import json
import os

from datetime import date, datetime
from functools import partial
from typing import Any, AsyncIterator

from sqlalchemy import TIMESTAMP, Integer, NullPool, func
from sqlalchemy.orm import mapped_column
from sqlalchemy.ext.asyncio import (
    AsyncSession,
    create_async_engine,
    async_sessionmaker,
    AsyncConnection,
)
from sqlmodel import SQLModel

class CustomJsonEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, datetime):
            return o.isoformat()
        elif isinstance(o, date):
            return o.isoformat()

        return super().default(o)


async def asyncpg_init(connection: asyncpg.Connection) -> None:
    await connection.set_type_codec(
        "jsonb",
        encoder=partial(json.dumps, cls=CustomJsonEncoder),
        decoder=json.loads,
        schema="pg_catalog",
    )

environment = os.getenv("ENV", "dev")
SQLALCHEMY_DATABASE_URL = os.getenv("DATABASE_URL")
assert SQLALCHEMY_DATABASE_URL is not None, "DATABASE_URL environment variable must be set"

class Base(SQLModel):
    id = mapped_column(Integer, autoincrement=True, primary_key=True, index=True)
    """ The ID of this row. """
    
    created = mapped_column(TIMESTAMP(timezone=True), index=False, server_default=func.now())
    """ The timestamp when this row was created. Automatically set on creation. """

    last_modified = mapped_column(TIMESTAMP(timezone=True), index=True, server_default=func.now(), onupdate=func.now())
    """ The timestamp when this row was last modified. Automatically updated on every update. """


# https://medium.com/@tclaitken/setting-up-a-fastapi-app-with-async-sqlalchemy-2-0-pydantic-v2-e6c540be4308
class DatabaseSessionManager:
    def __init__(self, host: str, engine_kwargs: dict[str, Any] = {}):
        if environment == 'test':
            poolclass = NullPool
        else:
            poolclass = None
            
        self._engine = create_async_engine(
            host, 
            poolclass=poolclass,
            json_serializer=partial(json.dumps, cls=CustomJsonEncoder),
            **engine_kwargs
        )
        
        self._sessionmaker = async_sessionmaker(
            autocommit=False,
            expire_on_commit=False,
            bind=self._engine,
        )

    async def close(self):
        if self._engine is None:
            raise Exception("DatabaseSessionManager is not initialized")
        await self._engine.dispose()

        self._engine = None
        self._sessionmaker = None

    @contextlib.asynccontextmanager
    async def connect(self) -> AsyncIterator[AsyncConnection]:
        if self._engine is None:
            raise Exception("DatabaseSessionManager is not initialized")

        async with self._engine.begin() as connection:
            try:
                yield connection
            except Exception:
                await connection.rollback()
                raise

    @contextlib.asynccontextmanager
    async def session(self) -> AsyncIterator[AsyncSession]:
        if self._sessionmaker is None:
            raise Exception("DatabaseSessionManager is not initialized")

        session = self._sessionmaker()
        try:
            yield session
            await session.commit()
        except Exception:
            await session.rollback()
            raise
        finally:
            await session.close()


sessionmanager = DatabaseSessionManager(SQLALCHEMY_DATABASE_URL, {"echo": False})


async def get_db_session():
    async with sessionmanager.session() as session:
        yield session
