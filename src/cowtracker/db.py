
import asyncpg  # type: ignore
from asyncpg import Connection
from asyncpg.pool import Pool
import logging
import re
import ujson
from typing import Any, Dict, Optional, Union


logger = logging.getLogger('db')


T_dbkey = Union[int, str]
T_dbdef = Union[str, Dict[str, Any]]

_DB_URI: str = 'undefined_db_uri'
_DB_NAME: str = 'pangote'
DEFAULT_DBDEF: str = _DB_URI
_CONFIG: Dict[str, Any] = {'dburi': DEFAULT_DBDEF}
_DBPOOL: Optional['_DbPool'] = None


def conf_db_uri(host: str, user: str, port: str = '5432', db: str = _DB_NAME) -> str:
    db_ = f'postgres://{user}@{host}:{port}/{db}'
    global _DB_URI, DEFAULT_DBDEF, _CONFIG
    _DB_URI = DEFAULT_DBDEF = db_
    _CONFIG = {'dburi': DEFAULT_DBDEF}
    return db_


async def shutdown() -> None:
    global _DBPOOL
    logger.debug('Shutting down DBPOOL...')
    await _DBPOOL.shutdown()
    _DBPOOL = None
    logger.debug('DBPOOL shutdown completed.')


class _DbPool:
    def __init__(self, dbdef: T_dbdef) -> None:
        self._dbdef = {'dsn': dbdef}
        self._pool: Optional[Pool] = None

    async def _init_pool_con(self, conn: Connection) -> None:
        await conn.set_type_codec('jsonb', encoder=ujson.dumps, decoder=ujson.loads, schema='pg_catalog')

    async def get_pool(self) -> Pool:
        if not self._pool:
            self._pool = await asyncpg.create_pool(**self._dbdef, init=self._init_pool_con, min_size=3)
        return self._pool

    async def shutdown(self) -> None:
        if self._pool:
            pool = self._pool
            self._pool = None
            await pool.close()


def _lookup_dbpool() -> _DbPool:
    global _DBPOOL
    if _DBPOOL is None:
        _DBPOOL = _DbPool(_CONFIG.get("dburi", DEFAULT_DBDEF))
    return _DBPOOL


async def pool() -> Pool:
    return await _lookup_dbpool().get_pool()


class ConnCtx():
    def __init__(self) -> None:
        self.pool = None
        self.conn = None
        self.trans = None

    async def __aenter__(self) -> Connection:
        dbpool = self.pool = await pool()
        assert dbpool is not None
        conn = await dbpool.acquire()
        try:
            trans = conn.transaction()
            await trans.start()
            self.trans = trans
        except Exception:
            try:
                await dbpool.release(self.conn)
            except Exception:
                pass
            raise
        self.conn = conn
        return conn

    async def __aexit__(self, extype, ex, tb):
        try:
            if self.trans:
                if extype is not None:
                    await self.trans.rollback()
                else:
                    await self.trans.commit()
                self.trans = None
        except Exception:
            pass
        await self.pool.release(self.conn)


def pg_str(s: Optional[str]) -> str:
    PG_QUOTE_REGEX = re.compile("([\\\\'])")
    return 'NULL' if s is None else "E'"+PG_QUOTE_REGEX.sub(r'\\\1', s)+"'"


async def connection() -> ConnCtx:
    return ConnCtx()
