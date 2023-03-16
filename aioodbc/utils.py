import sys
from collections.abc import Coroutine

from pyodbc import Error

PY_352 = sys.version_info >= (3, 5, 2)

# Issue #195.  Don't pollute the pool with bad conns
# Unfortunately occasionally sqlite will return 'HY000' for invalid query,
# so we need specialize the check
_CONN_CLOSE_ERRORS = {
    # [Microsoft][ODBC Driver 17 for SQL Server]Communication link failure
    "08S01": None,
    # [HY000] server closed the connection unexpectedly
    "HY000": "[HY000] server closed the connection unexpectedly",
}


def _is_conn_close_error(e):
    if not isinstance(e, Error) or len(e.args) < 2:
        return False

    sqlstate, msg = e.args[0], e.args[1]
    if sqlstate not in _CONN_CLOSE_ERRORS:
        return False

    check_msg = _CONN_CLOSE_ERRORS[sqlstate]
    if not check_msg:
        return True

    return msg.startswith(check_msg)


class _ContextManager(Coroutine):
    __slots__ = ("_coro", "_obj")

    def __init__(self, coro):
        self._coro = coro
        self._obj = None

    def send(self, value):
        return self._coro.send(value)

    def throw(self, typ, val=None, tb=None):
        if val is None:
            return self._coro.throw(typ)
        elif tb is None:
            return self._coro.throw(typ, val)
        else:
            return self._coro.throw(typ, val, tb)

    def close(self):
        return self._coro.close()

    @property
    def gi_frame(self):
        return self._coro.gi_frame

    @property
    def gi_running(self):
        return self._coro.gi_running

    @property
    def gi_code(self):
        return self._coro.gi_code

    def __next__(self):
        return self.send(None)

    def __await__(self):
        return self._coro.__await__()

    async def __aenter__(self):
        self._obj = await self._coro
        return self._obj

    async def __aexit__(self, exc_type, exc, tb):
        if exc_type:
            await self._obj.rollback()
        elif not self._obj.autocommit:
            await self._obj.commit()
        await self._obj.close()
        self._obj = None


class _PoolContextManager(_ContextManager):
    async def __aexit__(self, exc_type, exc, tb):
        self._obj.close()
        await self._obj.wait_closed()
        self._obj = None


class _PoolConnectionContextManager(_ContextManager):
    __slots__ = ("_coro", "_conn", "_pool")

    def __init__(self, coro, pool):
        self._coro = coro
        self._conn = None
        self._pool = pool

    def __await__(self):
        self._pool = None
        return self._coro.__await__()

    async def __aenter__(self):
        self._conn = await self._coro
        return self._conn

    async def __aexit__(self, exc_type, exc, tb):
        await self._pool.release(self._conn)
        self._pool = None
        self._conn = None
