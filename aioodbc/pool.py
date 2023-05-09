# copied from aiopg
# https://github.com/aio-libs/aiopg/blob/master/aiopg/pool.py

from __future__ import annotations

import asyncio
import collections
import warnings
from types import TracebackType
from typing import Any, Deque, Optional, Set, Type

from pyodbc import ProgrammingError

from .connection import Connection, connect
from .log import logger
from .utils import _ContextManager

__all__ = ["create_pool", "Pool"]


class Pool:
    """Connection pool"""

    def __init__(
        self,
        dsn: str,
        minsize: int,
        maxsize: int,
        echo: bool,
        pool_recycle: int,
        loop: Optional[asyncio.AbstractEventLoop] = None,
        **kwargs: Any,
    ) -> None:
        if minsize < 0:
            raise ValueError("minsize should be zero or greater")
        if maxsize < minsize:
            raise ValueError("maxsize should be not less than minsize")

        if loop is not None:
            msg = "Explicit loop is deprecated, and has no effect."
            warnings.warn(msg, DeprecationWarning, stacklevel=2)

        self._dsn = dsn
        self._minsize = minsize
        self._maxsize = maxsize
        self._loop = asyncio.get_event_loop()
        self._conn_kwargs = kwargs
        self._acquiring = 0
        self._recycle = pool_recycle
        self._free: Deque[Connection] = collections.deque(maxlen=maxsize)
        self._cond = asyncio.Condition()
        self._used: Set[Connection] = set()
        self._closing = False
        self._closed = False
        self._echo = echo

    @property
    def echo(self) -> bool:
        return self._echo

    @property
    def minsize(self) -> int:
        return self._minsize

    @property
    def maxsize(self) -> int:
        return self._free.maxlen or self._maxsize

    @property
    def size(self) -> int:
        return self.freesize + len(self._used) + self._acquiring

    @property
    def freesize(self) -> int:
        return len(self._free)

    @property
    def closed(self) -> bool:
        return self._closed

    async def clear(self) -> None:
        """Close all free connections in pool."""
        async with self._cond:
            while self._free:
                conn = self._free.popleft()
                await conn.close()
            self._cond.notify()

    def close(self) -> None:
        """Close pool.

        Mark all pool connections to be closed on getting back to pool.
        Closed pool doesn't allow to acquire new connections.
        """
        if self._closed:
            return
        self._closing = True

    async def wait_closed(self) -> None:
        """Wait for closing all pool's connections."""

        if self._closed:
            return
        if not self._closing:
            raise RuntimeError(
                ".wait_closed() should be called " "after .close()"
            )

        while self._free:
            conn = self._free.popleft()
            await conn.close()

        async with self._cond:
            while self.size > self.freesize:
                await self._cond.wait()

        self._closed = True

    def acquire(self) -> _ContextManager[Connection]:
        """Acquire free connection from the pool."""
        coro = self._acquire()
        return _ContextManager[Connection](coro, self.release)

    async def _acquire(self) -> Connection:
        if self._closing:
            raise RuntimeError("Cannot acquire connection after closing pool")
        async with self._cond:
            while True:
                await self._fill_free_pool(True)
                if self._free:
                    conn = self._free.popleft()
                    assert not conn.closed, conn
                    assert conn not in self._used, (conn, self._used)
                    self._used.add(conn)
                    return conn
                else:
                    await self._cond.wait()

    async def _fill_free_pool(self, override_min: bool) -> None:
        n, free = 0, len(self._free)
        while n < free:
            conn = self._free[-1]
            if (
                self._recycle > -1
                and self._loop.time() - conn.last_usage > self._recycle
            ):
                try:
                    if not conn.closed:
                        await conn.close()
                except ProgrammingError as e:
                    # Sometimes conn.closed is False even if connection has
                    # been already closed (ex. for impala driver
                    #   clouderaimpalaodbc_2.6.16.1022-2_amd64.deb).
                    # conn.close() will raise ProgrammingError in this case.
                    logger.warning(e)

                self._free.pop()
            else:
                self._free.rotate()
            n += 1

        while self.size < self.minsize:
            self._acquiring += 1
            try:
                conn = await connect(
                    dsn=self._dsn,
                    echo=self._echo,
                    **self._conn_kwargs,
                )
                # raise exception if pool is closing
                self._free.append(conn)
                self._cond.notify()
            finally:
                self._acquiring -= 1
        if self._free:
            return

        if override_min and self.size < self.maxsize:
            self._acquiring += 1
            try:
                conn = await connect(
                    dsn=self._dsn,
                    echo=self._echo,
                    **self._conn_kwargs,
                )
                # raise exception if pool is closing
                self._free.append(conn)
                self._cond.notify()
            finally:
                self._acquiring -= 1

    async def _wakeup(self) -> None:
        async with self._cond:
            self._cond.notify()

    async def release(self, conn: Connection) -> None:
        """Release free connection back to the connection pool."""
        assert conn in self._used, (conn, self._used)
        self._used.remove(conn)
        if not conn.closed:
            if self._closing:
                await conn.close()
            else:
                self._free.append(conn)
            await self._wakeup()

    async def __aenter__(self) -> Pool:
        return self

    async def __aexit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc: Optional[BaseException],
        tb: Optional[TracebackType],
    ) -> None:
        self.close()
        await self.wait_closed()


async def _destroy_pool(pool: Pool) -> None:
    pool.close()
    await pool.wait_closed()


async def _create_pool(
    dsn: str,
    minsize: int = 10,
    maxsize: int = 10,
    echo: bool = False,
    pool_recycle: int = -1,
    **kwargs: Any,
) -> Pool:
    pool = Pool(
        dsn=dsn,
        minsize=minsize,
        maxsize=maxsize,
        echo=echo,
        pool_recycle=pool_recycle,
        loop=None,
        **kwargs,
    )
    if minsize > 0:
        async with pool._cond:
            await pool._fill_free_pool(False)
    return pool


def create_pool(
    dsn: str,
    minsize: int = 10,
    maxsize: int = 10,
    echo: bool = False,
    loop: None = None,
    pool_recycle: int = -1,
    **kwargs: Any,
) -> _ContextManager[Pool]:
    if loop is not None:
        msg = "Explicit loop is deprecated, and has no effect."
        warnings.warn(msg, DeprecationWarning, stacklevel=2)

    return _ContextManager[Pool](
        _create_pool(
            dsn=dsn,
            minsize=minsize,
            maxsize=maxsize,
            echo=echo,
            pool_recycle=pool_recycle,
            **kwargs,
        ),
        _destroy_pool,
    )
