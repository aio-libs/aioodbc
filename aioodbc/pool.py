# copied from aiopg
# https://github.com/aio-libs/aiopg/blob/master/aiopg/pool.py

import asyncio
import collections

from .connection import connect


__all__ = ['create_pool', 'Pool']


async def create_pool(minsize=10, maxsize=10, echo=False, loop=None,
                      **kwargs):
    if loop is None:
        loop = asyncio.get_event_loop()

    pool = Pool(minsize=minsize, maxsize=maxsize, echo=echo, loop=loop,
                **kwargs)
    if minsize > 0:
        with (await pool._cond):
            await pool._fill_free_pool(False)
    return pool


class Pool(asyncio.AbstractServer):
    """Connection pool"""

    def __init__(self, minsize, maxsize, echo, loop, **kwargs):
        if minsize < 0:
            raise ValueError("minsize should be zero or greater")
        if maxsize < minsize:
            raise ValueError("maxsize should be not less than minsize")
        self._minsize = minsize
        self._loop = loop
        self._conn_kwargs = kwargs
        self._acquiring = 0
        self._free = collections.deque(maxlen=maxsize)
        self._cond = asyncio.Condition(loop=loop)
        self._used = set()
        self._closing = False
        self._closed = False
        self._echo = echo

    @property
    def echo(self):
        return self._echo

    @property
    def minsize(self):
        return self._minsize

    @property
    def maxsize(self):
        return self._free.maxlen

    @property
    def size(self):
        return self.freesize + len(self._used) + self._acquiring

    @property
    def freesize(self):
        return len(self._free)

    @property
    def closed(self):
        return self._closed

    async def clear(self):
        """Close all free connections in pool."""
        with (await self._cond):
            while self._free:
                conn = self._free.popleft()
                await conn.close()
            self._cond.notify()

    def close(self):
        """Close pool.

        Mark all pool connections to be closed on getting back to pool.
        Closed pool doesn't allow to acquire new connections.
        """
        if self._closed:
            return
        self._closing = True

    async def wait_closed(self):
        """Wait for closing all pool's connections."""

        if self._closed:
            return
        if not self._closing:
            raise RuntimeError(".wait_closed() should be called "
                               "after .close()")

        while self._free:
            conn = self._free.popleft()
            await conn.close()

        with (await self._cond):
            while self.size > self.freesize:
                await self._cond.wait()

        self._closed = True

    async def acquire(self):
        """Acquire free connection from the pool."""
        if self._closing:
            raise RuntimeError("Cannot acquire connection after closing pool")
        with (await self._cond):
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

    async def _fill_free_pool(self, override_min):
        while self.size < self.minsize:
            self._acquiring += 1
            try:
                conn = await connect(echo=self._echo, loop=self._loop,
                                     **self._conn_kwargs)
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
                conn = await connect(echo=self._echo, loop=self._loop,
                                     **self._conn_kwargs)
                # raise exception if pool is closing
                self._free.append(conn)
                self._cond.notify()
            finally:
                self._acquiring -= 1

    async def _wakeup(self):
        with (await self._cond):
            self._cond.notify()

    async def release(self, conn):
        """Release free connection back to the connection pool.
        """
        assert conn in self._used, (conn, self._used)
        self._used.remove(conn)
        if not conn.closed:
            if self._closing:
                await conn.close()
            else:
                self._free.append(conn)
            await self._wakeup()

    def __await__(self):
        # To make `with await pool` work
        conn = yield from self.acquire()
        return _ConnectionContextManager(self, conn)

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        self.close()
        await self.wait_closed()

    def get(self):
        """Return async context manager for working with connection.
        async with pool.get() as conn:
            await conn.get(key)
        """
        return _ConnectionContextManager(self)


class _ConnectionContextManager:
    """Context manager.

    This enables the following idiom for acquiring and releasing a
    connection around a block:

        async with pool.get() as conn:
            cur = await conn.cursor()

    """

    __slots__ = ('_pool', '_conn')

    def __init__(self, pool):
        self._pool = pool
        self._conn = None

    async def __aenter__(self):
        self._conn = await self._pool.acquire()
        return self._conn

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        try:
            await self._pool.release(self._conn)
        finally:
            self._pool = None
            self._conn = None
