# copied from aiopg
# https://github.com/aio-libs/aiopg/blob/master/aiopg/pool.py

import asyncio
import collections

from .connection import connect


@asyncio.coroutine
def create_pool(minsize=10, maxsize=10, echo=False, loop=None, **kwargs):
    if loop is None:
        loop = asyncio.get_event_loop()

    pool = Pool(minsize=minsize, maxsize=maxsize, echo=echo, loop=loop,
                **kwargs)
    if minsize > 0:
        with (yield from pool._cond):
            yield from pool._fill_free_pool(False)
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

    @asyncio.coroutine
    def clear(self):
        """Close all free connections in pool."""
        with (yield from self._cond):
            while self._free:
                conn = self._free.popleft()
                yield from conn.ensure_closed()
            self._cond.notify()

    def close(self):
        """Close pool.

        Mark all pool connections to be closed on getting back to pool.
        Closed pool doesn't allow to acquire new connections.
        """
        if self._closed:
            return
        self._closing = True

    @asyncio.coroutine
    def wait_closed(self):
        """Wait for closing all pool's connections."""

        if self._closed:
            return
        if not self._closing:
            raise RuntimeError(".wait_closed() should be called "
                               "after .close()")

        while self._free:
            conn = self._free.popleft()
            yield from conn.ensure_closed()

        with (yield from self._cond):
            while self.size > self.freesize:
                yield from self._cond.wait()

        self._closed = True

    @asyncio.coroutine
    def acquire(self):
        """Acquire free connection from the pool."""
        if self._closing:
            raise RuntimeError("Cannot acquire connection after closing pool")
        with (yield from self._cond):
            while True:
                yield from self._fill_free_pool(True)
                if self._free:
                    conn = self._free.popleft()
                    assert not conn.closed, conn
                    assert conn not in self._used, (conn, self._used)
                    self._used.add(conn)
                    return conn
                else:
                    yield from self._cond.wait()

    @asyncio.coroutine
    def _fill_free_pool(self, override_min):
        while self.size < self.minsize:
            self._acquiring += 1
            try:
                conn = yield from connect(echo=self._echo, loop=self._loop,
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
                conn = yield from connect(echo=self._echo, loop=self._loop,
                                          **self._conn_kwargs)
                # raise exception if pool is closing
                self._free.append(conn)
                self._cond.notify()
            finally:
                self._acquiring -= 1

    @asyncio.coroutine
    def _wakeup(self):
        with (yield from self._cond):
            self._cond.notify()

    @asyncio.coroutine
    def release(self, conn):
        """Release free connection back to the connection pool.
        """
        assert conn in self._used, (conn, self._used)
        self._used.remove(conn)
        if not conn.closed:
            if self._closing:
                yield from conn.ensure_closed()
            else:
                self._free.append(conn)
            yield from self._wakeup()

    def __await__(self):
        yield
        return _ConnectionContextManager(self)

    @asyncio.coroutine
    def __aenter__(self):
        return self

    @asyncio.coroutine
    def __aexit__(self, exc_type, exc_val, exc_tb):
        self.close()
        yield from self.wait_closed()


class _ConnectionContextManager:
    """Context manager.

    This enables the following idiom for acquiring and releasing a
    connection around a block:

        with (yield from pool) as conn:
            cur = yield from conn.cursor()

    while failing loudly when accidentally using:

        with pool:
            <block>
    """

    __slots__ = ('_pool', '_conn')

    def __init__(self, pool):
        self._pool = pool
        self._conn = None

    @asyncio.coroutine
    def __aenter__(self):
        assert not self._conn
        self._conn = yield from self._pool.acquire()
        return self._conn

    @asyncio.coroutine
    def __aexit__(self, exc_type, exc_val, exc_tb):
        try:
            yield from self._pool.release(self._conn)
        finally:
            self._pool = None
            self._conn = None
