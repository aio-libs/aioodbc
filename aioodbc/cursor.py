import asyncio


class Cursor:
    def __init__(self, impl, connection):
        self._conn = connection
        self._impl = impl
        self._loop = connection.loop
        self._executor = connection._executor

    def _execute(self, func, *args, **kwargs):
        f = self._conn._execute(func, *args, **kwargs)
        return f

    @property
    def description(self):
        return self._impl.description

    def close(self):
        """Close the cursor now."""
        self._impl.close()

    @property
    def closed(self):
        """Read-only boolean attribute: specifies if the cursor is closed."""
        return self._impl.closed

    @property
    def connection(self):
        """Read-only attribute returning a reference to the `Connection`."""
        return self._conn

    def execute(self, sql, *params):
        fut = self._execute(self._impl.execute, sql, *params)
        return fut

    @asyncio.coroutine
    def executemany(self, operation, seq_of_parameters):
        pass

    @asyncio.coroutine
    def callproc(self, procname, parameters=None, *, timeout=None):
        pass

    @asyncio.coroutine
    def fetchone(self):
        ret = self._impl.fetchone()
        return ret

    @asyncio.coroutine
    def fetchmany(self, size=None):
        if size is None:
            size = self._impl.arraysize
        ret = self._impl.fetchmany(size)
        return ret

    @asyncio.coroutine
    def fetchall(self):
        ret = self._execute(self._impl.fetchall)
        return ret

    @asyncio.coroutine
    def scroll(self, value, mode="relative"):
        ret = self._impl.scroll(value, mode)
        return ret

    @property
    def rowcount(self):
        return self._impl.rowcount

    @property
    def rownumber(self):
        return self._impl.rownumber

    @property
    def lastrowid(self):
        return self._impl.lastrowid

    @property
    def query(self):
        return self._impl.query

    @asyncio.coroutine
    def nextset(self):
        self._impl.nextset()
