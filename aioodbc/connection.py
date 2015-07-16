import pyodbc
import asyncio
from functools import partial
from .cursor import Cursor

__all__ = ['connect', 'Connection']


def connect(connectionstring, loop=None, executor=None, **kwargs):
    loop = loop or asyncio.get_event_loop()
    conn = Connection(connectionstring, loop=loop, executor=executor,
                      **kwargs)
    yield from conn._connect()
    return conn


class Connection:
    def __init__(self, connectionstring, autocommit=False, ansi=None,
                 timeout=0, executor=None, loop=None, **kwargs):
        self._executor = executor
        self._loop = loop or asyncio.get_event_loop()
        self._conn = None

        self._timeout = timeout
        self._autocommit = autocommit
        self._ansi = ansi

        self._connectionstring = connectionstring
        self._kwargs = kwargs

    def _execute(self, func, *args, **kwargs):
        func = partial(func, **kwargs)
        future = self._loop.run_in_executor(self._executor, func, *args)
        return future

    @asyncio.coroutine
    def _connect(self):
        f = self._execute(pyodbc.connect, self._connectionstring,
                          **self._kwargs)
        self._conn = yield from f

    @property
    def loop(self):
        return self._loop

    @property
    def autocommit(self):
        return self._conn.autocommit

    @property
    def timeout(self):
        return self._conn.timeout

    @asyncio.coroutine
    def cursor(self):
        c = yield from self._execute(self._conn.cursor)
        connection = self
        return Cursor(c, connection)

    @asyncio.coroutine
    def close(self):
        c = yield from self._execute(self._conn.close)
        return c

    @asyncio.coroutine
    def commit(self):
        c = yield from self._execute(self._conn.commit)
        return c

    @asyncio.coroutine
    def rollback(self):
        c = yield from self._execute(self._conn.rollback)
        return c

    @asyncio.coroutine
    def execute(self, sql, *args):
        c = yield from self._execute(self._conn.execute, sql, *args)
        return c

    @asyncio.coroutine
    def getinfo(self):
        c = yield from self._execute(self._conn.rollback)
        return c

    @asyncio.coroutine
    def add_output_converter(self):
        c = yield from self._execute(self._conn.rollback)
        return c

    @asyncio.coroutine
    def clear_output_converters(self):
        c = yield from self._execute(self._conn.rollback)
        return c
