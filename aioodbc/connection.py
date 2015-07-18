import sys
import pyodbc
import asyncio
import traceback
from functools import partial
import warnings
from .cursor import Cursor


__all__ = ['connect', 'Connection']
PY_341 = sys.version_info >= (3, 4, 1)


def connect(connectionstring, loop=None, executor=None, **kwargs):
    loop = loop or asyncio.get_event_loop()
    conn = Connection(connectionstring, loop=loop, executor=executor,
                      **kwargs)
    yield from conn._connect()
    return conn


class Connection:
    _source_traceback = None

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
        if loop.get_debug():
            self._source_traceback = traceback.extract_stack(sys._getframe(1))

    def _execute(self, func, *args, **kwargs):
        func = partial(func, *args, **kwargs)
        future = self._loop.run_in_executor(self._executor, func)
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
    def closed(self):
        if self._conn:
            return False
        return True

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
        if not self._conn:
            return
        c = yield from self._execute(self._conn.close)
        self._conn = None
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

    if PY_341:  # pragma: no branch
        def __del__(self):
            if not self.closed:
                # TODO: is any other way to close connection
                asyncio.Task(self.close(), loop=self._loop)
                warnings.warn("Unclosed connection {!r}".format(self),
                              ResourceWarning)

                context = {'connection': self,
                           'message': 'Unclosed connection'}
                if self._source_traceback is not None:
                    context['source_traceback'] = self._source_traceback
                self._loop.call_exception_handler(context)
