import asyncio
import sys
import traceback
import warnings
from functools import partial

import pyodbc
from .cursor import Cursor
from .utils import _ContextManager


__all__ = ['connect', 'Connection']
PY_341 = sys.version_info >= (3, 4, 1)


def connect(*, dsn, autocommit=False, ansi=False, timeout=0, loop=None,
            executor=None, echo=False, **kwargs):
    """Accepts an ODBC connection string and returns a new Connection object.

    The connection string can be passed as the string `str`, as a list of
    keywords,or a combination of the two.  Any keywords except autocommit,
    ansi, and timeout are simply added to the connection string.

    :param autocommit bool: False or zero, the default, if True or non-zero,
        the connection is put into ODBC autocommit mode and statements are
        committed automatically.
    :param ansi bool: By default, pyodbc first attempts to connect using
        the Unicode version of SQLDriverConnectW. If the driver returns IM001
        indicating it does not support the Unicode version, the ANSI version
        is tried.
    :param timeout int: An integer login timeout in seconds, used to set
        the SQL_ATTR_LOGIN_TIMEOUT attribute of the connection. The default is
         0  which means the database's default timeout, if any, is use
    """
    return _ContextManager(_connect(dsn=dsn, autocommit=autocommit,
                           ansi=ansi, timeout=timeout, loop=loop,
                           executor=executor, echo=echo, **kwargs))


async def _connect(*, dsn, autocommit=False, ansi=False, timeout=0, loop=None,
                   executor=None, echo=False, **kwargs):
    loop = loop or asyncio.get_event_loop()
    conn = Connection(dsn=dsn, autocommit=autocommit, ansi=ansi,
                      timeout=timeout, echo=echo, loop=loop, executor=executor,
                      **kwargs)
    await conn._connect()
    return conn


class Connection:
    _source_traceback = None

    def __init__(self, *, dsn, autocommit=False, ansi=None,
                 timeout=0, executor=None, echo=False, loop=None, **kwargs):
        self._executor = executor
        self._loop = loop or asyncio.get_event_loop()
        self._conn = None

        self._timeout = timeout
        self._autocommit = autocommit
        self._ansi = ansi
        self._dsn = dsn
        self._echo = echo
        self._kwargs = kwargs
        if loop.get_debug():
            self._source_traceback = traceback.extract_stack(sys._getframe(1))

    def _execute(self, func, *args, **kwargs):
        func = partial(func, *args, **kwargs)
        future = self._loop.run_in_executor(self._executor, func)
        return future

    async def _connect(self):
        f = self._execute(pyodbc.connect, self._dsn,
                          autocommit=self._autocommit, ansi=self._ansi,
                          timeout=self._timeout,
                          **self._kwargs)
        self._conn = await f

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

    @property
    def echo(self):
        return self._echo

    async def _cursor(self):
        c = await self._execute(self._conn.cursor)
        connection = self
        return Cursor(c, connection)

    def cursor(self):
        return _ContextManager(self._cursor())

    async def close(self):
        if not self._conn:
            return
        c = await self._execute(self._conn.close)
        self._conn = None
        return c

    def commit(self):
        fut = self._execute(self._conn.commit)
        return fut

    def rollback(self):
        fut = self._execute(self._conn.rollback)
        return fut

    async def execute(self, sql, *args):
        _cursor = await self._execute(self._conn.execute, sql, *args)
        connection = self
        cursor = Cursor(_cursor, connection)
        return cursor

    def getinfo(self, type_):
        fut = self._execute(self._conn.getinfo, type_)
        return fut

    def add_output_converter(self, sqltype, func):
        fut = self._execute(self._conn.add_output_converter, sqltype, func)
        return fut

    def clear_output_converters(self):
        fut = self._execute(self._conn.clear_output_converters)
        return fut

    def __del__(self):
        if not self.closed:
            # This will block the loop, please use close
            # coroutine to close connection
            self._conn.close()
            self._conn = None

            warnings.warn("Unclosed connection {!r}".format(self),
                          ResourceWarning)

            context = {'connection': self,
                       'message': 'Unclosed connection'}
            if self._source_traceback is not None:
                context['source_traceback'] = self._source_traceback
            self._loop.call_exception_handler(context)

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.close()
        return
