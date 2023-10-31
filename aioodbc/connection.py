from __future__ import annotations

import asyncio
import sys
import traceback
import warnings
from concurrent.futures import ThreadPoolExecutor
from functools import partial
from types import TracebackType
from typing import Any, Callable, Coroutine, Optional, Type, TypeVar

import pyodbc

from .cursor import Cursor
from .utils import _ContextManager, _is_conn_close_error

__all__ = ["connect", "Connection"]


async def _close_cursor(c: Cursor) -> None:
    if not c.autocommit:
        await c.commit()
    await c.close()


async def _close_cursor_on_error(c: Cursor) -> None:
    await c.rollback()
    await c.close()


_T = TypeVar("_T")


class Connection:
    """Connection objects manage connections to the database.

    Connections should only be created by the aioodbc.connect function.
    """

    _source_traceback = None

    def __init__(
        self,
        *,
        dsn: str,
        autocommit: bool = False,
        ansi: bool = False,
        timeout: int = 0,
        executor: Optional[ThreadPoolExecutor] = None,
        echo: bool = False,
        loop: Optional[asyncio.AbstractEventLoop] = None,  # deprecated
        after_created: Optional[
            Callable[[pyodbc.Connection], Coroutine[Any, Any, Any]],
        ] = None,
        **kwargs: Any,
    ) -> None:
        if loop is not None:
            msg = "Explicit loop is deprecated, and has no effect."
            warnings.warn(msg, DeprecationWarning, stacklevel=2)
        self._executor = executor
        self._loop = asyncio.get_event_loop()
        self._conn = None

        self._timeout = timeout
        self._last_usage = self._loop.time()
        self._autocommit = autocommit
        self._ansi = ansi
        self._dsn = dsn
        self._echo = echo
        self._posthook = after_created
        self._kwargs = kwargs
        if self._loop.get_debug():
            self._source_traceback = traceback.extract_stack(sys._getframe(1))

    def _execute(
        self,
        func: Callable[..., _T],
        *args: Any,
        **kwargs: Any,
    ) -> asyncio.Future[_T]:
        # execute function with args and kwargs in thread pool
        func = partial(func, *args, **kwargs)
        future = self._loop.run_in_executor(self._executor, func)
        return future

    async def _connect(self) -> None:
        # create pyodbc connection
        f = self._execute(
            pyodbc.connect,
            self._dsn,
            autocommit=self._autocommit,
            ansi=self._ansi,
            timeout=self._timeout,
            **self._kwargs,
        )
        self._conn = await f
        if self._posthook is not None:
            await self._posthook(self._conn)

    @property
    def loop(self) -> asyncio.AbstractEventLoop:
        return self._loop

    @property
    def closed(self) -> bool:
        if self._conn:
            return False
        return True

    @property
    def autocommit(self) -> bool:
        """Show autocommit mode for current database session. True if the
        connection is in autocommit mode; False otherwise. The default
        is False
        """
        assert self._conn is not None  # mypy
        return self._conn.autocommit

    @autocommit.setter
    def autocommit(self, value: bool) -> None:
        assert self._conn is not None  # mypy
        self._conn.autocommit = value

    @property
    def timeout(self) -> int:
        assert self._conn is not None  # mypy
        return self._conn.timeout

    @property
    def last_usage(self) -> float:
        return self._last_usage

    @property
    def echo(self) -> bool:
        return self._echo

    async def _cursor(self) -> Cursor:
        assert self._conn is not None  # mypy
        c = await self._execute(self._conn.cursor)
        self._last_usage = self._loop.time()
        connection = self
        return Cursor(c, connection, echo=self._echo)

    def cursor(self) -> _ContextManager[Cursor]:
        return _ContextManager["Cursor"](
            self._cursor(), _close_cursor, _close_cursor_on_error
        )

    async def close(self) -> None:
        """Close pyodbc connection"""
        if not self._conn:
            return
        c = await self._execute(self._conn.close)
        self._conn = None
        return c

    async def commit(self) -> None:
        """Commit any pending transaction to the database."""
        assert self._conn is not None  # mypy
        await self._execute(self._conn.commit)

    async def rollback(self) -> None:
        """Causes the database to roll back to the start of any pending
        transaction.
        """
        assert self._conn is not None  # mypy
        await self._execute(self._conn.rollback)

    async def execute(self, sql: str, *args: Any) -> Cursor:
        """Create a new Cursor object, call its execute method, and return it.

        See Cursor.execute for more details.This is a convenience method
        that is not part of the DB API.  Since a new Cursor is allocated
        by each call, this should not be used if more than one SQL
        statement needs to be executed.

        :param sql: str, formatted sql statement
        :param args: tuple, arguments for construction of sql statement
        """
        assert self._conn is not None  # mypy
        try:
            _cursor = await self._execute(self._conn.execute, sql, *args)
            connection = self
            cursor = Cursor(_cursor, connection, echo=self._echo)
            return cursor
        except pyodbc.Error as e:
            if _is_conn_close_error(e):
                await self.close()
            raise

    async def getinfo(self, type_: int) -> Any:
        """Returns general information about the driver and data source
        associated with a connection by calling SQLGetInfo and returning its
        results. See Microsoft's SQLGetInfo documentation for the types of
        information available.

        :param type_: int, pyodbc.SQL_* constant
        """
        assert self._conn is not None  # mypy
        fut = self._execute(self._conn.getinfo, type_)
        return await fut

    async def add_output_converter(
        self,
        sqltype: int,
        func: Callable[[Optional[str]], Any],
    ) -> None:
        """Register an output converter function that will be called whenever
        a value with the given SQL type is read from the database.

        :param sqltype: the integer SQL type value to convert, which can
            be one of the defined standard constants (pyodbc.SQL_VARCHAR)
            or a database-specific value (e.g. -151 for the SQL Server 2008
            geometry data type).
        :param func: the converter function which will be called with a
            single parameter, the value, and should return the converted
            value. If the value is NULL, the parameter will be None.
            Otherwise it will be a Python string.
        """
        assert self._conn is not None  # mypy
        fut = self._execute(self._conn.add_output_converter, sqltype, func)
        return await fut

    async def clear_output_converters(self) -> None:
        """Remove all output converter functions added by
        add_output_converter.
        """
        assert self._conn is not None  # mypy
        fut = self._execute(self._conn.clear_output_converters)
        return await fut

    async def set_attr(self, attr_id: int, value: int) -> None:
        """Calls SQLSetConnectAttr with the given values.

        :param attr_id: the attribute ID (integer) to set. These are ODBC or
            driver constants.
        :parm value: the connection attribute value to set. At this time
            only integer values are supported.
        """
        assert self._conn is not None  # mypy
        fut = self._execute(self._conn.set_attr, attr_id, value)
        return await fut

    def __del__(self) -> None:
        if not self.closed:
            # This will block the loop, please use close
            # coroutine to close connection
            assert self._conn is not None  # mypy
            self._conn.close()
            self._conn = None

            warnings.warn(
                f"Unclosed connection {self!r}", ResourceWarning, stacklevel=1
            )

            context = {"connection": self, "message": "Unclosed connection"}
            if self._source_traceback is not None:
                context["source_traceback"] = self._source_traceback
            self._loop.call_exception_handler(context)

    async def __aenter__(self) -> Connection:
        return self

    async def __aexit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc: Optional[BaseException],
        tb: Optional[TracebackType],
    ) -> None:
        await self.close()
        return


async def _disconnect(c: Connection) -> None:
    if not c.autocommit:
        await c.commit()
    await c.close()


async def _disconnect_on_error(c: Connection) -> None:
    await c.rollback()
    await c.close()


async def _connect(
    *,
    dsn: str,
    autocommit: bool = False,
    ansi: bool = False,
    timeout: int = 0,
    executor: Optional[ThreadPoolExecutor] = None,
    echo: bool = False,
    after_created: Optional[
        Callable[[pyodbc.Connection], Coroutine[Any, Any, Any]],
    ] = None,
    **kwargs: Any,
) -> Connection:
    conn = Connection(
        dsn=dsn,
        autocommit=autocommit,
        ansi=ansi,
        timeout=timeout,
        echo=echo,
        loop=None,  # deprecated
        executor=executor,
        after_created=after_created,
        **kwargs,
    )
    await conn._connect()
    return conn


def connect(
    *,
    dsn: str,
    autocommit: bool = False,
    ansi: bool = False,
    timeout: int = 0,
    loop: Optional[asyncio.AbstractEventLoop] = None,
    executor: Optional[ThreadPoolExecutor] = None,
    echo: bool = False,
    after_created: Optional[
        Callable[[pyodbc.Connection], Coroutine[Any, Any, Any]],
    ] = None,
    **kwargs: Any,
) -> _ContextManager[Connection]:
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
    :param after_created callable: support customize configuration after
        connection is connected.  Must be an async unary function, or leave it
        as None.
    """
    if loop is not None:
        msg = "Explicit loop is deprecated, and has no effect."
        warnings.warn(msg, DeprecationWarning, stacklevel=2)
    return _ContextManager(
        _connect(
            dsn=dsn,
            autocommit=autocommit,
            ansi=ansi,
            timeout=timeout,
            executor=executor,
            echo=echo,
            after_created=after_created,
            **kwargs,
        ),
        _disconnect,
        _disconnect_on_error,
    )
