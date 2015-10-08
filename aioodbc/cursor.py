import pyodbc
from .log import logger


__all__ = ['Cursor']


class Cursor:

    def __init__(self, pyodbc_cursor, connection, echo=False):
        self._conn = connection
        self._impl = pyodbc_cursor
        self._loop = connection.loop
        self._echo = echo

    def _run_operation(self, func, *args, **kwargs):
        if not self._conn:
            raise pyodbc.OperationalError('Cursor is closed.')
        future = self._conn._execute(func, *args, **kwargs)
        return future

    @property
    def echo(self):
        """Return echo mode status."""
        return self._echo

    @property
    def connection(self):
        return self._conn

    @property
    def rowcount(self):
        return self._impl.rowcount

    @property
    def description(self):
        return self._impl.description

    @property
    def closed(self):
        return self._conn is None

    @property
    def arraysize(self):
        return self._impl.arraysize

    @arraysize.setter
    def arraysize(self, size):
        self._impl.arraysize = size

    async def close(self):
        if self._conn is None:
            return
        await self._run_operation(self._impl.close)
        self._conn = None

    def execute(self, sql, *params):
        if self._echo:
            logger.info(sql)
            logger.info("%r", sql)
        fut = self._run_operation(self._impl.execute, sql, *params)
        return fut

    def executemany(self, sql, *params):
        fut = self._run_operation(self._impl.executemany, sql, *params)
        return fut

    def callproc(self, procname, args=()):
        raise NotImplementedError

    async def setinputsizes(self, *args, **kwargs):
        return None

    async def setoutputsize(self, *args, **kwargs):
        return None

    def fetchone(self):
        fut = self._run_operation(self._impl.fetchone)
        return fut

    def fetchall(self):
        fut = self._run_operation(self._impl.fetchall)
        return fut

    def fetchmany(self, size):
        fut = self._run_operation(self._impl.fetchmany, size)
        return fut

    def nextset(self):
        fut = self._run_operation(self._impl.nextset)
        return fut

    def tables(self, **kw):
        fut = self._run_operation(self._impl.tables, **kw)
        return fut

    def columns(self, **kw):
        fut = self._run_operation(self._impl.columns, **kw)
        return fut

    def statistics(self, catalog=None, schema=None, unique=False, quick=True):
        fut = self._run_operation(self._impl.statistics, catalog=catalog,
                                  schema=schema, unique=unique, quick=quick)
        return fut

    def rowIdColumns(self, table, catalog=None, schema=None, nullable=True):
        fut = self._run_operation(self._impl.rowIdColumns, table,
                                  catalog=catalog, schema=schema,
                                  nullable=nullable)
        return fut

    def rowVerColumns(self, table, catalog=None, schema=None, nullable=True):
        fut = self._run_operation(self._impl.rowVerColumns, table,
                                  catalog=catalog, schema=schema,
                                  nullable=nullable)
        return fut

    def primaryKeys(self, table, catalog=None, schema=None):
        fut = self._run_operation(self._impl.primaryKeys, table,
                                  catalog=catalog, schema=schema)
        return fut

    def foreignKeys(self, *a, **kw):
        fut = self._run_operation(self._impl.foreignKeys, *a, **kw)
        return fut

    def getTypeInfo(self, sql_type):
        fut = self._run_operation(self._impl.getTypeInfo, sql_type)
        return fut

    def procedures(self, *a, **kw):
        fut = self._run_operation(self._impl.procedures, *a, **kw)
        return fut

    def procedureColumns(self, *a, **kw):
        fut = self._run_operation(self._impl.procedureColumns, *a, **kw)
        return fut

    def skip(self, count):
        fut = self._run_operation(self._impl.skip, count)
        return fut

    def commit(self):
        fut = self._run_operation(self._impl.commit)
        return fut

    def rollback(self):
        fut = self._run_operation(self._impl.rollback)
        return fut

    async def __aiter__(self):
        return self

    async def __anext__(self):
        ret = await self.fetchone()
        if ret is not None:
            return ret
        else:
            # This exception is not available in python < 3.5,
            raise StopAsyncIteration

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.close()
        return
