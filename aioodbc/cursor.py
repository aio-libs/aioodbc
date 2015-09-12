import asyncio
from pyodbc import OperationalError


from .log import logger


class Cursor:
    def __init__(self, pyodbc_cursor, connection, echo=False):
        self._conn = connection
        self._impl = pyodbc_cursor
        self._loop = connection.loop
        self._echo = echo

    def _run_operation(self, func, *args, **kwargs):
        if not self._conn:
            raise OperationalError('Cursor is closed.')
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

    @asyncio.coroutine
    def close(self):
        if self._conn is None:
            return
        yield from self._run_operation(self._impl.close)
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

    @asyncio.coroutine
    def setinputsizes(self, *args, **kwargs):
        return None

    @asyncio.coroutine
    def setoutputsize(self, *args, **kwargs):
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

    def columns(self, table=None, catalog=None, schema=None, column=None):
        fut = self._run_operation(self._impl.columns, table=table,
                                  catalog=catalog, schema=schema,
                                  column=column)
        return fut

    @asyncio.coroutine
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

    # TODO: check source code of pyodbc regadring table argument
    def foreignKeys(self, table=None, catalog=None, schema=None,
                    foreignTable=None, foreignCatalog=None,
                    foreignSchema=None):
        fut = self._run_operation(self._impl.foreignKeys, table=table,
                                  catalog=catalog, schema=schema,
                                  foreignTable=foreignTable,
                                  foreignCatalog=foreignCatalog,
                                  foreignSchema=foreignSchema)
        return fut

    def getTypeInfo(self, sqlType=None):
        fut = self._run_operation(self._impl.getTypeInfo, sqlType)
        return fut

    def procedures(self, procedure=None, catalog=None, schema=None):
        fut = self._run_operation(self._impl.procedures, procedure=procedure,
                                  catalog=catalog, schema=schema)
        return fut

    def procedureColumns(self, procedure=None, catalog=None, schema=None):
        fut = self._run_operation(self._impl.procedureColumns,
                                  procedure=procedure, catalog=catalog,
                                  schema=schema)
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

    @asyncio.coroutine
    def __aiter__(self):
        return self

    @asyncio.coroutine
    def __anext__(self):
        ret = yield from self.fetchone()
        if ret is not None:
            return ret
        else:
            raise StopAsyncIteration

    @asyncio.coroutine
    def __aenter__(self):
        return self

    @asyncio.coroutine
    def __aexit__(self, exc_type, exc_val, exc_tb):
        yield from self.close()
        return
