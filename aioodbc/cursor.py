import pyodbc
from .log import logger


__all__ = ['Cursor']


class Cursor:
    """Cursors represent a database cursor (and map to ODBC HSTMTs), which
    is used to manage the context of a fetch operation.

    Cursors created from the same connection are not isolated, i.e., any
    changes made to the database by a cursor are immediately visible by
    the other cursors.
    """

    def __init__(self, pyodbc_cursor, connection, echo=False):
        self._conn = connection
        self._impl = pyodbc_cursor
        self._loop = connection.loop
        self._echo = echo

    def _run_operation(self, func, *args, **kwargs):
        # execute func in thread pool of attached to cursor connection
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
        """Cursors database connection"""
        return self._conn

    @property
    def rowcount(self):
        """The number of rows modified by the previous DDL statement.

        This is -1 if no SQL has been executed or if the number of rows is
        unknown. Note that it is not uncommon for databases to report -1
        after a select statement for performance reasons. (The exact number
        may not be known before the first records are returned to the
        application.)
        """
        return self._impl.rowcount

    @property
    def description(self):
        """This read-only attribute is a list of 7-item tuples, each
        containing (name, type_code, display_size, internal_size, precision,
        scale, null_ok).

        pyodbc only provides values for name, type_code, internal_size,
        and null_ok. The other values are set to None.

        This attribute will be None for operations that do not return rows
        or if one of the execute methods has not been called.

        The type_code member is the class type used to create the Python
        objects when reading rows. For example, a varchar column's type will
        be str.
        """
        return self._impl.description

    @property
    def closed(self):
        """Read only property indicates if cursor has been closed"""
        return self._conn is None

    @property
    def arraysize(self):
        """This read/write attribute specifies the number of rows to fetch
        at a time with .fetchmany() . It defaults to 1 meaning to fetch a
        single row at a time.
        """
        return self._impl.arraysize

    @arraysize.setter
    def arraysize(self, size):
        self._impl.arraysize = size

    async def close(self):
        """Close the cursor now (rather than whenever __del__ is called).

        The cursor will be unusable from this point forward; an Error
        (or subclass) exception will be raised if any operation is attempted
        with the cursor.
        """
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
            raise StopAsyncIteration

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.close()
        return
