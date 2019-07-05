import pyodbc
from .log import logger
from .utils import PY_352, _is_conn_close_error


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

    async def _run_operation(self, func, *args, **kwargs):
        # execute func in thread pool of attached to cursor connection
        if not self._conn:
            raise pyodbc.OperationalError('Cursor is closed.')

        try:
            result = await self._conn._execute(func, *args, **kwargs)
            return result
        except pyodbc.Error as e:
            if self._conn and _is_conn_close_error(e):
                await self._conn.close()
            raise

    @property
    def echo(self):
        """Return echo mode status."""
        return self._echo

    @property
    def connection(self):
        """Cursors database connection"""
        return self._conn

    @property
    def autocommit(self):
        """Show autocommit mode for current database session. True if
        connection is in autocommit mode; False otherwse. The default
        is False.
        """
        return self._conn.autocommit

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

    async def execute(self, sql, *params):
        """Executes the given operation substituting any markers with
        the given parameters.

        :param sql: the SQL statement to execute with optional ? parameter
            markers. Note that pyodbc never modifies the SQL statement.
        :param params: optional parameters for the markers in the SQL. They
            can be passed in a single sequence as defined by the DB API.
            For convenience, however, they can also be passed individually
        """
        if self._echo:
            logger.info(sql)
            logger.info("%r", sql)

        await self._run_operation(self._impl.execute, sql, *params)
        return self

    def executemany(self, sql, *params):
        """Prepare a database query or command and then execute it against
        all parameter sequences  found in the sequence seq_of_params.

        :param sql: the SQL statement to execute with optional ? parameters
        :param params: sequence parameters for the markers in the SQL.
        """
        fut = self._run_operation(self._impl.executemany, sql, *params)
        return fut

    def callproc(self, procname, args=()):
        raise NotImplementedError

    async def setinputsizes(self, *args, **kwargs):
        """Does nothing, required by DB API."""
        return None

    async def setoutputsize(self, *args, **kwargs):
        """Does nothing, required by DB API."""
        return None

    def fetchone(self):
        """Returns the next row or None when no more data is available.

        A ProgrammingError exception is raised if no SQL has been executed
        or if it did not return a result set (e.g. was not a SELECT
        statement).
        """
        fut = self._run_operation(self._impl.fetchone)
        return fut

    def fetchall(self):
        """Returns a list of all remaining rows.

        Since this reads all rows into memory, it should not be used if
        there are a lot of rows. Consider iterating over the rows instead.
        However, it is useful for freeing up a Cursor so you can perform a
        second query before processing the resulting rows.

        A ProgrammingError exception is raised if no SQL has been executed
        or if it did not return a result set (e.g. was not a SELECT statement)
        """
        fut = self._run_operation(self._impl.fetchall)
        return fut

    def fetchmany(self, size):
        """Returns a list of remaining rows, containing no more than size
        rows, used to process results in chunks. The list will be empty when
        there are no more rows.

        The default for cursor.arraysize is 1 which is no different than
        calling fetchone().

        A ProgrammingError exception is raised if no SQL has been executed
        or if it did not return a result set (e.g. was not a SELECT
        statement).

        :param size: int, max number of rows to return
        """
        fut = self._run_operation(self._impl.fetchmany, size)
        return fut

    def nextset(self):
        """This method will make the cursor skip to the next available
        set, discarding any remaining rows from the current set.

        If there are no more sets, the method returns None. Otherwise,
        it returns a true value and subsequent calls to the fetch methods
        will return rows from the next result set.

        This method is primarily used if you have stored procedures that
        return multiple results.
        """
        fut = self._run_operation(self._impl.nextset)
        return fut

    def tables(self, **kw):
        """Creates a result set of tables in the database that match the
        given criteria.

        :param table: the table tname
        :param catalog: the catalog name
        :param schema: the schmea name
        :param tableType: one of TABLE, VIEW, SYSTEM TABLE ...
        """
        fut = self._run_operation(self._impl.tables, **kw)
        return fut

    def columns(self, **kw):
        """Creates a results set of column names in specified tables by
        executing the ODBC SQLColumns function. Each row fetched has the
        following columns.

        :param table: the table tname
        :param catalog: the catalog name
        :param schema: the schmea name
        :param column: string search pattern for column names.
        """
        fut = self._run_operation(self._impl.columns, **kw)
        return fut

    def statistics(self, catalog=None, schema=None, unique=False, quick=True):
        """Creates a results set of statistics about a single table and
        the indexes associated with the table by executing SQLStatistics.

        :param catalog: the catalog name
        :param schema: the schmea name
        :param unique: if True, only unique indexes are retured. Otherwise
            all indexes are returned.
        :param quick: if True, CARDINALITY and PAGES are returned  only if
            they are readily available from the server
        """
        fut = self._run_operation(self._impl.statistics, catalog=catalog,
                                  schema=schema, unique=unique, quick=quick)
        return fut

    def rowIdColumns(self, table, catalog=None, schema=None,  # nopep8
                     nullable=True):
        """Executes SQLSpecialColumns with SQL_BEST_ROWID which creates a
        result set of columns that uniquely identify a row
        """
        fut = self._run_operation(self._impl.rowIdColumns, table,
                                  catalog=catalog, schema=schema,
                                  nullable=nullable)
        return fut

    def rowVerColumns(self, table, catalog=None, schema=None,  # nopep8
                      nullable=True):
        """Executes SQLSpecialColumns with SQL_ROWVER which creates a
        result set of columns that are automatically updated when any
        value in the row is updated.
        """
        fut = self._run_operation(self._impl.rowVerColumns, table,
                                  catalog=catalog, schema=schema,
                                  nullable=nullable)
        return fut

    def primaryKeys(self, table, catalog=None, schema=None):  # nopep8
        """Creates a result set of column names that make up the primary key
        for a table by executing the SQLPrimaryKeys function."""
        fut = self._run_operation(self._impl.primaryKeys, table,
                                  catalog=catalog, schema=schema)
        return fut

    def foreignKeys(self, *a, **kw):  # nopep8
        """Executes the SQLForeignKeys function and creates a result set
        of column names that are foreign keys in the specified table (columns
        in the specified table that refer to primary keys in other tables)
        or foreign keys in other tables that refer to the primary key in
        the specified table.
        """
        fut = self._run_operation(self._impl.foreignKeys, *a, **kw)
        return fut

    def getTypeInfo(self, sql_type):  # nopep8
        """Executes SQLGetTypeInfo a creates a result set with information
        about the specified data type or all data types supported by the
        ODBC driver if not specified.
        """
        fut = self._run_operation(self._impl.getTypeInfo, sql_type)
        return fut

    def procedures(self, *a, **kw):
        """Executes SQLProcedures and creates a result set of information
        about the procedures in the data source.
        """
        fut = self._run_operation(self._impl.procedures, *a, **kw)
        return fut

    def procedureColumns(self, *a, **kw):  # nopep8
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

    if PY_352:
        def __aiter__(self):
            return self
    else:
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
