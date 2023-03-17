import gc
from unittest import mock

import pyodbc
import pytest

import aioodbc


@pytest.mark.asyncio
async def test_connect(conn):
    assert not conn.autocommit
    assert conn.timeout == 0
    assert not conn.closed


@pytest.mark.parametrize("db", pytest.db_list)
@pytest.mark.asyncio
async def test_connect_hook(connection_maker):
    raw_conn = None

    async def hook(conn):
        nonlocal raw_conn
        raw_conn = conn

    connection = await connection_maker(after_created=hook)
    assert connection._conn == raw_conn


@pytest.mark.parametrize("db", pytest.db_list)
@pytest.mark.asyncio
async def test_basic_cursor(conn):
    cursor = await conn.cursor()
    sql = "SELECT 10;"
    await cursor.execute(sql)
    (resp,) = await cursor.fetchone()
    assert resp == 10


@pytest.mark.parametrize("db", pytest.db_list)
@pytest.mark.asyncio
async def test_close_twice(conn):
    await conn.close()
    await conn.close()
    assert conn.closed


@pytest.mark.parametrize("db", pytest.db_list)
@pytest.mark.asyncio
async def test_execute(conn):
    cur = await conn.execute("SELECT 10;")
    (resp,) = await cur.fetchone()
    await conn.close()
    assert resp == 10
    assert conn.closed


@pytest.mark.parametrize("db", pytest.db_list)
@pytest.mark.asyncio
async def test_getinfo(conn):
    data = await conn.getinfo(pyodbc.SQL_CREATE_TABLE)
    pg = 14057
    sqlite = 1793
    mysql = 3093
    assert data in (pg, sqlite, mysql)


@pytest.mark.parametrize("db", ["sqlite"])
@pytest.mark.asyncio
async def test_output_conversion(conn, table):
    def convert(value):
        # value will be a string.  We'll simply add an X at the
        # beginning at the end.
        if isinstance(value, str):
            return "X" + value + "X"
        return b"X" + value + b"X"

    await conn.add_output_converter(pyodbc.SQL_VARCHAR, convert)
    cur = await conn.cursor()

    await cur.execute("INSERT INTO t1 VALUES (3, '123.45')")
    await cur.execute("SELECT v FROM t1 WHERE n=3;")
    (value,) = await cur.fetchone()

    assert value in (b"X123.45X", "X123.45X")

    # Now clear the conversions and try again. There should be
    # no Xs this time.
    await conn.clear_output_converters()
    await cur.execute("SELECT v FROM t1")
    (value,) = await cur.fetchone()
    assert value == "123.45"
    await cur.close()


@pytest.mark.parametrize("db", pytest.db_list)
@pytest.mark.asyncio
async def test_autocommit(connection_maker):
    conn = await connection_maker(autocommit=True)
    assert conn.autocommit, True


@pytest.mark.parametrize("db", pytest.db_list)
@pytest.mark.asyncio
async def test_rollback(conn):
    assert not conn.autocommit

    cur = await conn.cursor()
    await cur.execute("CREATE TABLE t1(n INT, v VARCHAR(10));")

    await conn.commit()

    await cur.execute("INSERT INTO t1 VALUES (1, '123.45');")
    await cur.execute("SELECT v FROM t1")
    (value,) = await cur.fetchone()
    assert value == "123.45"

    await conn.rollback()
    await cur.execute("SELECT v FROM t1;")
    value = await cur.fetchone()
    assert value is None
    await cur.execute("DROP TABLE t1;")
    await conn.commit()

    await conn.close()


@pytest.mark.parametrize("db", pytest.db_list)
@pytest.mark.asyncio
async def test_custom_executor(dsn, executor):
    conn = await aioodbc.connect(dsn=dsn, executor=executor)
    assert conn._executor is executor
    cur = await conn.execute("SELECT 10;")
    (resp,) = await cur.fetchone()
    await conn.close()
    assert resp == 10
    assert conn.closed


@pytest.mark.asyncio
async def test_dataSources(executor):
    data = await aioodbc.dataSources(executor)
    assert isinstance(data, dict)


@pytest.mark.parametrize("db", pytest.db_list)
@pytest.mark.asyncio
async def test_connection_simple_with(conn):
    assert not conn.closed
    async with conn:
        pass

    assert conn.closed


@pytest.mark.parametrize("db", pytest.db_list)
@pytest.mark.asyncio
async def test_connect_context_manager(dsn):
    async with aioodbc.connect(dsn=dsn, echo=True) as conn:
        assert not conn.closed
        assert conn.echo

        cur = await conn.execute("SELECT 10;")
        assert cur.echo
        (resp,) = await cur.fetchone()
        assert resp == 10
        await cur.close()

    assert conn.closed


@pytest.mark.parametrize("db", pytest.db_list)
@pytest.mark.asyncio
async def test___del__(loop, dsn, recwarn, executor):
    conn = await aioodbc.connect(dsn=dsn, executor=executor)
    exc_handler = mock.Mock()
    loop.set_exception_handler(exc_handler)

    del conn
    gc.collect()
    w = recwarn.pop()
    assert issubclass(w.category, ResourceWarning)

    msg = {
        "connection": mock.ANY,  # conn was deleted
        "message": "Unclosed connection",
    }
    exc_handler.assert_called_with(loop, msg)
