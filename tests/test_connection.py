import asyncio
import gc
import sys
from unittest import mock

import pytest
import pyodbc

import aioodbc


PY_341 = sys.version_info >= (3, 4, 1)


def test_connect(loop, conn):
    assert conn.loop is loop
    assert not conn.autocommit
    assert conn.timeout == 0
    assert not conn.closed


@pytest.mark.parametrize('dsn', pytest.dsn_list)
@pytest.mark.run_loop
async def test_basic_cursor(conn):
    cursor = await conn.cursor()
    sql = 'SELECT 10;'
    await cursor.execute(sql)
    (resp,) = await cursor.fetchone()
    assert resp == 10


@pytest.mark.parametrize('dsn', pytest.dsn_list)
@pytest.mark.run_loop
async def test_default_event_loop(loop, dsn):
    asyncio.set_event_loop(loop)
    conn = await aioodbc.connect(dsn=dsn)
    assert conn._loop is loop
    await conn.close()


@pytest.mark.parametrize('dsn', pytest.dsn_list)
@pytest.mark.run_loop
async def test_close_twice(conn):
    await conn.close()
    await conn.close()
    assert conn.closed


@pytest.mark.parametrize('dsn', pytest.dsn_list)
@pytest.mark.run_loop
async def test_execute(conn):
    cur = await conn.execute('SELECT 10;')
    (resp,) = await cur.fetchone()
    await conn.close()
    assert resp == 10
    assert conn.closed


@pytest.mark.parametrize('dsn', pytest.dsn_list)
@pytest.mark.run_loop
async def test_getinfo(conn):
    data = await conn.getinfo(pyodbc.SQL_CREATE_TABLE)
    pg = 14057
    sqlite = 1793
    mysql = 3093
    assert data in (pg, sqlite, mysql)


@pytest.mark.parametrize('dsn', [pytest.sqlite])
@pytest.mark.run_loop
async def test_output_conversion(conn, table):
    def convert(value):
        # `value` will be a string.  We'll simply add an X at the
        # beginning at the end.
        return 'X' + value + 'X'

    await conn.add_output_converter(pyodbc.SQL_VARCHAR, convert)
    cur = await conn.cursor()

    await cur.execute("INSERT INTO t1 VALUES (3, '123.45')")
    await cur.execute("SELECT v FROM t1 WHERE n=3;")
    (value,) = await cur.fetchone()

    assert value == 'X123.45X'

    # Now clear the conversions and try again. There should be
    # no Xs this time.
    await conn.clear_output_converters()
    await cur.execute("SELECT v FROM t1")
    (value,) = await cur.fetchone()
    assert value == '123.45'
    await cur.close()


@pytest.mark.parametrize('dsn', pytest.dsn_list)
async def test_autocommit(loop, connection_maker):
    conn = connection_maker(loop, autocommit=True)
    assert conn.autocommit, True


@pytest.mark.parametrize('dsn', pytest.dsn_list)
@pytest.mark.run_loop
async def test_rollback(conn):
    assert not conn.autocommit

    cur = await conn.cursor()
    await cur.execute("CREATE TABLE t1(n INT, v VARCHAR(10));")

    await conn.commit()

    await cur.execute("INSERT INTO t1 VALUES (1, '123.45');")
    await cur.execute("SELECT v FROM t1")
    (value,) = await cur.fetchone()
    assert value == '123.45'

    await conn.rollback()
    await cur.execute("SELECT v FROM t1;")
    value = await cur.fetchone()
    assert value is None
    await cur.execute("DROP TABLE t1;")
    await conn.commit()

    await conn.close()


@pytest.mark.skipif(not PY_341, reason="Python 3.3 doesnt support __del__ "
                                       "calls from GC")
@pytest.mark.parametrize('dsn', pytest.dsn_list)
@pytest.mark.run_loop
async def test___del__(loop, dsn, recwarn):
    conn = await aioodbc.connect(dsn=dsn, loop=loop)
    exc_handler = mock.Mock()
    loop.set_exception_handler(exc_handler)

    del conn
    gc.collect()
    w = recwarn.pop()
    assert issubclass(w.category, ResourceWarning)

    msg = {'connection': mock.ANY,  # conn was deleted
           'message': 'Unclosed connection'}
    if loop.get_debug():
        msg['source_traceback'] = mock.ANY
    exc_handler.assert_called_with(loop, msg)


@pytest.mark.parametrize('dsn', pytest.dsn_list)
@pytest.mark.run_loop
async def test_custom_executor(loop, dsn, executor):
    conn = await aioodbc.connect(dsn=dsn, executor=executor, loop=loop)
    assert conn._executor is executor
    cur = await conn.execute('SELECT 10;')
    (resp,) = await cur.fetchone()
    await conn.close()
    assert resp == 10
    assert conn.closed


@pytest.mark.run_loop
async def test_dataSources(loop, executor):
    data = await aioodbc.dataSources(loop, executor)
    assert isinstance(data, dict)


@pytest.mark.parametrize('dsn', pytest.dsn_list)
@pytest.mark.run_loop
async def test_connection_simple_with(loop, conn):
    assert not conn.closed
    async with conn:
        pass

    assert conn.closed


@pytest.mark.parametrize('dsn', pytest.dsn_list)
@pytest.mark.run_loop
async def test_connect_context_manager(loop, dsn):
    async with aioodbc.connect(dsn=dsn, loop=loop) as conn:
        assert not conn.closed
    assert conn.closed
