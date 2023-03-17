import asyncio
import time

import pytest
from pyodbc import Error

import aioodbc
from aioodbc import Connection, Pool


@pytest.mark.asyncio
async def test_create_pool(pool_maker, dsn):
    pool = await pool_maker(dsn=dsn)
    assert isinstance(pool, Pool)
    assert 10 == pool.minsize
    assert 10 == pool.maxsize
    assert 10 == pool.size
    assert 10 == pool.freesize
    assert not pool.echo


@pytest.mark.asyncio
async def test_create_pool2(pool_maker, dsn):
    pool = await pool_maker(dsn=dsn, maxsize=20)
    assert isinstance(pool, Pool)
    assert 10 == pool.minsize
    assert 20 == pool.maxsize
    assert 10 == pool.size
    assert 10 == pool.freesize


@pytest.mark.parametrize("db", pytest.db_list)
@pytest.mark.asyncio
async def test_acquire(pool):
    conn = await pool.acquire()
    try:
        assert isinstance(conn, Connection)
        assert not conn.closed
        cur = await conn.cursor()
        await cur.execute("SELECT 1")
        val = await cur.fetchone()
        assert (1,) == tuple(val)
    finally:
        await pool.release(conn)


@pytest.mark.asyncio
async def test_release(pool):
    conn = await pool.acquire()
    try:
        assert 9 == pool.freesize
        assert {conn} == pool._used
    finally:
        await pool.release(conn)
    assert 10 == pool.freesize
    assert not pool._used


@pytest.mark.skip(reason="PG fixture needs update.")
@pytest.mark.asyncio
async def test_op_error_release(pool_maker, pg_server_local):
    pool = await pool_maker(dsn=pg_server_local["dsn"], autocommit=True)

    async with pool.acquire() as conn:

        async def execute():
            start = time.time()

            while time.time() - start < 20:
                await conn.execute("SELECT 1; SELECT pg_sleep(1);")

        async def _kill_conn():
            await asyncio.sleep(2)
            await pg_server_local["container"].kill()
            await pg_server_local["container"].delete(v=True, force=True)
            pg_server_local["container"] = None

        result = await asyncio.gather(
            _kill_conn(), execute(), return_exceptions=True
        )
        exc = result[1]
        assert isinstance(exc, Error)

    assert 9 == pool.freesize
    assert not pool._used


@pytest.mark.asyncio
async def test_release_closed(pool):
    conn = await pool.acquire()
    assert 9 == pool.freesize
    await conn.close()
    await pool.release(conn)
    assert 9 == pool.freesize
    assert not pool._used
    assert 9 == pool.size

    conn2 = await pool.acquire()
    assert 9 == pool.freesize
    assert 10 == pool.size
    await pool.release(conn2)


@pytest.mark.asyncio
async def test_context_manager(pool):
    conn = await pool.acquire()
    try:
        assert isinstance(conn, Connection)
        assert 9 == pool.freesize
        assert {conn} == pool._used
    finally:
        await pool.release(conn)
    assert 10 == pool.freesize


@pytest.mark.asyncio
async def test_clear(pool):
    await pool.clear()
    assert 0 == pool.freesize


@pytest.mark.asyncio
async def test_initial_empty(pool_maker, dsn):
    pool = await pool_maker(dsn=dsn, minsize=0)

    assert 10 == pool.maxsize
    assert 0 == pool.minsize
    assert 0 == pool.size
    assert 0 == pool.freesize

    conn = await pool.acquire()
    try:
        assert 1 == pool.size
        assert 0 == pool.freesize
    finally:
        await pool.release(conn)
    assert 1 == pool.size
    assert 1 == pool.freesize

    conn1 = await pool.acquire()
    assert 1 == pool.size
    assert 0 == pool.freesize

    conn2 = await pool.acquire()
    assert 2 == pool.size
    assert 0 == pool.freesize

    await pool.release(conn1)
    assert 2 == pool.size
    assert 1 == pool.freesize

    await pool.release(conn2)
    assert 2 == pool.size
    assert 2 == pool.freesize


@pytest.mark.asyncio
async def test_parallel_tasks(pool_maker, dsn):
    pool = await pool_maker(dsn=dsn, minsize=0, maxsize=2)

    assert 2 == pool.maxsize
    assert 0 == pool.minsize
    assert 0 == pool.size
    assert 0 == pool.freesize

    fut1 = pool.acquire()
    fut2 = pool.acquire()

    conn1, conn2 = await asyncio.gather(fut1, fut2)
    assert 2 == pool.size
    assert 0 == pool.freesize
    assert {conn1, conn2} == pool._used

    await pool.release(conn1)
    assert 2 == pool.size
    assert 1 == pool.freesize
    assert {conn2} == pool._used

    await pool.release(conn2)
    assert 2 == pool.size
    assert 2 == pool.freesize
    assert not conn1.closed
    assert not conn2.closed

    conn3 = await pool.acquire()
    assert conn3 is conn1
    await pool.release(conn3)


@pytest.mark.asyncio
async def test_parallel_tasks_more(pool_maker, dsn):
    pool = await pool_maker(dsn=dsn, minsize=0, maxsize=3)

    fut1 = pool.acquire()
    fut2 = pool.acquire()
    fut3 = pool.acquire()

    conn1, conn2, conn3 = await asyncio.gather(fut1, fut2, fut3)
    assert 3 == pool.size
    assert 0 == pool.freesize
    assert {conn1, conn2, conn3} == pool._used

    await pool.release(conn1)
    assert 3 == pool.size
    assert 1 == pool.freesize
    assert {conn2, conn3} == pool._used

    await pool.release(conn2)
    assert 3 == pool.size
    assert 2 == pool.freesize
    assert {conn3} == pool._used
    assert not conn1.closed
    assert not conn2.closed

    await pool.release(conn3)
    assert 3 == pool.size
    assert 3 == pool.freesize
    assert not pool._used
    assert not conn1.closed
    assert not conn2.closed
    assert not conn3.closed

    conn4 = await pool.acquire()
    assert conn4 is conn1
    await pool.release(conn4)


@pytest.mark.asyncio
async def test__fill_free(pool_maker, dsn):
    pool = await pool_maker(dsn=dsn, minsize=1)

    first_conn = await pool.acquire()
    try:
        assert 0 == pool.freesize
        assert 1 == pool.size

        conn = await asyncio.wait_for(pool.acquire(), timeout=0.5)
        assert 0 == pool.freesize
        assert 2 == pool.size
        await pool.release(conn)
        assert 1 == pool.freesize
        assert 2 == pool.size
    finally:
        await pool.release(first_conn)
    assert 2 == pool.freesize
    assert 2 == pool.size


@pytest.mark.asyncio
async def test_connect_from_acquire(pool_maker, dsn):
    pool = await pool_maker(dsn=dsn, minsize=0)

    assert 0 == pool.freesize
    assert 0 == pool.size
    conn = await pool.acquire()
    try:
        assert 1 == pool.size
        assert 0 == pool.freesize
    finally:
        await pool.release(conn)
    assert 1 == pool.size
    assert 1 == pool.freesize


@pytest.mark.asyncio
async def test_pool_with_connection_recycling(pool_maker, dsn):
    pool = await pool_maker(dsn=dsn, minsize=1, maxsize=1, pool_recycle=3)
    async with pool.acquire() as conn:
        conn1 = conn

    await asyncio.sleep(5)

    assert 1 == pool.freesize
    async with pool.acquire() as conn:
        conn2 = conn

    assert conn1 is not conn2


@pytest.mark.asyncio
async def test_concurrency(pool_maker, dsn):
    pool = await pool_maker(dsn=dsn, minsize=2, maxsize=4)

    c1 = await pool.acquire()
    c2 = await pool.acquire()
    assert 0 == pool.freesize
    assert 2 == pool.size
    await pool.release(c1)
    await pool.release(c2)


@pytest.mark.asyncio
async def test_invalid_minsize_and_maxsize(dsn):
    with pytest.raises(ValueError):
        await aioodbc.create_pool(dsn=dsn, minsize=-1)

    with pytest.raises(ValueError):
        await aioodbc.create_pool(dsn=dsn, minsize=5, maxsize=2)


@pytest.mark.asyncio
async def test_true_parallel_tasks(pool_maker, dsn):
    pool = await pool_maker(dsn=dsn, minsize=0, maxsize=1)

    assert 1 == pool.maxsize
    assert 0 == pool.minsize
    assert 0 == pool.size
    assert 0 == pool.freesize

    maxsize = 0
    minfreesize = 100

    async def inner():
        nonlocal maxsize, minfreesize
        maxsize = max(maxsize, pool.size)
        minfreesize = min(minfreesize, pool.freesize)
        conn = await pool.acquire()
        maxsize = max(maxsize, pool.size)
        minfreesize = min(minfreesize, pool.freesize)
        await asyncio.sleep(0.01)
        await pool.release(conn)
        maxsize = max(maxsize, pool.size)
        minfreesize = min(minfreesize, pool.freesize)

    await asyncio.gather(inner(), inner())

    assert 1 == maxsize
    assert 0 == minfreesize


@pytest.mark.asyncio
async def test_cannot_acquire_after_closing(pool_maker, dsn):
    pool = await pool_maker(dsn=dsn)

    pool.close()

    with pytest.raises(RuntimeError):
        await pool.acquire()


@pytest.mark.asyncio
async def test_wait_closed(pool_maker, dsn):
    pool = await pool_maker(dsn=dsn)

    c1 = await pool.acquire()
    c2 = await pool.acquire()
    assert 10 == pool.size
    assert 8 == pool.freesize

    ops = []

    async def do_release(conn):
        await asyncio.sleep(0)
        await pool.release(conn)
        ops.append("release")

    async def wait_closed():
        await pool.wait_closed()
        ops.append("wait_closed")

    pool.close()
    await asyncio.gather(wait_closed(), do_release(c1), do_release(c2))
    assert ["release", "release", "wait_closed"] == ops
    assert 0 == pool.freesize


@pytest.mark.asyncio
async def test_echo(pool_maker, dsn):
    pool = await pool_maker(dsn=dsn, echo=True)

    assert pool.echo
    conn = await pool.acquire()
    assert conn.echo
    await pool.release(conn)


@pytest.mark.asyncio
async def test_release_closed_connection(pool_maker, dsn):
    pool = await pool_maker(dsn=dsn)

    conn = await pool.acquire()
    await conn.close()

    await pool.release(conn)
    pool.close()


@pytest.mark.asyncio
async def test_wait_closing_on_not_closed(pool_maker, dsn):
    pool = await pool_maker(dsn=dsn)

    with pytest.raises(RuntimeError):
        await pool.wait_closed()
    pool.close()


@pytest.mark.asyncio
async def test_close_with_acquired_connections(pool_maker, dsn):
    pool = await pool_maker(dsn=dsn)

    conn = await pool.acquire()
    pool.close()

    with pytest.raises(asyncio.TimeoutError):
        await asyncio.wait_for(pool.wait_closed(), 0.1)
    await conn.close()
    await pool.release(conn)


@pytest.mark.parametrize("db", pytest.db_list)
@pytest.mark.asyncio
async def test_pool_with_executor(pool_maker, dsn, executor):
    pool = await pool_maker(executor=executor, dsn=dsn, minsize=2, maxsize=2)

    conn = await pool.acquire()
    try:
        assert isinstance(conn, Connection)
        assert not conn.closed
        assert conn._executor is executor
        cur = await conn.cursor()
        await cur.execute("SELECT 1")
        val = await cur.fetchone()
        assert (1,) == tuple(val)
    finally:
        await pool.release(conn)
    # we close pool here instead in finalizer because of pool should be
    # closed before executor
    pool.close()
    await pool.wait_closed()


@pytest.mark.parametrize("db", pytest.db_list)
@pytest.mark.asyncio
async def test_pool_context_manager(pool):
    assert not pool.closed
    async with pool:
        assert not pool.closed
    assert pool.closed


@pytest.mark.parametrize("db", pytest.db_list)
@pytest.mark.asyncio
async def test_pool_context_manager2(pool):
    async with pool.acquire() as conn:
        assert not conn.closed
        cur = await conn.cursor()
        await cur.execute("SELECT 1")
        val = await cur.fetchone()
        assert (1,) == tuple(val)


@pytest.mark.parametrize("db", pytest.db_list)
@pytest.mark.asyncio
async def test_all_context_managers(dsn, executor):
    kw = {"dsn": dsn, "executor": executor}
    async with aioodbc.create_pool(**kw) as pool:
        async with pool.acquire() as conn:
            async with conn.cursor() as cur:
                assert not pool.closed
                assert not conn.closed
                assert not cur.closed

                await cur.execute("SELECT 1")
                val = await cur.fetchone()
                assert (1,) == tuple(val)

    assert pool.closed
    assert conn.closed
    assert cur.closed


@pytest.mark.parametrize("db", pytest.db_list)
@pytest.mark.asyncio
async def test_context_manager_aexit(connection_maker):
    async def aexit_conntex_managet(conn):
        # commit on exit if no error
        params = (1, "123.45")
        async with conn.cursor() as cur:
            await cur.execute("CREATE TABLE cmt1(n int, v VARCHAR(10))")
            await cur.execute("INSERT INTO cmt1 VALUES (?,?);", params)
        async with conn.cursor() as cur:
            await cur.execute("SELECT v FROM cmt1 WHERE n=1;")
            (value,) = await cur.fetchone()
            assert value == params[1]

        # rollback on exit if error
        with pytest.raises(Error):
            async with conn.cursor() as cur:
                await cur.execute("ins INTO cmt1 VALUES (2, '666');")
        async with conn.cursor() as cur:
            await cur.execute("SELECT v FROM cmt1 WHERE n=2;")
            row = await cur.fetchone()
            assert row is None

        async with conn.cursor() as cur:
            await cur.execute("DROP TABLE cmt1;")

    conn = await connection_maker(autocommit=False)
    assert not conn.autocommit
    await aexit_conntex_managet(conn)

    conn = await connection_maker(autocommit=True)
    assert conn.autocommit
    await aexit_conntex_managet(conn)
