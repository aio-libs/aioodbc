import asyncio

import pytest
import aioodbc
from aioodbc import Pool, Connection


def test_create_pool(loop, pool_maker, dsn):
    pool = pool_maker(loop, dsn=dsn)
    assert isinstance(pool, Pool)
    assert 10 == pool.minsize
    assert 10 == pool.maxsize
    assert 10 == pool.size
    assert 10 == pool.freesize
    assert not pool.echo


def test_create_pool2(loop, pool_maker, dsn):
    pool = pool_maker(loop, dsn=dsn, maxsize=20)
    assert isinstance(pool, Pool)
    assert 10 == pool.minsize
    assert 20 == pool.maxsize
    assert 10 == pool.size
    assert 10 == pool.freesize


@pytest.mark.parametrize('dsn', pytest.dsn_list)
def test_acquire(loop, pool):
    async def go():
        conn = await pool.acquire()
        try:
            assert isinstance(conn, Connection)
            assert not conn.closed
            cur = await conn.cursor()
            await cur.execute('SELECT 1')
            val = await cur.fetchone()
            assert (1,) == tuple(val)
        finally:
            await pool.release(conn)

    loop.run_until_complete(go())


def test_release(loop, pool):
    async def go():
        conn = await pool.acquire()
        try:
            assert 9 == pool.freesize
            assert {conn} == pool._used
        finally:
            await pool.release(conn)
        assert 10 == pool.freesize
        assert not pool._used

    loop.run_until_complete(go())


def test_release_closed(loop, pool):
    async def go():
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

    loop.run_until_complete(go())


def test_context_manager(loop, pool):

    async def go():
        conn = await pool.acquire()
        try:
            assert isinstance(conn, Connection)
            assert 9 == pool.freesize
            assert {conn} == pool._used
        finally:
            await pool.release(conn)
        assert 10 == pool.freesize

    loop.run_until_complete(go())


def test_clear(loop, pool):
    async def go():
        await pool.clear()
        assert 0 == pool.freesize

    loop.run_until_complete(go())


def test_initial_empty(loop, pool_maker, dsn):
    pool = pool_maker(loop, dsn=dsn, minsize=0)

    async def go():
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

    loop.run_until_complete(go())


def test_parallel_tasks(loop, pool_maker, dsn):
    pool = pool_maker(loop, dsn=dsn, minsize=0, maxsize=2)

    async def go():
        assert 2 == pool.maxsize
        assert 0 == pool.minsize
        assert 0 == pool.size
        assert 0 == pool.freesize

        fut1 = pool.acquire()
        fut2 = pool.acquire()

        conn1, conn2 = await asyncio.gather(fut1, fut2, loop=loop)
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

    loop.run_until_complete(go())


def test_parallel_tasks_more(loop, pool_maker, dsn):
    pool = pool_maker(loop, dsn=dsn, minsize=0, maxsize=3)

    async def go():
        fut1 = pool.acquire()
        fut2 = pool.acquire()
        fut3 = pool.acquire()

        conn1, conn2, conn3 = await asyncio.gather(fut1, fut2, fut3,
                                                   loop=loop)
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

        loop.run_until_complete(go())


def test_default_event_loop(loop, dsn):
    asyncio.set_event_loop(loop)

    async def go():
        pool = await aioodbc.create_pool(dsn=dsn)
        assert pool._loop is loop
        pool.close()
        await pool.wait_closed()

    loop.run_until_complete(go())


def test__fill_free(loop, pool_maker, dsn):
    pool = pool_maker(loop, dsn=dsn, minsize=1)

    async def go():
        first_conn = await pool.acquire()
        try:
            assert 0 == pool.freesize
            assert 1 == pool.size

            conn = await asyncio.wait_for(pool.acquire(), timeout=0.5,
                                          loop=loop)
            assert 0 == pool.freesize
            assert 2 == pool.size
            await pool.release(conn)
            assert 1 == pool.freesize
            assert 2 == pool.size
        finally:
            await pool.release(first_conn)
        assert 2 == pool.freesize
        assert 2 == pool.size

    loop.run_until_complete(go())


def test_connect_from_acquire(loop, pool_maker, dsn):
    pool = pool_maker(loop, dsn=dsn, minsize=0)

    async def go():
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

    loop.run_until_complete(go())


def test_concurrency(loop, pool_maker, dsn):
    pool = pool_maker(loop, dsn=dsn, minsize=2, maxsize=4)

    async def go():
        c1 = await pool.acquire()
        c2 = await pool.acquire()
        assert 0 == pool.freesize
        assert 2 == pool.size
        await pool.release(c1)
        await pool.release(c2)

    loop.run_until_complete(go())


def test_invalid_minsize_and_maxsize(loop, dsn):
    async def go():
        with pytest.raises(ValueError):
            await aioodbc.create_pool(dsn=dsn, loop=loop, minsize=-1)

        with pytest.raises(ValueError):
            await aioodbc.create_pool(dsn=dsn, loop=loop, minsize=5,
                                      maxsize=2)

    loop.run_until_complete(go())


def test_true_parallel_tasks(loop, pool_maker, dsn):
    pool = pool_maker(loop, dsn=dsn, minsize=0, maxsize=1)

    async def go():
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
            await asyncio.sleep(0.01, loop=loop)
            await pool.release(conn)
            maxsize = max(maxsize, pool.size)
            minfreesize = min(minfreesize, pool.freesize)

        await asyncio.gather(inner(), inner(), loop=loop)

        assert 1 == maxsize
        assert 0 == minfreesize

    loop.run_until_complete(go())


def test_cannot_acquire_after_closing(loop, pool_maker, dsn):
    pool = pool_maker(loop, dsn=dsn)

    async def go():
        pool.close()

        with pytest.raises(RuntimeError):
            await pool.acquire()

    loop.run_until_complete(go())


def test_wait_closed(loop, pool_maker, dsn):
    pool = pool_maker(loop, dsn=dsn)

    async def go():
        c1 = await pool.acquire()
        c2 = await pool.acquire()
        assert 10 == pool.size
        assert 8 == pool.freesize

        ops = []

        async def do_release(conn):
            await asyncio.sleep(0, loop=loop)
            await pool.release(conn)
            ops.append('release')

        async def wait_closed():
            await pool.wait_closed()
            ops.append('wait_closed')

        pool.close()
        await asyncio.gather(wait_closed(),
                             do_release(c1),
                             do_release(c2),
                             loop=loop)
        assert ['release', 'release', 'wait_closed'] == ops
        assert 0 == pool.freesize

    loop.run_until_complete(go())


def test_echo(loop, pool_maker, dsn):
    pool = pool_maker(loop, dsn=dsn, echo=True)

    async def go():
        assert pool.echo
        conn = await pool.acquire()
        assert conn.echo
        await pool.release(conn)

    loop.run_until_complete(go())


def test_release_closed_connection(loop, pool_maker, dsn):
    pool = pool_maker(loop, dsn=dsn)

    async def go():
        conn = await pool.acquire()
        await conn.close()

        await pool.release(conn)
        pool.close()

    loop.run_until_complete(go())


def test_wait_closing_on_not_closed(loop, pool_maker, dsn):
    pool = pool_maker(loop, dsn=dsn)

    async def go():
        with pytest.raises(RuntimeError):
            await pool.wait_closed()
        pool.close()

    loop.run_until_complete(go())


def test_close_with_acquired_connections(loop, pool_maker, dsn):
    pool = pool_maker(loop, dsn=dsn)

    async def go():
        conn = await pool.acquire()
        pool.close()

        with pytest.raises(asyncio.TimeoutError):
            await asyncio.wait_for(pool.wait_closed(), 0.1, loop=loop)
        await conn.close()
        await pool.release(conn)

    loop.run_until_complete(go())


@pytest.mark.parametrize('dsn', pytest.dsn_list)
def test_pool_with_executor(loop, pool_maker, dsn, executor):
    pool = pool_maker(loop, executor=executor, dsn=dsn, minsize=2, maxsize=2)

    async def go():
        conn = await pool.acquire()
        try:
            assert isinstance(conn, Connection)
            assert not conn.closed
            assert conn._executor is executor
            cur = await conn.cursor()
            await cur.execute('SELECT 1')
            val = await cur.fetchone()
            assert (1,) == tuple(val)
        finally:
            await pool.release(conn)
        # we close pool here instead in finalizer because of pool should be
        # closed before executor
        pool.close()
        await pool.wait_closed()

    loop.run_until_complete(go())


@pytest.mark.parametrize('dsn', pytest.dsn_list)
def test_pool_context_manager(loop, pool):
    async def go():
        assert not pool.closed
        async with pool:
            assert not pool.closed
        assert pool.closed

    loop.run_until_complete(go())


@pytest.mark.parametrize('dsn', pytest.dsn_list)
def test_pool_context_manager2(loop, pool):
    async def go():
        async with pool.get() as conn:
            assert not conn.closed
            cur = await conn.cursor()
            await cur.execute('SELECT 1')
            val = await cur.fetchone()
            assert (1,) == tuple(val)

    loop.run_until_complete(go())


@pytest.mark.parametrize('dsn', pytest.dsn_list)
def test_all_context_managets(dsn, loop):
    async def go():
        async with aioodbc.create_pool(dsn=dsn, loop=loop) as pool:
            async with pool.get() as conn:
                async with conn.cursor() as cur:
                    assert not pool.closed
                    assert not conn.closed
                    assert not cur.closed

                    await cur.execute('SELECT 1')
                    val = await cur.fetchone()
                    assert (1,) == tuple(val)

        assert pool.closed
        assert conn.closed
        assert cur.closed
    loop.run_until_complete(go())
