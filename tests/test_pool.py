import asyncio

import pytest
import aioodbc
from aioodbc.connection import Connection
from aioodbc.pool import Pool


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
    @asyncio.coroutine
    def go():
        conn = yield from pool.acquire()
        try:
            assert isinstance(conn, Connection)
            assert not conn.closed
            cur = yield from conn.cursor()
            yield from cur.execute('SELECT 1')
            val = yield from cur.fetchone()
            assert (1,) == tuple(val)
        finally:
            yield from pool.release(conn)

    loop.run_until_complete(go())


def test_release(loop, pool):
    @asyncio.coroutine
    def go():
        conn = yield from pool.acquire()
        try:
            assert 9 == pool.freesize
            assert {conn} == pool._used
        finally:
            yield from pool.release(conn)
        assert 10 == pool.freesize
        assert not pool._used

    loop.run_until_complete(go())


def test_release_closed(loop, pool):
    @asyncio.coroutine
    def go():
        conn = yield from pool.acquire()
        assert 9 == pool.freesize
        yield from conn.close()
        yield from pool.release(conn)
        assert 9 == pool.freesize
        assert not pool._used
        assert 9 == pool.size

        conn2 = yield from pool.acquire()
        assert 9 == pool.freesize
        assert 10 == pool.size
        yield from pool.release(conn2)

    loop.run_until_complete(go())


def test_context_manager(loop, pool):

    @asyncio.coroutine
    def go():
        conn = yield from pool.acquire()
        try:
            assert isinstance(conn, Connection)
            assert 9 == pool.freesize
            assert {conn} == pool._used
        finally:
            yield from pool.release(conn)
        assert 10 == pool.freesize

    loop.run_until_complete(go())


def test_clear(loop, pool):
    @asyncio.coroutine
    def go():
        yield from pool.clear()
        assert 0 == pool.freesize

    loop.run_until_complete(go())


def test_initial_empty(loop, pool_maker, dsn):
    pool = pool_maker(loop, dsn=dsn, minsize=0)

    @asyncio.coroutine
    def go():
        assert 10 == pool.maxsize
        assert 0 == pool.minsize
        assert 0 == pool.size
        assert 0 == pool.freesize

        conn = yield from pool.acquire()
        try:
            assert 1 == pool.size
            assert 0 == pool.freesize
        finally:
            yield from pool.release(conn)
        assert 1 == pool.size
        assert 1 == pool.freesize

        conn1 = yield from pool.acquire()
        assert 1 == pool.size
        assert 0 == pool.freesize

        conn2 = yield from pool.acquire()
        assert 2 == pool.size
        assert 0 == pool.freesize

        yield from pool.release(conn1)
        assert 2 == pool.size
        assert 1 == pool.freesize

        yield from pool.release(conn2)
        assert 2 == pool.size
        assert 2 == pool.freesize

    loop.run_until_complete(go())


def test_parallel_tasks(loop, pool_maker, dsn):
    pool = pool_maker(loop, dsn=dsn, minsize=0, maxsize=2)

    @asyncio.coroutine
    def go():
        assert 2 == pool.maxsize
        assert 0 == pool.minsize
        assert 0 == pool.size
        assert 0 == pool.freesize

        fut1 = pool.acquire()
        fut2 = pool.acquire()

        conn1, conn2 = yield from asyncio.gather(fut1, fut2, loop=loop)
        assert 2 == pool.size
        assert 0 == pool.freesize
        assert {conn1, conn2} == pool._used

        yield from pool.release(conn1)
        assert 2 == pool.size
        assert 1 == pool.freesize
        assert {conn2} == pool._used

        yield from pool.release(conn2)
        assert 2 == pool.size
        assert 2 == pool.freesize
        assert not conn1.closed
        assert not conn2.closed

        conn3 = yield from pool.acquire()
        assert conn3 is conn1
        yield from pool.release(conn3)

    loop.run_until_complete(go())


def test_parallel_tasks_more(loop, pool_maker, dsn):
    pool = pool_maker(loop, dsn=dsn, minsize=0, maxsize=3)

    @asyncio.coroutine
    def go():
        fut1 = pool.acquire()
        fut2 = pool.acquire()
        fut3 = pool.acquire()

        conn1, conn2, conn3 = yield from asyncio.gather(fut1, fut2, fut3,
                                                        loop=loop)
        assert 3 == pool.size
        assert 0 == pool.freesize
        assert {conn1, conn2, conn3} == pool._used

        yield from pool.release(conn1)
        assert 3 == pool.size
        assert 1 == pool.freesize
        assert {conn2, conn3} == pool._used

        yield from pool.release(conn2)
        assert 3 == pool.size
        assert 2 == pool.freesize
        assert {conn3} == pool._used
        assert not conn1.closed
        assert not conn2.closed

        yield from pool.release(conn3)
        assert 3 == pool.size
        assert 3 == pool.freesize
        assert not pool._used
        assert not conn1.closed
        assert not conn2.closed
        assert not conn3.closed

        conn4 = yield from pool.acquire()
        assert conn4 is conn1
        yield from pool.release(conn4)

        loop.run_until_complete(go())


def test_default_event_loop(loop, dsn):
    asyncio.set_event_loop(loop)

    @asyncio.coroutine
    def go():
        pool = yield from aioodbc.create_pool(dsn=dsn)
        assert pool._loop is loop
        pool.close()
        yield from pool.wait_closed()

    loop.run_until_complete(go())


def test__fill_free(loop, pool_maker, dsn):
    pool = pool_maker(loop, dsn=dsn, minsize=1)

    @asyncio.coroutine
    def go():
        first_conn = yield from pool.acquire()
        try:
            assert 0 == pool.freesize
            assert 1 == pool.size

            conn = yield from asyncio.wait_for(pool.acquire(), timeout=0.5,
                                               loop=loop)
            assert 0 == pool.freesize
            assert 2 == pool.size
            yield from pool.release(conn)
            assert 1 == pool.freesize
            assert 2 == pool.size
        finally:
            yield from pool.release(first_conn)
        assert 2 == pool.freesize
        assert 2 == pool.size

    loop.run_until_complete(go())


def test_connect_from_acquire(loop, pool_maker, dsn):
    pool = pool_maker(loop, dsn=dsn, minsize=0)

    @asyncio.coroutine
    def go():
        assert 0 == pool.freesize
        assert 0 == pool.size
        conn = yield from pool.acquire()
        try:
            assert 1 == pool.size
            assert 0 == pool.freesize
        finally:
            yield from pool.release(conn)
        assert 1 == pool.size
        assert 1 == pool.freesize

    loop.run_until_complete(go())


def test_concurrency(loop, pool_maker, dsn):
    pool = pool_maker(loop, dsn=dsn, minsize=2, maxsize=4)

    @asyncio.coroutine
    def go():
        c1 = yield from pool.acquire()
        c2 = yield from pool.acquire()
        assert 0 == pool.freesize
        assert 2 == pool.size
        yield from pool.release(c1)
        yield from pool.release(c2)

    loop.run_until_complete(go())


def test_invalid_minsize_and_maxsize(loop, dsn):
    @asyncio.coroutine
    def go():
        with pytest.raises(ValueError):
            yield from aioodbc.create_pool(dsn=dsn, loop=loop, minsize=-1)

        with pytest.raises(ValueError):
            yield from aioodbc.create_pool(dsn=dsn, loop=loop, minsize=5,
                                           maxsize=2)

    loop.run_until_complete(go())


def test_true_parallel_tasks(loop, pool_maker, dsn):
    pool = pool_maker(loop, dsn=dsn, minsize=0, maxsize=1)

    @asyncio.coroutine
    def go():
        assert 1 == pool.maxsize
        assert 0 == pool.minsize
        assert 0 == pool.size
        assert 0 == pool.freesize

        maxsize = 0
        minfreesize = 100

        def inner():
            nonlocal maxsize, minfreesize
            maxsize = max(maxsize, pool.size)
            minfreesize = min(minfreesize, pool.freesize)
            conn = yield from pool.acquire()
            maxsize = max(maxsize, pool.size)
            minfreesize = min(minfreesize, pool.freesize)
            yield from asyncio.sleep(0.01, loop=loop)
            yield from pool.release(conn)
            maxsize = max(maxsize, pool.size)
            minfreesize = min(minfreesize, pool.freesize)

        yield from asyncio.gather(inner(), inner(), loop=loop)

        assert 1 == maxsize
        assert 0 == minfreesize

    loop.run_until_complete(go())


def test_cannot_acquire_after_closing(loop, pool_maker, dsn):
    pool = pool_maker(loop, dsn=dsn)

    @asyncio.coroutine
    def go():
        pool.close()

        with pytest.raises(RuntimeError):
            yield from pool.acquire()

    loop.run_until_complete(go())


def test_wait_closed(loop, pool_maker, dsn):
    pool = pool_maker(loop, dsn=dsn)

    @asyncio.coroutine
    def go():
        c1 = yield from pool.acquire()
        c2 = yield from pool.acquire()
        assert 10 == pool.size
        assert 8 == pool.freesize

        ops = []

        @asyncio.coroutine
        def do_release(conn):
            yield from asyncio.sleep(0, loop=loop)
            yield from pool.release(conn)
            ops.append('release')

        @asyncio.coroutine
        def wait_closed():
            yield from pool.wait_closed()
            ops.append('wait_closed')

        pool.close()
        yield from asyncio.gather(wait_closed(),
                                  do_release(c1),
                                  do_release(c2),
                                  loop=loop)
        assert ['release', 'release', 'wait_closed'] == ops
        assert 0 == pool.freesize

    loop.run_until_complete(go())


def test_echo(loop, pool_maker, dsn):
    pool = pool_maker(loop, dsn=dsn, echo=True)

    @asyncio.coroutine
    def go():
        assert pool.echo
        conn = yield from pool.acquire()
        assert conn.echo
        yield from pool.release(conn)

    loop.run_until_complete(go())


def test_release_closed_connection(loop, pool_maker, dsn):
    pool = pool_maker(loop, dsn=dsn)

    @asyncio.coroutine
    def go():
        conn = yield from pool.acquire()
        yield from conn.close()

        yield from pool.release(conn)
        pool.close()

    loop.run_until_complete(go())


def test_wait_closing_on_not_closed(loop, pool_maker, dsn):
    pool = pool_maker(loop, dsn=dsn)

    @asyncio.coroutine
    def go():
        with pytest.raises(RuntimeError):
            yield from pool.wait_closed()
        pool.close()

    loop.run_until_complete(go())


def test_close_with_acquired_connections(loop, pool_maker, dsn):
    pool = pool_maker(loop, dsn=dsn)

    @asyncio.coroutine
    def go():
        conn = yield from pool.acquire()
        pool.close()

        with pytest.raises(asyncio.TimeoutError):
            yield from asyncio.wait_for(pool.wait_closed(),
                                        0.1, loop=loop)
        yield from conn.close()
        yield from pool.release(conn)

    loop.run_until_complete(go())


@pytest.mark.parametrize('dsn', pytest.dsn_list)
def test_pool_with_executor(loop, pool_maker, dsn, executor):
    pool = pool_maker(loop, executor=executor, dsn=dsn, minsize=2, maxsize=2)

    @asyncio.coroutine
    def go():
        conn = yield from pool.acquire()
        try:
            assert isinstance(conn, Connection)
            assert not conn.closed
            assert conn._executor is executor
            cur = yield from conn.cursor()
            yield from cur.execute('SELECT 1')
            val = yield from cur.fetchone()
            assert (1,) == tuple(val)
        finally:
            yield from pool.release(conn)
        # we close pool here instead in finalizer because of pool should be
        # closed before executor
        pool.close()
        yield from pool.wait_closed()

    loop.run_until_complete(go())
