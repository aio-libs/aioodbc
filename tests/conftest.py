import asyncio
import os

import pytest
import aioodbc


@pytest.fixture
def loop(request):
    old_loop = asyncio.get_event_loop()
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(None)

    def fin():
        loop.close()
        asyncio.set_event_loop(old_loop)

    request.addfinalizer(fin)
    return loop


@pytest.fixture
def dsn(request):
    conf = os.environ.get('DSN', 'Driver=SQLite;Database=sqlite.db')
    return conf


@pytest.fixture
def conn(request, loop):
    return _connect(loop, request.addfinalizer)


@pytest.fixture
def connection_maker(request):
    def f(loop, **kw):
        return _connect(loop, request.addfinalizer, **kw)
    return f


def _connect(loop, finalizer, **kw):
    dsn = os.environ.get('DSN', 'Driver=SQLite;Database=sqlite.db')
    conn = loop.run_until_complete(aioodbc.connect(dsn, loop=loop, **kw))

    def fin():
        loop.run_until_complete(conn.ensure_closed())

    finalizer(fin)
    return conn


@pytest.mark.tryfirst
def pytest_pycollect_makeitem(collector, name, obj):
    if collector.funcnamefilter(name):
        item = pytest.Function(name, parent=collector)
        if 'run_loop' in item.keywords:
            return list(collector._genfunctions(name, obj))


@pytest.mark.tryfirst
def pytest_pyfunc_call(pyfuncitem):
    """
    Run asyncio marked test functions in an event loop instead of a normal
    function call.
    """
    if 'run_loop' in pyfuncitem.keywords:
        funcargs = pyfuncitem.funcargs
        loop = funcargs['loop']
        testargs = {arg: funcargs[arg]
                    for arg in pyfuncitem._fixtureinfo.argnames}
        loop.run_until_complete(pyfuncitem.obj(**testargs))
        return True


def pytest_runtest_setup(item):
    if 'run_loop' in item.keywords and 'loop' not in item.fixturenames:
        # inject an event loop fixture for all async tests
        item.fixturenames.append('loop')


@pytest.fixture
def table(request, conn, loop):

    @asyncio.coroutine
    def go():
        cur = yield from conn.cursor()

        yield from cur.execute("CREATE TABLE t1(n INT, v VARCHAR(10));")
        yield from cur.execute("INSERT INTO t1 VALUES (1, '123.45');")
        yield from cur.execute("INSERT INTO t1 VALUES (2, 'foo');")
        yield from conn.commit()
        yield from cur.close()

    @asyncio.coroutine
    def drop_table():
        cur = yield from conn.cursor()
        yield from cur.execute("DROP TABLE t1;")
        yield from cur.commit()
        yield from cur.close()

    def fin():
        loop.run_until_complete(drop_table())

    request.addfinalizer(fin)

    loop.run_until_complete(go())
    return 't1'
