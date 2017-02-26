import asyncio
import gc
import os
import socket
import time
import uuid
from concurrent.futures import ThreadPoolExecutor

import aioodbc
import pyodbc
import pytest
import uvloop

from docker import Client as DockerClient


@pytest.fixture(scope='session')
def session_id():
    """Unique session identifier, random string."""
    return str(uuid.uuid4())


@pytest.fixture(scope='session')
def docker():
    if os.environ.get('DOCKER_MACHINE_IP') is not None:
        docker = DockerClient.from_env(assert_hostname=False)
    else:
        docker = DockerClient(version='auto')
    return docker


@pytest.fixture(scope='session')
def host():
    return os.environ.get('DOCKER_MACHINE_IP', '127.0.0.1')


@pytest.fixture(scope='session')
def unused_port():
    def f():
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.bind(('127.0.0.1', 0))
            return s.getsockname()[1]
    return f


def pytest_generate_tests(metafunc):
    if 'loop_type' in metafunc.fixturenames:
        loop_type = ['default', 'uvloop']
        metafunc.parametrize("loop_type", loop_type)


@pytest.yield_fixture
def loop(request, loop_type):
    old_loop = asyncio.get_event_loop()
    asyncio.set_event_loop(None)
    if loop_type == 'uvloop':
        loop = uvloop.new_event_loop()
    else:
        loop = asyncio.new_event_loop()

    yield loop
    gc.collect()
    loop.close()
    asyncio.set_event_loop(old_loop)



@pytest.fixture
def pg_params(pg_server):
    return dict(**pg_server['pg_params'])


@pytest.yield_fixture(scope='session')
def pg_server(host, unused_port, docker, session_id):
    pg_tag = '9.5'
    docker.pull('postgres:{}'.format(pg_tag))
    port = unused_port()
    container = docker.create_container(
        image='postgres:{}'.format(pg_tag),
        name='aioodbc-test-server-{}-{}'.format(pg_tag, session_id),
        ports=[5432],
        detach=True,
        host_config=docker.create_host_config(port_bindings={5432: port})
    )
    docker.start(container=container['Id'])
    pg_params = dict(database='postgres',
                     user='postgres',
                     password='mysecretpassword',
                     host=host,
                     port=port)
    delay = 0.001
    dsn = create_pg_dsn(pg_params)
    last_error = None
    for i in range(100):
        try:
            conn = pyodbc.connect(dsn)
            cur = conn.cursor()
            cur.execute("SELECT 1;")
            cur.close()
            conn.close()
            break
        except pyodbc.Error as e:
            last_error = e
            time.sleep(delay)
            delay *= 2
    else:
        pytest.fail("Cannot start postgres server: {}".format(last_error))
    container['port'] = port
    container['pg_params'] = pg_params
    yield container

    docker.kill(container=container['Id'])
    docker.remove_container(container['Id'])


@pytest.fixture
def mysql_params(mysql_server):
    return dict(**mysql_server['mysql_params'])


@pytest.yield_fixture(scope='session')
def mysql_server(host, unused_port, docker, session_id):
    mysql_tag = '5.7'
    docker.pull('mysql:{}'.format(mysql_tag))
    port = unused_port()
    container = docker.create_container(
        image='mysql:{}'.format(mysql_tag),
        name='aioodbc-test-server-{}-{}'.format(mysql_tag, session_id),
        ports=[3306],
        detach=True,
        environment={'MYSQL_USER': 'aioodbc',
                     'MYSQL_PASSWORD': 'mysecretpassword',
                     'MYSQL_DATABASE': 'aioodbc',
                     'MYSQL_ROOT_PASSWORD': 'mysecretpassword'},
        host_config=docker.create_host_config(port_bindings={3306: port})
    )
    docker.start(container=container['Id'])
    mysql_params = dict(database='aioodbc',
                        user='aioodbc',
                        password='mysecretpassword',
                        host=host,
                        port=port)
    delay = 0.001
    dsn = create_mysql_dsn(mysql_params)
    last_error = None
    for i in range(100):
        try:
            conn = pyodbc.connect(dsn)
            cur = conn.cursor()
            cur.execute("SELECT 1;")
            cur.close()
            conn.close()
            break
        except pyodbc.Error as e:
            last_error = e
            time.sleep(delay)
            delay *= 2
    else:
        pytest.fail("Cannot start postgres server: {}".format(last_error))
    container['port'] = port
    container['mysql_params'] = mysql_params
    yield container

    docker.kill(container=container['Id'])
    docker.remove_container(container['Id'])


@pytest.fixture
def executor(request):
    executor = ThreadPoolExecutor(max_workers=3)

    def fin():
        executor.shutdown()

    request.addfinalizer(fin)
    return executor


def pytest_namespace():
    return {'db_list': ['pg', 'mysql', 'sqlite']}


@pytest.fixture
def db(request):
    return 'sqlite'


def create_pg_dsn(pg_params):
    dsn = ('Driver=PostgreSQL Unicode;'
           'Server={host};Port={port};'
           'Database={database};Uid={user};'
           'Pwd={password};'.format(**pg_params))
    return dsn


def create_mysql_dsn(mysql_params):
    dsn = ('Driver=MySQL;Server={host};Port={port};'
           'Database={database};User={user};'
           'Password={password}'.format(**mysql_params))
    return dsn


@pytest.fixture
def dsn(request, db):
    if db == 'pg':
        pg_params = request.getfuncargvalue('pg_params')
        conf = create_pg_dsn(pg_params)
    elif db == 'mysql':
        mysql_params = request.getfuncargvalue('mysql_params')
        conf = create_mysql_dsn(mysql_params)
    else:
        conf = os.environ.get('DSN', 'Driver=SQLite;Database=sqlite.db')
    return conf


@pytest.yield_fixture
def conn(request, loop, dsn):
    connection = loop.run_until_complete(_connect(loop, dsn))
    yield connection
    loop.run_until_complete(connection.close())


@pytest.fixture
def connection_maker(request, loop, dsn):
    _conn = None
    async def f(**kw):
        nonlocal _conn
        _conn = await _connect(loop, dsn, **kw)
        return _conn

    def fin():
        if _conn is not None:
            loop.run_until_complete(_conn.close())
    request.addfinalizer(fin)
    return f


@pytest.fixture
def pool_maker(request):
    def f(loop, **kw):
        return _connect_pool(loop, request.addfinalizer, **kw)
    return f


def _connect_pool(loop, finalizer, **kw):
    pool = loop.run_until_complete(aioodbc.create_pool(loop=loop, **kw))

    def fin():
        pool.close()
        loop.run_until_complete(pool.wait_closed())

    finalizer(fin)
    return pool


@pytest.fixture
def pool(request, loop, dsn):
    return _connect_pool(loop, request.addfinalizer, dsn=dsn)


async def _connect(loop, dsn, **kw):
    conn = await aioodbc.connect(dsn=dsn, loop=loop, **kw)
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

        if not asyncio.iscoroutinefunction(pyfuncitem.obj):
            func = asyncio.coroutine(pyfuncitem.obj)
        else:
            func = pyfuncitem.obj
        loop.run_until_complete(func(**testargs))
        return True


def pytest_runtest_setup(item):
    if 'run_loop' in item.keywords and 'loop' not in item.fixturenames:
        # inject an event loop fixture for all async tests
        item.fixturenames.append('loop')


@pytest.fixture
def table(request, conn, loop):

    async def go():
        cur = await conn.cursor()

        await cur.execute("CREATE TABLE t1(n INT, v VARCHAR(10));")
        await cur.execute("INSERT INTO t1 VALUES (1, '123.45');")
        await cur.execute("INSERT INTO t1 VALUES (2, 'foo');")
        await conn.commit()
        await cur.close()

    async def drop_table():
        cur = await conn.cursor()
        await cur.execute("DROP TABLE t1;")
        await cur.commit()
        await cur.close()

    def fin():
        loop.run_until_complete(drop_table())

    request.addfinalizer(fin)

    loop.run_until_complete(go())
    return 't1'
