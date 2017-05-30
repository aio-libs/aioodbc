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

from aiodocker import Docker


@pytest.fixture(scope='session')
def session_id():
    """Unique session identifier, random string."""
    return str(uuid.uuid4())


def pytest_generate_tests(metafunc):
    if 'loop_type' in metafunc.fixturenames:
        loop_type = ['default', 'uvloop']
        metafunc.parametrize("loop_type", loop_type, scope='session')


@pytest.fixture(scope='session')
def event_loop(loop_type):
    if loop_type == 'default':
        asyncio.set_event_loop_policy(asyncio.DefaultEventLoopPolicy())
    elif loop_type == 'uvloop':
        asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
    loop = asyncio.get_event_loop_policy().new_event_loop()
    yield loop
    gc.collect()
    loop.close()


@pytest.fixture(scope='session')
async def docker(event_loop):
    client = Docker()
    yield client
    await client.close()


@pytest.fixture(scope='session')
def host():
    return os.environ.get('DOCKER_MACHINE_IP', '127.0.0.1')


@pytest.fixture
async def pg_params(event_loop, pg_server):
    server_info = (pg_server)['pg_params']
    return dict(**server_info)


@pytest.fixture(scope='session')
async def pg_server(event_loop, host, docker, session_id):
    pg_tag = '9.5'

    await docker.pull('postgres:{}'.format(pg_tag))
    container = await docker.containers.create_or_replace(
        name=f'aioodbc-test-server-{pg_tag}-{session_id}',
        config={
            'Image': f'postgres:{pg_tag}',
            'AttachStdout': False,
            'AttachStderr': False,
            'HostConfig': {
                'PublishAllPorts': True,
            },
        }
    )
    await container.start()
    port = (await container.port(5432))[0]['HostPort']

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

    container_info = {
        'port': port,
        'pg_params': pg_params,
    }
    yield container_info

    await container.kill()
    await container.delete(force=True)


@pytest.fixture
async def mysql_params(event_loop, mysql_server):
    server_info = (mysql_server)['mysql_params']
    return dict(**server_info)


@pytest.fixture(scope='session')
async def mysql_server(event_loop, host, docker, session_id):
    mysql_tag = '5.7'
    await docker.pull('mysql:{}'.format(mysql_tag))
    container = await docker.containers.create_or_replace(
        name=f'aioodbc-test-server-{mysql_tag}-{session_id}',
        config={
            'Image': 'mysql:{}'.format(mysql_tag),
            'AttachStdout': False,
            'AttachStderr': False,
            'Env': ['MYSQL_USER=aioodbc',
                    'MYSQL_PASSWORD=mysecretpassword',
                    'MYSQL_DATABASE=aioodbc',
                    'MYSQL_ROOT_PASSWORD=mysecretpassword'],
            'HostConfig': {
                'PublishAllPorts': True,
            },
        }
    )
    await container.start()
    port = (await container.port(3306))[0]['HostPort']
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
        pytest.fail("Cannot start mysql server: {}".format(last_error))
    container_info = {
        'port': port,
        'mysql_params': mysql_params,
    }
    yield container_info

    await container.kill()
    await container.delete(force=True)


@pytest.fixture
def executor():
    executor = ThreadPoolExecutor(max_workers=1)
    yield executor
    executor.shutdown()


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
        pg_params = request.getfixturevalue('pg_params')
        conf = create_pg_dsn(pg_params)
    elif db == 'mysql':
        mysql_params = request.getfixturevalue('mysql_params')
        conf = create_mysql_dsn(mysql_params)
    else:
        conf = os.environ.get('DSN', 'Driver=SQLite;Database=sqlite.db')
    return conf


@pytest.fixture
async def conn(event_loop, dsn, connection_maker):
    connection = await connection_maker()
    yield connection


@pytest.fixture
async def connection_maker(event_loop, dsn):
    cleanup = []

    async def make(**kw):
        if kw.get('executor', None) is None:
            executor = ThreadPoolExecutor(max_workers=1)
            kw['executor'] = executor
        else:
            executor = kw['executor']

        conn = await aioodbc.connect(dsn=dsn, loop=event_loop, **kw)
        cleanup.append((conn, executor))
        return conn

    yield make

    for conn, executor in cleanup:
        await conn.close()
        executor.shutdown()


@pytest.fixture
async def pool(event_loop, dsn):
    pool = await aioodbc.create_pool(loop=event_loop, dsn=dsn)

    yield pool

    pool.close()
    await pool.wait_closed()


@pytest.fixture
async def pool_maker(event_loop):
    pool = None

    async def make(loop, **kw):
        nonlocal pool
        pool = await aioodbc.create_pool(loop=loop, **kw)
        return pool

    yield make

    if pool:
        pool.close()
        await pool.wait_closed()


@pytest.fixture
async def table(event_loop, conn):

    cur = await conn.cursor()
    await cur.execute("CREATE TABLE t1(n INT, v VARCHAR(10));")
    await cur.execute("INSERT INTO t1 VALUES (1, '123.45');")
    await cur.execute("INSERT INTO t1 VALUES (2, 'foo');")
    await conn.commit()
    await cur.close()

    yield 't1'

    cur = await conn.cursor()
    await cur.execute("DROP TABLE t1;")
    await cur.commit()
    await cur.close()
