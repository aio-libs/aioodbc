import asyncio
import gc
import os
import uuid
from concurrent.futures import ThreadPoolExecutor

import pytest
import pytest_asyncio
import uvloop

import aioodbc


@pytest.fixture(scope="session")
def session_id():
    """Unique session identifier, random string."""
    return str(uuid.uuid4())


@pytest.fixture(autouse=True, scope="session", params=["default", "uvloop"])
def event_loop(request):
    if request.param == "default":
        asyncio.set_event_loop_policy(asyncio.DefaultEventLoopPolicy())
    elif request.param == "uvloop":
        asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
    loop = asyncio.get_event_loop_policy().new_event_loop()

    try:
        yield loop
    finally:
        gc.collect()
        loop.close()


# alias
@pytest.fixture(scope="session")
def loop(event_loop):
    return event_loop


@pytest.fixture
def executor():
    executor = ThreadPoolExecutor(max_workers=1)

    try:
        yield executor
    finally:
        executor.shutdown(True)


def pytest_configure():
    pytest.db_list = ["sqlite"]


@pytest.fixture
def db():
    return "sqlite"


def create_pg_dsn(pg_params):
    dsn = (
        "Driver=PostgreSQL Unicode;"
        "Server={host};Port={port};"
        "Database={database};Uid={user};"
        "Pwd={password};".format(**pg_params)
    )
    return dsn


def create_mysql_dsn(mysql_params):
    dsn = (
        "Driver=MySQL;Server={host};Port={port};"
        "Database={database};User={user};"
        "Password={password}".format(**mysql_params)
    )
    return dsn


@pytest.fixture
def dsn(tmp_path, request, db):
    if db == "pg":
        pg_params = request.getfixturevalue("pg_params")
        conf = create_pg_dsn(pg_params)
    elif db == "mysql":
        mysql_params = request.getfixturevalue("mysql_params")
        conf = create_mysql_dsn(mysql_params)
    else:
        p = tmp_path / "sqlite.db"
        conf = os.environ.get("DSN", "Driver=SQLite3;Database={}".format(p))

    return conf


@pytest_asyncio.fixture
async def conn(dsn, connection_maker):
    assert dsn
    connection = await connection_maker()
    yield connection


@pytest_asyncio.fixture
async def connection_maker(dsn):
    cleanup = []

    async def make(**kw):
        if kw.get("executor", None) is None:
            executor = ThreadPoolExecutor(max_workers=1)
            kw["executor"] = executor
        else:
            executor = kw["executor"]

        conn = await aioodbc.connect(dsn=dsn, **kw)
        cleanup.append((conn, executor))
        return conn

    try:
        yield make
    finally:
        for conn, executor in cleanup:
            await conn.close()
            executor.shutdown(True)


@pytest_asyncio.fixture
async def pool(dsn):
    pool = await aioodbc.create_pool(dsn=dsn)

    try:
        yield pool
    finally:
        pool.close()
        await pool.wait_closed()


@pytest_asyncio.fixture
async def pool_maker():
    pool_list = []

    async def make(**kw):
        pool = await aioodbc.create_pool(**kw)
        pool_list.append(pool)
        return pool

    try:
        yield make
    finally:
        for pool in pool_list:
            pool.close()
            await pool.wait_closed()


@pytest_asyncio.fixture
async def table(conn):
    cur = await conn.cursor()
    await cur.execute("CREATE TABLE t1(n INT, v VARCHAR(10));")
    await cur.execute("INSERT INTO t1 VALUES (1, '123.45');")
    await cur.execute("INSERT INTO t1 VALUES (2, 'foo');")
    await conn.commit()
    await cur.close()

    try:
        yield "t1"
    finally:
        cur = await conn.cursor()
        await cur.execute("DROP TABLE t1;")
        await cur.commit()
        await cur.close()
