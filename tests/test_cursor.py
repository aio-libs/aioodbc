import pyodbc
import pytest


@pytest.mark.parametrize("db", pytest.db_list)
@pytest.mark.asyncio
async def test_cursor_with(conn, table):
    ret = []

    # regular cursor usage
    cur = await conn.cursor()
    await cur.execute("SELECT * FROM t1;")
    assert not cur.closed
    assert not cur.echo

    # cursor should be closed
    async with cur:
        assert not cur.echo
        async for i in cur:
            ret.append(i)
    expected = [tuple(r) for r in ret]
    assert [(1, "123.45"), (2, "foo")] == expected
    assert cur.closed


@pytest.mark.parametrize("db", pytest.db_list)
@pytest.mark.asyncio
async def test_cursor_lightweight(conn, table):
    cur = await conn.cursor()
    ex_cursor = await cur.execute("SELECT * FROM t1;")
    assert ex_cursor is cur

    assert not cur.closed
    async with cur:
        pass

    assert cur.closed


@pytest.mark.parametrize("db", pytest.db_list)
@pytest.mark.asyncio
async def test_cursor_await(conn, table):
    async with conn.cursor() as cur:
        await cur.execute("SELECT * FROM t1;")
        assert not cur.closed

    assert cur.closed


@pytest.mark.parametrize("db", pytest.db_list)
@pytest.mark.asyncio
async def test_cursor(conn):
    cur = await conn.cursor()
    assert cur.connection is conn
    assert cur._loop, conn.loop
    assert cur.arraysize == 1
    assert cur.rowcount == -1

    r = await cur.setinputsizes(
        [
            (pyodbc.SQL_WVARCHAR, 50, 0),
        ]
    )
    assert r is None

    await cur.setoutputsize()
    assert r is None
    await cur.close()


@pytest.mark.parametrize("db", pytest.db_list)
@pytest.mark.asyncio
async def test_execute_on_closed_cursor(conn):
    cur = await conn.cursor()
    await cur.close()
    with pytest.raises(pyodbc.OperationalError):
        await cur.execute("SELECT 1;")


@pytest.mark.parametrize("db", pytest.db_list)
@pytest.mark.asyncio
async def test_close(conn):
    cur = await conn.cursor()
    assert not cur.closed
    await cur.close()
    await cur.close()
    assert cur.closed


@pytest.mark.parametrize("db", ["sqlite"])
@pytest.mark.asyncio
async def test_description(conn):
    cur = await conn.cursor()
    assert cur.description is None
    await cur.execute("SELECT 1;")
    expected = (("1", int, None, 10, 10, 0, True),)
    assert cur.description == expected
    await cur.close()


@pytest.mark.parametrize("db", pytest.db_list)
@pytest.mark.asyncio
async def test_description_with_real_table(conn, table):
    cur = await conn.cursor()
    await cur.execute("SELECT * FROM t1;")

    expected = (
        ("n", int, None, 10, 10, 0, True),
        ("v", str, None, 10, 10, 0, True),
    )
    assert cur.description == expected
    await cur.close()


@pytest.mark.parametrize("db", pytest.db_list)
@pytest.mark.asyncio
async def test_rowcount_with_table(conn, table):
    cur = await conn.cursor()
    await cur.execute("SELECT * FROM t1;")
    await cur.fetchall()
    # sqlite does not provide working rowcount attribute
    # http://stackoverflow.com/questions/4911404/in-pythons-sqlite3-
    # module-why-cant-cursor-rowcount-tell-me-the-number-of-ro
    # TODO: figure out for proper test
    assert cur.rowcount in (0, 2)
    await cur.close()


@pytest.mark.parametrize("db", pytest.db_list)
@pytest.mark.asyncio
async def test_arraysize(conn):
    cur = await conn.cursor()
    assert 1 == cur.arraysize
    cur.arraysize = 10
    assert 10 == cur.arraysize
    await cur.close()


@pytest.mark.parametrize("db", pytest.db_list)
@pytest.mark.asyncio
async def test_fetchall(conn, table):
    cur = await conn.cursor()
    await cur.execute("SELECT * FROM t1;")
    resp = await cur.fetchall()
    expected = [(1, "123.45"), (2, "foo")]

    for row, exp in zip(resp, expected):
        assert exp == tuple(row)

    await cur.close()


@pytest.mark.parametrize("db", pytest.db_list)
@pytest.mark.asyncio
async def test_fetchmany(conn, table):
    cur = await conn.cursor()
    await cur.execute("SELECT * FROM t1;")
    resp = await cur.fetchmany(1)
    expected = [(1, "123.45")]

    for row, exp in zip(resp, expected):
        assert exp == tuple(row)

    await cur.close()


@pytest.mark.parametrize("db", pytest.db_list)
@pytest.mark.asyncio
async def test_fetchone(conn, table):
    cur = await conn.cursor()
    await cur.execute("SELECT * FROM t1;")
    resp = await cur.fetchone()
    expected = (1, "123.45")

    assert expected == tuple(resp)
    await cur.close()


@pytest.mark.parametrize("db", pytest.db_list)
@pytest.mark.asyncio
async def test_fetchval(conn, table):
    cur = await conn.cursor()
    await cur.execute("SELECT * FROM t1;")
    resp = await cur.fetchval()
    expected = 1

    assert expected == resp
    await cur.close()


@pytest.mark.parametrize("db", ["sqlite"])
@pytest.mark.asyncio
async def test_tables(conn, table):
    cur = await conn.cursor()
    await cur.tables()
    resp = await cur.fetchall()
    expectd = (None, None, "t1", "TABLE", None)
    assert len(resp) == 1, resp
    assert expectd == tuple(resp[0]), resp


@pytest.mark.parametrize("db", pytest.db_list)
@pytest.mark.asyncio
async def test_cursor_rollback(conn, table):
    cur = await conn.cursor()
    await cur.execute("INSERT INTO t1 VALUES (3, '123.45');")
    await cur.execute("SELECT v FROM t1 WHERE n=3;")
    (value,) = await cur.fetchone()
    assert value == "123.45"

    await cur.rollback()
    await cur.execute("SELECT v FROM t1 WHERE n=3;")
    value = await cur.fetchone()
    assert value is None


@pytest.mark.parametrize("db", ["sqlite"])
@pytest.mark.asyncio
async def test_columns(conn, table):
    cur = await conn.cursor()
    await cur.columns()
    resp = await cur.fetchall()
    expectd = [
        (
            "",
            "",
            "t1",
            "n",
            4,
            "INT",
            9,
            10,
            10,
            0,
            1,
            None,
            "NULL",
            4,
            None,
            16384,
            1,
            "YES",
        ),
        (
            "",
            "",
            "t1",
            "v",
            12,
            "VARCHAR(10)",
            10,
            10,
            10,
            0,
            1,
            None,
            "NULL",
            12,
            None,
            16384,
            2,
            "YES",
        ),
    ]
    columns = [tuple(r) for r in resp]
    assert expectd == columns


@pytest.mark.parametrize("db", pytest.db_list)
@pytest.mark.asyncio
async def test_executemany(conn):
    cur = await conn.cursor()
    await cur.execute("CREATE TABLE t1(a int, b VARCHAR(10))")
    # TODO: figure out why it is possible to insert only strings... but not int
    params = [(str(i), str(i)) for i in range(1, 6)]
    await cur.executemany("INSERT INTO t1(a, b) VALUES (?, ?)", params)
    await cur.execute("SELECT COUNT(*) FROM t1")
    count = await cur.fetchone()
    assert count[0] == len(params)

    await cur.execute("SELECT a, b FROM t1 ORDER BY a")
    rows = await cur.fetchall()
    assert count[0] == len(rows)

    for param, row in zip(params, rows):
        assert int(param[0]) == row[0]
        assert param[1] == row[1]
    await cur.execute("DROP TABLE t1;")


@pytest.mark.parametrize("db", ["sqlite"])
@pytest.mark.asyncio
async def test_procedures_empty(conn, table):
    cur = await conn.cursor()
    await cur.procedures()
    resp = await cur.fetchall()
    assert resp == []


@pytest.mark.parametrize("db", ["sqlite"])
@pytest.mark.asyncio
async def test_procedureColumns_empty(conn, table):
    cur = await conn.cursor()
    await cur.procedureColumns()
    resp = await cur.fetchall()
    assert resp == []


@pytest.mark.parametrize("db", ["sqlite"])
@pytest.mark.asyncio
async def test_primaryKeys_empty(conn, table):
    cur = await conn.cursor()
    await cur.primaryKeys("t1", "t1", "t1")
    resp = await cur.fetchall()
    assert resp == []


@pytest.mark.parametrize("db", ["sqlite"])
@pytest.mark.asyncio
async def test_foreignKeys_empty(conn, table):
    cur = await conn.cursor()
    await cur.foreignKeys("t1")
    resp = await cur.fetchall()
    assert resp == []


@pytest.mark.asyncio
async def test_getTypeInfo_empty(conn, table):
    cur = await conn.cursor()
    await cur.getTypeInfo(pyodbc.SQL_CHAR)
    resp = await cur.fetchall()
    expected = [
        (
            "char",
            1,
            255,
            "'",
            "'",
            "length",
            1,
            0,
            3,
            None,
            0,
            0,
            "char",
            None,
            None,
            1,
            0,
            None,
            None,
        )
    ]
    type_info = [tuple(r) for r in resp]
    assert type_info == expected
