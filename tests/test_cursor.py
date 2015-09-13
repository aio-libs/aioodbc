import pytest
from pyodbc import OperationalError


@pytest.mark.run_loop
def test_cursor(conn):
    cur = yield from conn.cursor()
    assert cur.connection is conn
    assert cur._loop, conn.loop
    assert cur.arraysize == 1
    assert cur.rowcount == -1

    r = yield from cur.setinputsizes()
    assert r is None

    yield from cur.setoutputsize()
    assert r is None
    yield from cur.close()


@pytest.mark.run_loop
def test_execute_on_closed_cursor(conn):
    cur = yield from conn.cursor()
    yield from cur.close()
    with pytest.raises(OperationalError):
        yield from cur.execute('SELECT 1;')


@pytest.mark.run_loop
def test_close(conn):
    cur = yield from conn.cursor()
    assert not cur.closed
    yield from cur.close()
    yield from cur.close()
    assert cur.closed


@pytest.mark.run_loop
def test_description(conn):
    cur = yield from conn.cursor()
    assert cur.description is None
    yield from cur.execute('SELECT 1;')
    expected = (('1', float, None, 54, 54, 0, True), )
    assert cur.description == expected
    yield from cur.close()


@pytest.mark.run_loop
def test_description_with_real_table(conn, table):
    cur = yield from conn.cursor()
    yield from cur.execute("SELECT * FROM t1;")

    expected = (('n', int, None, 10, 10, 0, True),
                ('v', str, None, 10, 10, 0, True))
    assert cur.description == expected
    yield from cur.close()


@pytest.mark.run_loop
def test_rowcount_with_table(conn, table):
    cur = yield from conn.cursor()
    yield from cur.execute("SELECT * FROM t1;")
    yield from cur.fetchall()
    # sqlite does not provide working rowcount attribute
    # http://stackoverflow.com/questions/4911404/in-pythons-sqlite3-
    # module-why-cant-cursor-rowcount-tell-me-the-number-of-ro
    # TODO: figure out for proper test
    assert cur.rowcount in (0, 2)
    yield from cur.close()


@pytest.mark.run_loop
def test_arraysize(conn):
    cur = yield from conn.cursor()
    assert 1 == cur.arraysize
    cur.arraysize = 10
    assert 10 == cur.arraysize
    yield from cur.close()


@pytest.mark.run_loop
def test_fetchall(conn, table):
    cur = yield from conn.cursor()
    yield from cur.execute("SELECT * FROM t1;")
    resp = yield from cur.fetchall()
    expected = [(1, '123.45'), (2, 'foo')]

    for row, exp in zip(resp, expected):
        assert exp == tuple(row)

    yield from cur.close()


@pytest.mark.run_loop
def test_fetchmany(conn, table):
    cur = yield from conn.cursor()
    yield from cur.execute("SELECT * FROM t1;")
    resp = yield from cur.fetchmany(1)
    expected = [(1, '123.45')]

    for row, exp in zip(resp, expected):
        assert exp == tuple(row)

    yield from cur.close()


@pytest.mark.run_loop
def test_fetchone(conn, table):
    cur = yield from conn.cursor()
    yield from cur.execute("SELECT * FROM t1;")
    resp = yield from cur.fetchone()
    expected = (1, '123.45')

    assert expected == tuple(resp)
    yield from cur.close()


@pytest.mark.run_loop
def test_tables(conn, table):
    cur = yield from conn.cursor()
    yield from cur.tables()
    resp = yield from cur.fetchall()
    expectd = (None, None, 't1', 'TABLE', None)
    assert len(resp) == 1, resp
    assert expectd == tuple(resp[0]), resp


@pytest.mark.run_loop
def test_cursor_rollback(conn, table):

    cur = yield from conn.cursor()
    yield from cur.execute("INSERT INTO t1 VALUES (3, '123.45');")
    yield from cur.execute("SELECT v FROM t1 WHERE n=3;")
    (value, ) = yield from cur.fetchone()
    assert value == '123.45'

    yield from cur.rollback()
    yield from cur.execute("SELECT v FROM t1 WHERE n=3;")
    value = yield from cur.fetchone()
    assert value is None


@pytest.mark.run_loop
def test_columns(conn, table):
    cur = yield from conn.cursor()
    yield from cur.columns()
    resp = yield from cur.fetchall()
    expectd = [('', '', 't1', 'n', 4, 'INT', 9, 10, 10, 0, 1, None,
                'NULL', 4, None, 16384, 1, 'YES'),
               ('', '', 't1', 'v', 12, 'VARCHAR(10)', 10, 10, 10, 0, 1, None,
                'NULL', 12, None, 16384, 2, 'YES')]
    columns = [tuple(r) for r in resp]
    assert expectd == columns
