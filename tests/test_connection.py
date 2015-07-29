import asyncio
import pyodbc
from aioodbc.cursor import Cursor
from tests import base

from tests._testutils import run_until_complete


class TestConversion(base.ODBCTestCase):

    @run_until_complete
    def test_connect(self):
        conn = yield from self.connect()
        self.assertIs(conn.loop, self.loop)
        self.assertEqual(conn.autocommit, False)
        self.assertEqual(conn.timeout, 0)
        self.assertEqual(conn.closed, False)
        yield from conn.close()

    @run_until_complete
    def test_basic_cursor(self):
        conn = yield from self.connect()
        cursor = yield from conn.cursor()
        sql = 'SELECT 10;'
        yield from cursor.execute(sql)
        (resp, ) = yield from cursor.fetchone()
        yield from conn.close()
        self.assertEqual(resp, 10)

    @run_until_complete
    def test_default_event_loop(self):
        asyncio.set_event_loop(self.loop)

        conn = yield from self.connect(no_loop=True)
        cur = yield from conn.cursor()
        self.assertIsInstance(cur, Cursor)
        yield from cur.execute('SELECT 1;')
        (ret, ) = yield from cur.fetchone()
        self.assertEqual(1, ret)
        self.assertIs(conn._loop, self.loop)
        yield from conn.close()

    @run_until_complete
    def test_close_twice(self):
        conn = yield from self.connect()
        yield from conn.close()
        yield from conn.close()
        self.assertTrue(conn.closed)

    @run_until_complete
    def test_execute(self):
        conn = yield from self.connect()
        cur = yield from conn.execute('SELECT 10;')
        (resp, ) = yield from cur.fetchone()
        yield from conn.close()
        self.assertEqual(resp, 10)
        self.assertTrue(conn.closed)

    @run_until_complete
    def test_getinfo(self):
        conn = yield from self.connect()
        data = yield from conn.getinfo(pyodbc.SQL_CREATE_TABLE)
        self.assertEqual(data, 1793)
        yield from conn.close()

    @run_until_complete
    def test_output_conversion(self):
        def convert(value):
            # `value` will be a string.  We'll simply add an X at the
            # beginning at the end.
            return 'X' + value + 'X'
        conn = yield from self.connect()
        yield from conn.add_output_converter(pyodbc.SQL_VARCHAR, convert)
        cur = yield from conn.cursor()

        yield from cur.execute("DROP TABLE IF EXISTS t1;")
        yield from cur.execute("CREATE TABLE t1(n INT, v VARCHAR(10))")
        yield from cur.execute("INSERT INTO t1 VALUES (1, '123.45')")
        yield from cur.execute("SELECT v FROM t1")
        (value, ) = yield from cur.fetchone()

        self.assertEqual(value, 'X123.45X')

        # Now clear the conversions and try again.  There should be
        # no Xs this time.
        yield from conn.clear_output_converters()
        yield from cur.execute("SELECT v FROM t1")
        (value, ) = yield from cur.fetchone()
        self.assertEqual(value, '123.45')
        yield from conn.close()

    @run_until_complete
    def test_autocommit(self):
        conn = yield from self.connect(autocommit=True)
        self.assertEqual(conn.autocommit, True)
        yield from conn.close()
