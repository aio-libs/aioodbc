import asyncio
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
        yield from cur.execute('SELECT 1')
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

