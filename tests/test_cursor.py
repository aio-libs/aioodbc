import asyncio
import pyodbc
from aioodbc.cursor import Cursor
from tests import base

from tests._testutils import run_until_complete


class TestCursor(base.ODBCTestCase):

    @run_until_complete
    def test_cursor(self):
        conn = yield from self.connect()
        cursor = yield from conn.cursor()
        self.assertIs(cursor.connection, conn)
        self.assertIs(cursor._loop, conn.loop)
        self.assertEqual(cursor.arraysize, 1)
        self.assertEqual(cursor.rowcount, -1)
        yield from conn.close()

    @run_until_complete
    def test_close(self):
        conn = yield from self.connect()
        cursor = yield from conn.cursor()
        self.assertFalse(cursor.closed)
        yield from cursor.close()
        self.assertTrue(cursor.closed)
        yield from conn.close()
