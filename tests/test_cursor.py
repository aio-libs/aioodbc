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

    @run_until_complete
    def test_description(self):
        conn = yield from self.connect()
        cursor = yield from conn.cursor()
        self.assertEqual(cursor.description, None)
        yield from cursor.execute('SELECT 1;')
        expected = (('1', float, None, 54, 54, 0, True), )
        self.assertEqual(cursor.description, expected)
        yield from cursor.close()
        yield from conn.close()

    @run_until_complete
    def test_description_with_real_table(self):
        conn = yield from self.connect()

        cur = yield from conn.cursor()
        yield from cur.execute("DROP TABLE t1;")
        yield from cur.execute("CREATE TABLE t1(n INT, v VARCHAR(10));")
        yield from cur.execute("INSERT INTO t1 VALUES (1, '123.45');")
        yield from conn.commit()
        yield from cur.close()

        cur = yield from conn.cursor()
        yield from cur.execute("SELECT * FROM t1;")

        expected = (('n', int, None, 10, 10, 0, True),
                    ('v', str, None, 10, 10, 0, True))
        self.assertEqual(cur.description, expected)
        yield from conn.close()
