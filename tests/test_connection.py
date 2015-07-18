from tests import base

from tests._testutils import run_until_complete


class TestConversion(base.ODBCTestCase):
    @run_until_complete
    def test_connect(self):
        conn = yield from self.connect()
        self.assertIs(conn.loop, self.loop)
        self.assertEqual(conn.autocommit, False)
        self.assertEqual(conn.timeout, 0)
        yield from conn.close()

    @run_until_complete
    def test_basic_cursor(self):
        conn = yield from self.connect()
        cursor = yield from conn.cursor()
        sql = 'SELECT 10;'
        yield from cursor.execute(sql)
        resp = yield from cursor.fetchall()
        print(resp)
        yield from conn.close()
