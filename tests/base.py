import asyncio
import os
import aioodbc
from tests._testutils import BaseTest


class AIOPyMySQLTestCase(BaseTest):


    def setUp(self):
        super(AIOPyMySQLTestCase, self).setUp()
        self.host = os.environ.get('MYSQL_HOST', 'localhost')
        self.port = os.environ.get('MYSQL_PORT', 3306)
        self.user = os.environ.get('MYSQL_USER', 'root')
        self.db = os.environ.get('MYSQL_DB', 'test_pymysql')
        self.other_db = os.environ.get('OTHER_MYSQL_DB', 'test_pymysql2')
        self.password = os.environ.get('MYSQL_PASSWORD', '')


    def tearDown(self):
        for connection in self.connections:
            self.loop.run_until_complete(connection.ensure_closed())
        self.doCleanups()
        super(AIOPyMySQLTestCase, self).tearDown()

    @asyncio.coroutine
    def connect(self, **kwargs):

        conn = yield from aioodbc.connect('Driver=SQLite;Database=sqlite.db',
                                          **kwargs)
        self.addCleanup(conn.close)
        return conn

