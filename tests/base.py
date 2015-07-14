import asyncio
import os
import aioodbc
from tests._testutils import BaseTest


class ODBCTestCase(BaseTest):

    def setUp(self):
        super(ODBCTestCase, self).setUp()
        self.dsn = os.environ.get('DSN', 'Driver=SQLite;Database=sqlite.db')

    def tearDown(self):
        self.doCleanups()
        super(ODBCTestCase, self).tearDown()

    @asyncio.coroutine
    def connect(self, **kwargs):
        conn = yield from aioodbc.connect(self.dsn, loop=self.loop, **kwargs)
        return conn
