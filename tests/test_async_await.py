

def test_cursor(loop, conn, table):

    async def go():
        ret = []

        cur = await conn.cursor()
        await cur.execute('SELECT * FROM t1;')

        assert not cur.closed
        async with cur:
            async for i in cur:
                ret.append(i)
        expected = [tuple(r) for r in ret]
        assert [(1, '123.45'), (2, 'foo')] == expected
        assert cur.closed

    loop.run_until_complete(go())


def test_cursor_lightweight(loop, conn, table):

    async def go():

        cur = await conn.cursor()
        await cur.execute('SELECT * FROM t1;')

        assert not cur.closed
        async with cur:
            pass

        assert cur.closed

    loop.run_until_complete(go())


def test_cursor_awit(loop, conn, table):

    async def go():
        async with await conn.cursor() as cur:
            await cur.execute('SELECT * FROM t1;')
            assert not cur.closed
            pass

        assert cur.closed

    loop.run_until_complete(go())


def test_connection(loop, conn):

    async def go():
        assert not conn.closed
        async with conn:
            pass

        assert conn.closed

    loop.run_until_complete(go())
