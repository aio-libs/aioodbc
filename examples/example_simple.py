import asyncio
import aioodbc


loop = asyncio.get_event_loop()


@asyncio.coroutine
def test_example():
    dsn = 'Driver=SQLite;Database=sqlite.db'
    conn = yield from aioodbc.connect(dsn=dsn, loop=loop)

    cur = yield from conn.cursor()
    yield from cur.execute("SELECT 42;")
    r = yield from cur.fetchall()
    print(r)
    yield from cur.close()
    yield from conn.ensure_closed()

loop.run_until_complete(test_example())
