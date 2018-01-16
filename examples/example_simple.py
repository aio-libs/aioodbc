import asyncio
import aioodbc


loop = asyncio.get_event_loop()


async def test_example():
    dsn = 'Driver=SQLite;Database=sqlite.db'
    conn = await aioodbc.connect(dsn=dsn, loop=loop)

    cur = await conn.cursor()
    await cur.execute("SELECT 42 AS age;")
    rows = await cur.fetchall()
    print(rows)
    print(rows[0])
    print(rows[0].age)
    await cur.close()
    await conn.close()

loop.run_until_complete(test_example())
