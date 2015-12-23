import asyncio
import aioodbc


loop = asyncio.get_event_loop()


async def test_pool():
    dsn = 'Driver=SQLite;Database=sqlite.db'
    pool = await aioodbc.create_pool(dsn=dsn, loop=loop)

    async with pool.acquire() as conn:
        cur = await conn.cursor()
        await cur.execute("SELECT 42;")
        r = await cur.fetchall()
        print(r)
        await cur.close()
        await conn.close()
    pool.close()
    await pool.wait_closed()

loop.run_until_complete(test_pool())
