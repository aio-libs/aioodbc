import asyncio

import aioodbc


async def test_pool():
    dsn = "Driver=SQLite3;Database=sqlite_pool.db"
    pool = await aioodbc.create_pool(dsn=dsn)

    async with pool.acquire() as conn:
        cur = await conn.cursor()
        await cur.execute("SELECT 42;")
        r = await cur.fetchall()
        print(r)
        await cur.close()
        await conn.close()
    pool.close()
    await pool.wait_closed()


asyncio.run(test_pool())
