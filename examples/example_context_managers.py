import asyncio

import aioodbc


async def test_example():
    dsn = "Driver=SQLite3;Database=sqlite_context.db"

    async with aioodbc.create_pool(dsn=dsn) as pool:
        async with pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute("SELECT 42 AS age;")
                val = await cur.fetchone()
                print(val)
                print(val.age)


asyncio.run(test_example())
