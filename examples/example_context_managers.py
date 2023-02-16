import asyncio
import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
import aioodbc


loop = asyncio.get_event_loop()


async def test_example():
    dsn = 'Driver=SQLite;Database=sqlite.db'

    async with aioodbc.create_pool(dsn=dsn, loop=loop) as pool:
        async with pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute('SELECT 42 AS age;')
                val = await cur.fetchone()
                print(val)
                print(val.age)

loop.run_until_complete(test_example())
