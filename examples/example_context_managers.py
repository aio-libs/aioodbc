import asyncio
import aioodbc


loop = asyncio.get_event_loop()


async def test_example():
    dsn = 'Driver=SQLite;Database=sqlite.db'

    async with aioodbc.create_pool(dsn=dsn, loop=loop) as pool:
        async with pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute('SELECT 42;')
                val = await cur.fetchone()
                print(val)

loop.run_until_complete(test_example())
