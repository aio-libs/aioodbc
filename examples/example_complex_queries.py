import asyncio

import aioodbc

dsn = "Driver=SQLite3;Database=sqlite_complex.db"


async def test_init_database():
    """
    Initialize test database with sample schema/data to reuse in other tests.
    Make sure that in real applications you have database initialization
    file as separate *.sql script or rely on autogenerated code provided
    by your ORM.
    """
    async with aioodbc.connect(dsn=dsn, echo=True, autocommit=True) as conn:
        async with conn.cursor() as cur:
            sql = "CREATE TABLE IF NOT EXISTS t1(n INTEGER, v TEXT);"
            await cur.execute(sql)


async def test_error_without_context_managers():
    """
    When not using context manager you may end up having unclosed connections
    in case of any error which lead to resource leakage. To avoid
    `Unclosed connection` errors in your code always close after yourself.
    """
    conn = await aioodbc.connect(dsn=dsn)
    cur = await conn.cursor()

    try:
        await cur.execute("SELECT 42 AS;")
        rows = await cur.fetchall()
        print(rows)
    except Exception:
        pass
    finally:
        await cur.close()
        await conn.close()


async def test_insert_with_values():
    """
    When providing data to your SQL statement make sure to parametrize it with
    question marks placeholders. Do not use string formatting or make sure
    your data is escaped to prevent sql injections.

    NOTE: pyodbc does not support named placeholders syntax.
    """
    async with aioodbc.connect(dsn=dsn, echo=True, autocommit=True) as conn:
        async with conn.cursor() as cur:
            # Substitute sql markers with variables
            await cur.execute(
                "INSERT INTO t1(n, v) VALUES(?, ?);", ("2", "test 2")
            )
            # NOTE: make sure to pass variables as tuple of strings even if
            # your data types are different to prevent
            # pyodbc.ProgrammingError errors. You can even do like this
            values = (3, "test 3")
            await cur.execute(
                "INSERT INTO t1(n, v) VALUES(?, ?);", *map(str, values)
            )

            # Retrieve id of last inserted row
            await cur.execute("SELECT last_insert_rowid();")
            result = await cur.fetchone()
            print(result[0])


async def test_commit():
    """
    When not using `autocommit` parameter do not forget to explicitly call
    this method for your changes to persist within database.
    """
    async with aioodbc.connect(dsn=dsn) as conn:
        async with conn.cursor() as cur:
            sql = 'INSERT INTO t1 VALUES(1, "test");'
            await cur.execute(sql)
            # Make sure your changes will be actually saved into database
            await cur.commit()

    async with aioodbc.connect(dsn=dsn) as conn:
        async with conn.cursor() as cur:
            sql_select = "SELECT * FROM t1;"
            await cur.execute(sql_select)
            # At this point without autocommiting you will not see
            # the data inserted above
            print(await cur.fetchone())


async def run_all():
    await test_init_database()
    await test_commit()
    await test_insert_with_values()
    await test_error_without_context_managers()


if __name__ == "__main__":
    asyncio.run(run_all())
