aioodbc
=======
.. image:: https://github.com/aio-libs/aioodbc/workflows/CI/badge.svg
   :target: https://github.com/aio-libs/aioodbc/actions?query=workflow%3ACI
   :alt: GitHub Actions status for master branch
.. image:: https://codecov.io/gh/aio-libs/aioodbc/branch/master/graph/badge.svg
    :target: https://codecov.io/gh/aio-libs/aioodbc
.. image:: https://img.shields.io/pypi/v/aioodbc.svg
    :target: https://pypi.python.org/pypi/aioodbc
.. image:: https://img.shields.io/pypi/pyversions/aioodbc.svg
    :target: https://pypi.org/project/aioodbc
.. image:: https://badges.gitter.im/Join%20Chat.svg
    :target: https://gitter.im/aio-libs/Lobby
    :alt: Chat on Gitter

**aioodbc** is a Python 3.7+ module that makes it possible to access ODBC_ databases
with asyncio_. It relies on the awesome pyodbc_ library and preserves the same look and
feel. Internally *aioodbc* employs threads to avoid blocking the event loop,
threads_ are not that as bad as you think!. Other drivers like motor_ use the
same approach.

**aioodbc** is fully compatible and tested with uvloop_. Take a look at the test
suite, all tests are executed with both the default event loop and uvloop_.


Basic Example
-------------

**aioodbc** is based on pyodbc_ and provides the same api, you just need
to use  ``yield from conn.f()`` or ``await conn.f()`` instead of ``conn.f()``

Properties are unchanged, so ``conn.prop`` is correct as well as
``conn.prop = val``.


.. code:: python

    import asyncio

    import aioodbc


    async def test_example():
        dsn = "Driver=SQLite;Database=sqlite.db"
        conn = await aioodbc.connect(dsn=dsn)

        cur = await conn.cursor()
        await cur.execute("SELECT 42 AS age;")
        rows = await cur.fetchall()
        print(rows)
        print(rows[0])
        print(rows[0].age)
        await cur.close()
        await conn.close()


    asyncio.run(test_example())


Connection Pool
---------------
Connection pooling is ported from aiopg_ and relies on PEP492_ features:

.. code:: python

    import asyncio

    import aioodbc


    async def test_pool():
        dsn = "Driver=SQLite3;Database=sqlite.db"
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


Context Managers
----------------
`Pool`, `Connection` and `Cursor` objects support the context management
protocol:

.. code:: python

    import asyncio

    import aioodbc


    async def test_example():
        dsn = "Driver=SQLite;Database=sqlite.db"

        async with aioodbc.create_pool(dsn=dsn) as pool:
            async with pool.acquire() as conn:
                async with conn.cursor() as cur:
                    await cur.execute("SELECT 42 AS age;")
                    val = await cur.fetchone()
                    print(val)
                    print(val.age)


    asyncio.run(test_example())


Installation
------------

In a linux environment pyodbc_ (hence *aioodbc*) requires the unixODBC_ library.
You can install it using your package manager, for example::

      $ sudo apt-get install unixodbc
      $ sudo apt-get install unixodbc-dev

Then::

   pip install aioodbc


Run tests
---------
To run tests locally without docker, install `unixodbc` and `sqlite` driver::

      $ sudo apt-get install unixodbc
      $ sudo apt-get install libsqliteodbc

Create virtualenv and install package with requirements::

      $ pip install -r requirements-dev.txt

Run tests, lints etc::

      $ make fmt
      $ make lint
      $ make test


Other SQL Drivers
-----------------

* aiopg_ - asyncio client for PostgreSQL
* aiomysql_ - asyncio client form MySQL


Requirements
------------

* Python_ 3.7+
* pyodbc_
* uvloop_ (optional)


.. _Python: https://www.python.org
.. _asyncio: http://docs.python.org/3.4/library/asyncio.html
.. _pyodbc: https://github.com/mkleehammer/pyodbc
.. _uvloop: https://github.com/MagicStack/uvloop
.. _ODBC: https://en.wikipedia.org/wiki/Open_Database_Connectivity
.. _aiopg: https://github.com/aio-libs/aiopg
.. _aiomysql: https://github.com/aio-libs/aiomysql
.. _PEP492: https://www.python.org/dev/peps/pep-0492/
.. _unixODBC: http://www.unixodbc.org/
.. _threads: http://techspot.zzzeek.org/2015/02/15/asynchronous-python-and-databases/
.. _docker: https://docs.docker.com/engine/installation/
.. _motor: https://emptysqua.re/blog/motor-0-7-beta/
