aioodbc
=======
.. image:: https://travis-ci.org/jettify/aioodbc.svg?branch=master
    :target: https://travis-ci.org/jettify/aioodbc
.. image:: https://coveralls.io/repos/jettify/aioodbc/badge.svg?branch=master&service=github
    :target: https://coveralls.io/github/jettify/aioodbc?branch=master

**aioodbc** is Python module that makes possible accessing ODBC_ databases
with asyncio_. It is rely on awesome pyodbc_ library, preserve same look and
feel.


Basic Example
-------------

**aioodbc** based on pyodbc_ , and provides same api, you just need
to use  ``yield from conn.f()`` or ``await conn.f()`` instead of just
call ``conn.f()`` for every method.

Properties are unchanged, so ``conn.prop`` is correct as well as
``conn.prop = val``.


.. code:: python

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


Connection Pool
---------------
Connection pooling ported from aiopg_ and rely on PEP492_ features:

.. code:: python

    import asyncio
    import aioodbc


    loop = asyncio.get_event_loop()


    async def test_pool():
        dsn = 'Driver=SQLite;Database=sqlite.db'
        pool = await aioodbc.create_pool(dsn=dsn, loop=loop)

        async with (await pool) as conn:
            cur = await conn.cursor()
            await cur.execute("SELECT 42;")
            r = await cur.fetchall()
            print(r)
            await cur.close()
            await conn.ensure_closed()
        pool.close()
        await pool.wait_closed()

    loop.run_until_complete(test_example())


Installation
------------

.. code::

   pip3 install git+https://github.com/jettify/aioodbc.git

.. note::  in Linux environment pyodbc_ (hence *aioodbc*) requires unixODBC_
           library.

   You can use standard one from your distro like::

      $ sudo apt-get install unixodbc

   but if you like to use virtual environments
   (`virtualenvwrapper`, `virtualenv` or `venv`) you
   probably have to install :term:`unixodbc-dev` development package::

      $ sudo apt-get install unixodbc-dev


Other SQL Drivers
-----------------

* aiopg_ - asyncio client for PostgreSQL
* aioodbc_ - asyncio client form MySQL


Requirements
------------

* Python_ 3.3+
* asyncio_ or Python_ 3.4+
* pyodbc_


.. _Python: https://www.python.org
.. _asyncio: http://docs.python.org/3.4/library/asyncio.html
.. _pyodbc: https://github.com/mkleehammer/pyodbc
.. _ODBC: https://en.wikipedia.org/wiki/Open_Database_Connectivity
.. _aiopg: https://github.com/aio-libs/aiopg
.. _aioodbc: https://github.com/aio-libs/aioodbc
.. _PEP492: https://www.python.org/dev/peps/pep-0492/
.. _unixODBC: http://www.unixodbc.org/
