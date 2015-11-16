.. aioodbc documentation master file, created by
   sphinx-quickstart on Sun Jan 18 22:02:31 2015.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

Welcome to aioodbc's documentation!
====================================

.. _GitHub: https://github.com/aio-libs/aioodbc
.. _asyncio: http://docs.python.org/3.4/library/asyncio.html
.. _aiopg: https://github.com/aio-libs/aiopg
.. _aio-libs: https://github.com/aio-libs


**aioodbc** is Python 3.5+ module that makes possible accessing ODBC_ databases
with asyncio_. It is rely on awesome pyodbc_ library, preserve same look and
feel. *aioodbc* was written `async/await` syntax (PEP492_) thus not
compatible with Python older then 3.5. Internally *aioodbc* employ threads
to avoid blocking the event loop, btw threads_ are not that bad as you think :)


Features
--------

* Implements *asyncio* :term:`DBAPI` *like* interface for
  :term:`MySQL`.  It includes :ref:`aioodbc-connection`,
  :ref:`aioodbc-cursors` and :ref:`aiomysql-pool` objects.
* Implements *optional* support for charming :term:`sqlalchemy`
  functional sql layer.

Basics
------

**aioodbc** based on :term:`PyMySQL` , and provides same api, you just need
to use  ``yield from conn.f()`` instead of just call ``conn.f()`` for
every method.

Properties are unchanged, so ``conn.prop`` is correct as well as
``conn.prop = val``.

See example:

.. code:: python

    import asyncio
    import aioodbc

    loop = asyncio.get_event_loop()

    @asyncio.coroutine
    def test_example():
        conn = yield from aioodbc.connect(host='127.0.0.1', port=3306,
                                           user='root', password='', db='mysql',
                                           loop=loop)

        cur = yield from conn.cursor()
        yield from cur.execute("SELECT Host,User FROM user")
        print(cur.description)
        r = yield from cur.fetchall()
        print(r)
        yield from cur.close()
        conn.close()

    loop.run_until_complete(test_example())


Installation
------------

.. code::

   pip3 install aioodbc

.. note:: :mod:`aioodbc` requires :term:`PyMySQL` library.


Also you probably want to use :mod:`aioodbc.sa`.

.. _aioodbc-install-sqlalchemy:

:mod:`aioodbc.sa` module is **optional** and requires
:term:`sqlalchemy`. You can install *sqlalchemy* by running::

  pip3 install sqlalchemy

Source code
-----------

The project is hosted on GitHub_

Please feel free to file an issue on `bug tracker
<https://github.com/aio-libs/aioodbc/issues>`_ if you have found a bug
or have some suggestion for library improvement.

The library uses `Travis <https://travis-ci.org/aio-libs/aioodbc>`_ for
Continious Integration and `Coveralls
<https://coveralls.io/r/jettify/aioodbc?branch=master>`_ for
coverage reports.


Dependencies
------------

- Python 3.3 and :mod:`asyncio` or Python 3.4+
- :term:`PyMySQL`
- aioodbc.sa requires :term:`sqlalchemy`.


Authors and License
-------------------

The ``aioodbc`` package is written by Nikolay Novik and aio-libs_ contributors.
It's MIT licensed.

Feel free to improve this package and send a pull request to GitHub_.

Contents:
---------

.. toctree::
   :maxdepth: 2

   examples
   glossary
   contributing

Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`
