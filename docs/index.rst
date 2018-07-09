.. aioodbc documentation master file, created by
   sphinx-quickstart on Sun Jan 18 22:02:31 2015.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

Welcome to aioodbc's documentation!
===================================

.. _GitHub: https://github.com/aio-libs/aioodbc
.. _asyncio: http://docs.python.org/3.4/library/asyncio.html
.. _aiopg: https://github.com/aio-libs/aiopg
.. _aio-libs: https://github.com/aio-libs
.. _pyodbc: https://github.com/mkleehammer/pyodbc
.. _PEP492: https://www.python.org/dev/peps/pep-0492/
.. _unixODBC: http://www.unixodbc.org/
.. _threads: http://techspot.zzzeek.org/2015/02/15/asynchronous-python-and-databases/


**aioodbc** is Python 3.5+ module that makes possible accessing ODBC_ databases
with asyncio_. It is rely on awesome pyodbc_ library, preserve same look and
feel. *aioodbc* was written `async/await` syntax (PEP492_) thus not
compatible with Python older then 3.5. Internally *aioodbc* employ threads
to avoid blocking the event loop, btw threads_ are not that bad as you think :)


Features
--------
* Implements `asyncio` :term:`DBAPI` *like* interface for
  :term:`ODBC`.  It includes :ref:`aioodbc-connection`,
  :ref:`aioodbc-cursor` and :ref:`aioodbc-pool` objects.
* Support connection pooling.


Source code
-----------

The project is hosted on GitHub_

Please feel free to file an issue on `bug tracker
<https://github.com/aio-libs/aioodbc/issues>`_ if you have found a bug
or have some suggestion for library improvement.

The library uses `Travis <https://travis-ci.org/aio-libs/aioodbc>`_ for
Continious Integration and `Coveralls
<https://coveralls.io/r/aio-libs/aioodbc?branch=master>`_ for
coverage reports.


Dependencies
------------

- Python 3.5 (PEP492_ coroutines)
- pyodbc_
- unixODBC_


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
   tuning
   glossary
   contributing

Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`
