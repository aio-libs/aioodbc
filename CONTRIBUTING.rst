Contributing
============

Thanks for your interest in contributing to ``aioodbc``, there are multiple
ways and places you can contribute.

Reporting an Issue
------------------
If you have found issue with `aioodbc` please do
not hesitate to file an issue on the GitHub_ project. When filing your
issue please make sure you can express the issue with a reproducible test
case.

When reporting an issue we also need as much information about your environment
that you can include. We never know what information will be pertinent when
trying narrow down the issue. Please include at least the following
information:

* Version of `aioodbc` and `python`.
* Version of your ODBC database
* Version of database ODBC driver
* Version of unixODBC_
* Platform you're running on (OS X, Linux, Windows).


Instructions for contributors
-----------------------------


In order to make a clone of the GitHub_ repo: open the link and press the
"Fork" button on the upper-right menu of the web page.

I hope everybody knows how to work with git and github nowadays :)

Work flow is pretty straightforward:

  1. Clone the GitHub_ repo

  2. Make a change

  3. Make sure all tests passed

  4. Commit changes to own aioodbc clone

  5. Make pull request from github page for your clone

Preconditions for running aioodbc test suite
---------------------------------------------

We expect you to use a python virtual environment and docker_ to run
our tests.

There are several ways to make a virtual environment.

If you like to use *virtualenv* please run::

   $ cd aioodbc
   $ virtualenv --python=`which python3.5` venv

For standard python *venv*::

   $ cd aioodbc
   $ python3.5 -m venv venv

For *virtualenvwrapper*::

   $ cd aioodbc
   $ mkvirtualenv --python=`which python3.5` aioodbc

There are other tools like *pyvenv* but you know the rule of thumb
now: create a python3.5 virtual environment and activate it.

After that please install libraries required for development::

   $ pip install -r requirements-dev.txt

We also recommend to install *ipdb* but it's on your own::

   $ pip install ipdb

Congratulations, you are ready to run the test suite


Install database
----------------
You do not need to install any databases, docker_ will pull images and create
containers for you automatically, after the tests, containers will be removed.


Run aioodbc test suite
----------------------

After all the preconditions are met you can run tests typing the next
command::

   $ make test

Or if you want to run only one particular test::

    $ py.test tests/test_connection.py -k test_basic_cursor

The command at first will run the static and style checkers (sorry, we don't
accept pull requests with `pep8` or `pyflakes` errors).

On `flake8` success the tests will be run.

Please take a look on the produced output.

Any extra texts (print statements and so on) should be removed.


Tests coverage
--------------

We are trying hard to have good test coverage; please don't make it worse.

Use::

   $ make cov

to run test suite and collect coverage information. Once the command
has finished check your coverage at the file that appears in the last
line of the output:
``open file:///.../aioodbc/htmlcov/index.html``

Please go to the link and make sure that your code change is covered.


Documentation
-------------

We encourage documentation improvements.

Please before making a Pull Request about documentation changes run::

   $ make doc

Once it finishes it will output the index html page
``open file:///.../aioodbc/docs/_build/html/index.html``.

Go to the link and make sure your doc changes looks good.

The End
-------

After finishing all steps make a GitHub_ Pull Request, thanks.


.. _unixODBC: http://www.unixodbc.org/
.. _GitHub: https://github.com/aio-libs/aioodbc
.. _docker: https://docs.docker.com/engine/installation/
