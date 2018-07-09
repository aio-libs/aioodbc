.. _tuning:


********
Configuration Tuning
********


   after_created

      When calling ``aioodbc.connect`` it is possible to pass an async
      unary function as a parameter for ``after_created``. This allows
      you to configure additional attributes on the underlying
      pyodbc connection such as ``.setencoding`` or ``.setdecoding``.

   TheadPoolExecutor

       When using ``aoiodbc.create_pool`` it is considered a
       good practice to use ``ThreadPoolExecutor`` from
       ``concurrent.futures`` to create worker threads that
       are dedicated for database work allowing default threads
       to do other work and prevent competition between database
       and default workers.

