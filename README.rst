|Build Status| |Coverage Status|

pg diff
=======

A simple tool to diff serveral properties of schemas in two postgresql
databases.

Now supported diff options are:

-  table\_name
-  table\_count
-  table\_schema
-  row\_count
-  table\_size
-  index\_size
-  ``table_total_size``

Why
===

Recently I worked on database migration things, and very need a tool to
analyze the consistency between source and target database. So I make
this tool to some simple comparison, and it looks good to me:)

Prerequisite
============

You need to install ``postgresql`` firstly, because ``pg_diff`` will use
``psycopg2`` to execute some commands.

Installation
============

``pip install pg_diff``

Usage
=====

``pg_diff --type=table_count 'host=source dbname=test user=postgres password=secret port=5432' 'host=target dbname=test user=postgres password=secret port=5432' --verbose``

::

    Usage:
      pg_diff --type=T_NAME SOURCE_DSN TARGET_DSN [--verbose]
      pg_diff -h | --help
      pg_diff --version

    Arguments:
      SOURCE_DSN     dsn for source database, like "host=xxx dbname=test user=postgres password=secret port=5432"
      TARGET_DSN     dsn for target database, like "host=xxx dbname=test user=postgres password=secret port=5432"

    Options:
      --type=T_NAME  Type name to compare in category, valid input likes: table_name, table_count, table_schema, row_count, table size. index size, table_total_size
      -h --help      Show help info.
      --verbose      Show verbose information.
      --version      Show version.

Implementation
==============

Mainly using libraries below to make this tool:

-  docopt==0.6.2
-  schema==0.6.5
-  deepdiff==2.5.1
-  psycopg2==2.6.2

And I use some SQL to query the status of schema, like table size, index
size and etc.

Contribution
============

#. Fork the repository on GitHub.
#. Make a branch off of master and commit your changes to it.
#. Run the tests with ``tox``

-  Either use ``tox`` to build against all supported Python versions (if
   you have them installed) or use ``tox -e py{version}`` to test
   against a specific version, e.g., ``tox -e py27`` or ``tox -e py33``.

#. Submit a Pull Request to the master branch on GitHub.

If you’d like to have a development environment for ``pg_diff``, you
should create a virtualenv and then do ``pip install -e .`` from within
the directory.

Lisence
=======

MIT Lisence

.. |Build Status| image:: https://travis-ci.org/hanks/pg_diff.svg?branch=master
   :target: https://travis-ci.org/hanks/pg_diff
.. |Coverage Status| image:: https://coveralls.io/repos/github/hanks/pg_diff/badge.svg?branch=master
   :target: https://coveralls.io/github/hanks/pg_diff?branch=master