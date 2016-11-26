[![Build Status](https://travis-ci.org/hanks/pg_diff.svg?branch=master)](https://travis-ci.org/hanks/pg_diff) [![Coverage Status](https://coveralls.io/repos/github/hanks/pg_diff/badge.svg?branch=master)](https://coveralls.io/github/hanks/pg_diff?branch=master)
pg diff
=======
A simple tool to diff serveral properties of schemas in two postgresql databases.

Now supported diff options are:

* table_name 
* table_count
* table_schema
* row_count
* table_size 
* index_size
* `table_total_size`

# Why

Recently I worked on database migration things, and very need a tool to analyze the consistency between source and target database. So I make this tool to some simple comparison, and it looks good to me:)

# Prerequisite

You need to install `postgresql` firstly, because `pg_diff` will use `psycopg2` to execute some commands.

# Installation

`pip install pg_diff`

# Usage

```
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
```

# Implementation

Mainly using libraries below to make this tool:

* docopt==0.6.2
* schema==0.6.5
* deepdiff==2.5.1
* psycopg2==2.6.2

And I use some SQL to query the status of schema, like table size, index size and etc.

# Contribution
**Waiting for your pull requests**

# Lisence
MIT Lisence