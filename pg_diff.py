#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""PG Diff

Compare between two postgres databases, like all table row count, schema, and etc

Usage:
  pg_diff --type=T_NAME SOURCE_DSN TARGET_DSN
  pg_diff -h | --help
  pg_diff --version

Arguments:
  SOURCE_DSN     dsn for source database, like "host=xxx dbname=test user=postgres password=secret port=5432"
  TARGET_DSN     dsn for target database, like "host=xxx dbname=test user=postgres password=secret port=5432"

Options:
  --type=T_NAME  Type name to compare in category, valid input likes: table_name, table_count, table_schema, row_count.
  -h --help      Show help info.
  --version      Show version.
"""

from pprint import pprint
from threading import Thread
import subprocess

from docopt import docopt
import psycopg2
from schema import Schema, And, SchemaError
from deepdiff import DeepDiff


DIFF_TYPE_TABLE_COUNT = 'table_count'
DIFF_TYPE_TABLE_NAME = 'table_name'
DIFF_TYPE_ROW_COUNT = 'row_count'
DIFF_TYPE_TABLE_SCHEMA = 'table_schema'


class DBDiffBase(object):
    """Class to represent comparison data for database
    """

    def __init__(self, dsn):
        """Constructor for class Database

        Args:
            dsn: str, dsn url for pg database
        """
        self.dsn = dsn
        self.conn = None
        self.table_data = {}

    def create_conn(self):
        """Create db connection by dsn

        Returns:
            connection object
        """
        try:
            conn = psycopg2.connect(self.dsn)
        except:
            exit('Unable to connect to the database')

        return conn

    def load(self):
        """Load database table data based on diff type
        """
        raise NotImplementedError

    def diff(self, target):
        """Do diff between two databases

        Args:
            target: DBDiffBase object

        Returns:
            diff result
        """
        assert isinstance(target, DBDiffBase)

        thread_pool = [
            Thread(target=self.load),
            Thread(target=target.load),
        ]

        for t in thread_pool:
            t.start()

        for t in thread_pool:
            t.join()

        src_table_data = self.table_data
        target_table_data = target.table_data

        diff_result = DeepDiff(src_table_data, target_table_data)

        return diff_result


class DBTableRowCountDiff(DBDiffBase):
    """Diff class to represent table row count comparison data
    """

    CREATE_ROW_COUNT_FUNC_SQL = """
create or replace function
count_rows(schema text, tablename text) returns integer
as
$body$
declare
  result integer;
  query varchar;
begin
  query := 'SELECT count(1) FROM ' || schema || '.' || tablename;
  execute query into result;
  return result;
end;
$body$
language plpgsql
    """

    REMOVE_ROW_COUNT_RUNC_SQL = """
drop function if exists count_rows(text, text)
    """

    TABLE_INFO_WITH_ROW_COUNT_SQL = """
select
  table_schema,
  table_name,
  count_rows(table_schema, table_name)
from information_schema.tables
where
  table_schema not in ('pg_catalog', 'information_schema', 'bucardo')
  and table_type='BASE TABLE'
order by 2, 3 desc
    """

    def _load_row_count(self, connection):
        try:
            cur = connection.cursor()
            cur.execute(self.CREATE_ROW_COUNT_FUNC_SQL)
            cur.execute(self.TABLE_INFO_WITH_ROW_COUNT_SQL)

            rows = cur.fetchall()
            for row in rows:
                self.table_data[row[1]] = row[2]

            cur.execute(self.REMOVE_ROW_COUNT_RUNC_SQL)
        except Exception as e:
            exit('Load row count error, please check:\n{}'.format(e))

    def load(self):
        """Load database table data based on diff type
        """
        self.conn = self.create_conn()
        self._load_row_count(self.conn)


class DBTableSchemaDiff(DBDiffBase):
    """Diff class to represent table row count comparison data
    """
    TABLE_SCHEMA_PSQL_COMMAND = r'export PGPASSWORD={password}; psql -h {host} -U {user} -p {port} {dbname} -c "\d {table}"'

    TABLE_BASIC_INFO_SQL = """
select
  table_schema,
  table_name
from information_schema.tables
where
  table_schema not in ('pg_catalog', 'information_schema', 'bucardo')
  and table_type='BASE TABLE'
    """

    def _load_table_basic_info(self, connection):
        try:
            cur = connection.cursor()
            cur.execute(self.TABLE_BASIC_INFO_SQL)

            rows = cur.fetchall()
            for row in rows:
                self.table_data[row[1]] = row[0]
        except Exception as e:
            exit('Load table basic info error, please check:\n{}'.format(e))

    def _load_table_schema(self):
        """Use psql with meta command to fetch table schema, to execute `\d table`
        """
        # parse dsn for psql command
        kwargs = dict([item.split('=') for item in self.dsn.split()])

        try:
            for table in self.table_data:
                kwargs['table'] = table
                command = self.TABLE_SCHEMA_PSQL_COMMAND.format(**kwargs)

                schema = subprocess.check_output(command, shell=True)
                self.table_data[table] = schema
        except Exception as e:
            exit('Load table schema error, please check:\n{}'.format(e))

    def load(self):
        """Load database table data based on diff type
        """
        self.conn = self.create_conn()
        self._load_table_basic_info(self.conn)
        self._load_table_schema()


class DBTableBasicInfoDiff(DBDiffBase):
    """Diff class to represent table basic info comparison data, like table name, table count
    """

    TABLE_BASIC_INFO_SQL = """
select
  table_schema,
  table_name
from information_schema.tables
where
  table_schema not in ('pg_catalog', 'information_schema', 'bucardo')
  and table_type='BASE TABLE'
    """

    def _load_table_basic_info(self, connection):
        try:
            cur = connection.cursor()
            cur.execute(self.TABLE_BASIC_INFO_SQL)

            rows = cur.fetchall()
            for row in rows:
                self.table_data[row[1]] = row[0]
        except Exception as e:
            exit('Load table basic info error, please check:\n{}'.format(e))

    def load(self):
        """Load database table data based on diff type
        """
        self.conn = self.create_conn()
        self._load_table_basic_info(self.conn)


DiffClassMapper = {
    DIFF_TYPE_TABLE_COUNT: DBTableBasicInfoDiff,
    DIFF_TYPE_TABLE_NAME: DBTableBasicInfoDiff,
    DIFF_TYPE_ROW_COUNT: DBTableRowCountDiff,
    DIFF_TYPE_TABLE_SCHEMA: DBTableSchemaDiff,
}


def diff(src_dsn, target_dsn, diff_type):
    """Compare all tables row count between two dbs

    Args:
        src_dsn: str, dsn for postgres database, like "host=xxx dbname=test user=postgres password=secret port=5432"
        target_dsn: str, dsn for postgres database, like "host=xxx dbname=test user=postgres password=secret port=5432"
        diff_type: str, diff type

    Returns:
        diff result
    """
    diff_class = DiffClassMapper[diff_type]

    src_db = diff_class(src_dsn)
    target_db = diff_class(target_dsn)

    diff_result = src_db.diff(target_db)

    print('Diff Result:\n')

    if diff_result:
        pprint(diff_result, indent=2)
    else:
        print('They are the same.')


def _validate(args):
    """Do validation to args

    Args:
        args: dict, arguments dictionary

    Returns:
        dict, valid args
    """
    schema = Schema({
        '--type': And(str, lambda x: x in (
            DIFF_TYPE_TABLE_COUNT,
            DIFF_TYPE_TABLE_NAME,
            DIFF_TYPE_ROW_COUNT,
            DIFF_TYPE_TABLE_SCHEMA
        )),
        'SOURCE_DSN': And(str, len),
        'TARGET_DSN': And(str, len),
        '--version': And(bool),
        '--help': And(bool),
    })

    try:
        args = schema.validate(args)
    except SchemaError as e:
        exit('Validation error, please check:\n{}'.format(e))

    return args


def main():
    args = docopt(__doc__, version='PG Diff 0.1')

    args = _validate(args)

    kwargs = {
        'src_dsn': args['SOURCE_DSN'],
        'target_dsn': args['TARGET_DSN'],
        'diff_type': args['--type'],
    }

    diff(**kwargs)


if __name__ == '__main__':
    main()
