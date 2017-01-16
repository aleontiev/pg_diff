#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""PG Diff

Compare between two postgres databases, like all table row count, schema, and etc

Usage:
  pg_diff --type=T_NAME SOURCE_DSN TARGET_DSN [--verbose]
  pg_diff -h | --help
  pg_diff --version

Arguments:
  SOURCE_DSN     dsn for source database, like "host=xxx dbname=test user=postgres password=secret port=5432"
  TARGET_DSN     dsn for target database, like "host=xxx dbname=test user=postgres password=secret port=5432"

Options:
  --type=T_NAME  Type name to compare in category, valid input likes: table_name, table_count, table_schema, row_count,
                 table size, index size, table_total_size, sequence
  -h --help      Show help info.
  --verbose      Show verbose information.
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
DIFF_TYPE_TABLE_SIZE = 'table_size'
DIFF_TYPE_INDEX_SIZE = 'index_size'
DIFF_TYPE_TABLE_TOTAL_SIZE = 'table_total_size'
DIFF_TYPE_SEQUENCE = 'sequence'


class DBDiffBase(object):
    """Class to represent comparison data for database
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

    def diff(self, target, verbose=False):
        """Do diff between two databases

        Args:
            target: DBDiffBase object
            verbose: boolean, show verbose diff result or not

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

        if verbose:
            print("Source: ", src_table_data)
            print('')
            print("Target: ", target_table_data)

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

    REMOVE_ROW_COUNT_FUNC_SQL = """
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
        except Exception as e:
            exit('Load row count error, please check:\n{}'.format(e))
        finally:
            cur.execute(self.REMOVE_ROW_COUNT_FUNC_SQL)

    def load(self):
        """Load database table data based on diff type
        """
        self.conn = self.create_conn()
        self._load_row_count(self.conn)


class DBSequenceDiff(DBDiffBase):
    """Diff class to represent sequence data
    """

    CREATE_GET_SEQUENCE_LAST_VALUE_FUNC_SQL = """
create or replace function get_seq_last_value(seq_name text)
returns integer as
$$
declare
  last_value integer;
  query varchar;
begin
  query := 'SELECT last_value FROM ' || seq_name;
  execute query into last_value;
  return last_value;
end;
$$
language plpgsql
    """

    REMOVE_GET_SEQUENCE_LAST_VALUE_FUNC_SQL = """
drop function if exists get_seq_last_value(text, text)
    """

    SEQUENCE_COUNT_SQL = """
SELECT c.relname, get_seq_last_value(c.relname) FROM pg_class c WHERE c.relkind = 'S' order by c.relname
    """

    def _load_sequence_count(self, connection):
        try:
            cur = connection.cursor()
            cur.execute(self.CREATE_GET_SEQUENCE_LAST_VALUE_FUNC_SQL)
            cur.execute(self.SEQUENCE_COUNT_SQL)

            rows = cur.fetchall()
            for row in rows:
                self.table_data[row[0]] = row[1]
        except Exception as e:
            exit('Load sequence error, please check:\n{}'.format(e))
        finally:
            cur.execute(self.REMOVE_GET_SEQUENCE_LAST_VALUE_FUNC_SQL)

    def load(self):
        """Load database table data based on diff type
        """
        self.conn = self.create_conn()
        self._load_sequence_count(self.conn)


class DBTableSchemaDiff(DBDiffBase):
    """Diff class to represent table row count comparison data
    """
    TABLE_SCHEMA_PSQL_COMMAND = r'PGPASSWORD={password} psql -h {host} -U {user} -p {port} {dbname} -c "\d {table}"'

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
                self.table_data[table] = self._format_raw_schema(schema, ['bucardo'])
        except Exception as e:
            exit('Load table schema error, please check:\n{}'.format(e))

    def _format_raw_schema(self, raw_schema, exclued_keywords=None):
        if exclued_keywords is None:
            exclued_keywords = []

        item_list = raw_schema.split('\n')
        item_list = [item.strip() for item in item_list if item]

        result = {
            'Columns:': [],
        }

        section_title_list = [
            'Indexes:',
            'Foreign-key constraints:',
            'Referenced by:',
            'Triggers:',
        ]

        index = 0
        length = len(item_list)

        # skip unnecessary lines
        if item_list[index].startswith('Table'):
            index += 1

        if item_list[index].startswith('Column'):
            index += 1

        if item_list[index].startswith('---'):
            index += 1

        # parse table columns
        item = item_list[index]
        while item not in section_title_list:
            result['Columns:'].append([element.strip() for element in item.split('|')])
            index += 1
            if index >= length:
                break
            item = item_list[index]
        # sort Columns section
        result['Columns:'].sort()

        # parse other sections
        while index < length:
            section = item_list[index]
            result[section] = []
            index += 1

            item = item_list[index]
            while item not in section_title_list:
                is_excluded = False

                # check excluded keywords
                for keyword in exclued_keywords:
                    if keyword in item:
                        is_excluded = True
                        break

                if not is_excluded:
                    result[section].append(item.strip())

                index += 1
                if index >= length:
                    break
                item = item_list[index]

            # sort result after processing one section
            result[section].sort()

        return result

    def load(self):
        """Load database table data based on diff type
        """
        self.conn = self.create_conn()
        self._load_table_basic_info(self.conn)
        self._load_table_schema()


class DBTableBasicInfoDiff(DBDiffBase):
    """Diff class to represent table basic info comparison data, like table name, table count
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


class DBTableSizeDiff(DBDiffBase):
    """Diff class to represent table size comparison data
    """

    TABLE_SIZE_COUNT_SQL = """
    select
      table_schema,
      table_name,
      pg_size_pretty(pg_table_size(table_name))
    from information_schema.tables
    where
      table_schema not in ('pg_catalog', 'information_schema', 'bucardo')
      and table_type='BASE TABLE'
    order by 2, 3 desc
    """

    def _load_tabale_size_info(self, connection):
        try:
            cur = connection.cursor()
            cur.execute(self.TABLE_SIZE_COUNT_SQL)

            rows = cur.fetchall()
            for row in rows:
                self.table_data[row[1]] = row[2]
        except Exception as e:
            exit('Load table size error, please check:\n{}'.format(e))

    def load(self):
        """Load database table data based on diff type
        """
        self.conn = self.create_conn()
        self._load_tabale_size_info(self.conn)


class DBIndexSizeDiff(DBDiffBase):
    """Diff class to represent index size comparison data
    """

    INDEX_SIZE_COUNT_SQL = """
    select
      table_schema,
      table_name,
      pg_size_pretty(pg_indexes_size(table_name))
    from information_schema.tables
    where
      table_schema not in ('pg_catalog', 'information_schema', 'bucardo')
      and table_type='BASE TABLE'
    order by 2, 3 desc
    """

    def _load_index_size_info(self, connection):
        try:
            cur = connection.cursor()
            cur.execute(self.INDEX_SIZE_COUNT_SQL)

            rows = cur.fetchall()
            for row in rows:
                self.table_data[row[1]] = row[2]
        except Exception as e:
            exit('Load index size error, please check:\n{}'.format(e))

    def load(self):
        """Load database table data based on diff type
        """
        self.conn = self.create_conn()
        self._load_index_size_info(self.conn)


class DBTableTotalSizeDiff(DBDiffBase):
    """Diff class to represent table total size comparison data
    """

    TABLE_TOTAL_SIZE_COUNT_SQL = """
    select
      table_schema,
      table_name,
      pg_size_pretty(pg_total_relation_size(table_name))
    from information_schema.tables
    where
      table_schema not in ('pg_catalog', 'information_schema', 'bucardo')
      and table_type='BASE TABLE'
    order by 2, 3 desc
    """

    def _load_table_total_size_info(self, connection):
        try:
            cur = connection.cursor()
            cur.execute(self.INDEX_SIZE_COUNT_SQL)

            rows = cur.fetchall()
            for row in rows:
                self.table_data[row[1]] = row[2]
        except Exception as e:
            exit('Load index size error, please check:\n{}'.format(e))

    def load(self):
        """Load database table data based on diff type
        """
        self.conn = self.create_conn()
        self._load_table_total_size_info(self.conn)


DiffClassMapper = {
    DIFF_TYPE_TABLE_COUNT: DBTableBasicInfoDiff,
    DIFF_TYPE_TABLE_NAME: DBTableBasicInfoDiff,
    DIFF_TYPE_ROW_COUNT: DBTableRowCountDiff,
    DIFF_TYPE_TABLE_SCHEMA: DBTableSchemaDiff,
    DIFF_TYPE_TABLE_SIZE: DBTableSizeDiff,
    DIFF_TYPE_INDEX_SIZE: DBIndexSizeDiff,
    DIFF_TYPE_TABLE_TOTAL_SIZE: DBTableTotalSizeDiff,
    DIFF_TYPE_SEQUENCE: DBSequenceDiff,
}


def diff(src_dsn, target_dsn, diff_type, verbose=False):
    """Compare all tables row count between two dbs

    Args:
        src_dsn: str, dsn for postgres database, like "host=xxx dbname=test user=postgres password=secret port=5432"
        target_dsn: str, dsn for postgres database, like "host=xxx dbname=test user=postgres password=secret port=5432"
        diff_type: str, diff type
        verbose: boolean, to show verbose diff result or not

    Returns:
        diff result
    """
    diff_class = DiffClassMapper[diff_type]

    src_db = diff_class(src_dsn)
    target_db = diff_class(target_dsn)

    diff_result = src_db.diff(target_db, verbose)

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
            DIFF_TYPE_TABLE_SCHEMA,
            DIFF_TYPE_TABLE_SIZE,
            DIFF_TYPE_INDEX_SIZE,
            DIFF_TYPE_TABLE_TOTAL_SIZE,
            DIFF_TYPE_SEQUENCE,
        )),
        'SOURCE_DSN': And(str, len),
        'TARGET_DSN': And(str, len),
        '--version': And(bool),
        '--verbose': And(bool),
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
        'verbose': args['--verbose'],
    }

    diff(**kwargs)


if __name__ == '__main__':
    main()
