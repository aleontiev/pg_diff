#!/usr/bin/env python
# -*- coding: utf-8 -*-

from pg_diff.pg_diff import _validate


def test_validate_pass():
    expected = {
        '--type': 'table_count',
        'SOURCE_DSN': 'source_dsn',
        'TARGET_DSN': 'target_dsn',
        '--version': True,
        '--verbose': False,
        '--help': False,
    }
    result = _validate(expected)

    assert expected == result
