#!/usr/bin/env python
# -*- coding: utf-8 -*-

from setuptools import setup, find_packages

VERSION = '0.2.1'

with open('README.rst') as f:
    long_description = f.read()

with open('LICENSE') as f:
    long_license = f.read()

setup(
    name='pg_diff',
    version=VERSION,
    description="a simple tool to diff schemas in two postgresql database",
    long_description=long_description,
    classifiers=[
        'Topic :: Utilities',
        'Development Status :: 5 - Production/Stable',
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python :: 2',
        'Programming Language :: Python :: 2.6',
        'Programming Language :: Python :: 2.7',
        'Intended Audience :: Developers',
        'Operating System :: OS Independent',
    ],
    keywords='pg_diff postgresql diff',
    author='hanks',
    author_email='zhouhan315@gmail.com',
    url='https://github.com/hanks/pg_diff',
    license=long_license,
    packages=find_packages(),
    include_package_data=True,
    zip_safe=True,
    install_requires=[
        'docopt>=0.6.2',
        'schema>=0.6.5',
        'deepdiff>=2.5.1',
        'psycopg2>=2.6.2',
    ],
    entry_points={
        'console_scripts': [
            'pg_diff = pg_diff:main'
        ]
    },
)
