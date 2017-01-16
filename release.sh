#!/bin/bash

# create changelog and tag
standard-version

# build wheel for new version
rm dist/*
python setup.py sdist bdist_wheel

# upload to pypy
twine upload dist/*
