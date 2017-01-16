.PHONY: cut-release build upload test

test:
    tox

cut-release:
    standard-version

build:
    rm dist/*
    python setup.py sdist bdist_wheel

upload:
    twine upload dist/*
