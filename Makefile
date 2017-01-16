.PHONY: cut-release build upload test clean

test:
	tox

cut-release:
	standard-version

build:
	rm -f dist/*
	python setup.py sdist bdist_wheel

upload:
	twine upload dist/*

clean:
	rm -f dist/*
