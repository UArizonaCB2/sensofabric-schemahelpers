.PHONY: install-deps format create_athena_table
export PYTHONPATH := $(shell pwd)/src/python:$(PYTHONPATH)

install-deps:
	python3 -m pip install -r requirements.txt

format:
	python3 -m black src/python
	python3 -m autoflake --recursive --in-place --remove-all-unused-imports src/python 
	python3 -m isort src/python

create_athena_table:
	python3 src/python/athena/create_athena_table.py $(path)