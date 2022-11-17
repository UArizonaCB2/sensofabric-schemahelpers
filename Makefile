.PHONY: install-deps format create_athena_table

install-deps:
	python3 -m pip install -r requirements.txt
	# python3 -m pip install pyarrow==10.0.0

format:
	python3 -m black src/python
	python3 -m isort --recursive src/python

create_athena_table:
	python3 src/python/create_athena_table.py $(path)