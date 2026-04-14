.PHONY: install test lint build publish

install:
	pip install -e ".[all]"

test:
	pytest tests/ -v

lint:
	ruff check src/
	black --check src/
	mypy src/

build:
	python -m build

publish:
	twine upload dist/*
