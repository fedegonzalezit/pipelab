.PHONY: clean clean-build clean-pyc clean-test coverage dist help install lint lint/flake8 lint/black
.DEFAULT_GOAL := help

# Print help for each target
help:
	@python -c "import re, sys; [print(f'{m.group(1):<20} {m.group(2)}') for line in sys.stdin for m in [re.match(r'^([a-zA-Z_\-]+):.*?## (.*)$$', line)] if m]" < $(MAKEFILE_LIST)

clean: clean-build clean-pyc clean-test ## remove all build, test, coverage and Python artifacts

clean-build: ## remove build artifacts
	rm -fr build/
	rm -fr dist/
	rm -fr .eggs/
	find . -name '*.egg-info' -exec rm -fr {} +
	find . -name '*.egg' -exec rm -f {} +

clean-pyc: ## remove Python file artifacts
	find . -name '*.pyc' -exec rm -f {} +
	find . -name '*.pyo' -exec rm -f {} +
	find . -name '*~' -exec rm -f {} +
	find . -name '__pycache__' -exec rm -fr {} +

clean-test: ## remove test and coverage artifacts
	rm -fr .tox/
	rm -f .coverage
	rm -fr htmlcov/
	rm -fr .pytest_cache

lint/flake8: ## check style with flake8
	flake8 --ignore=E501 pipelab tests

lint/black: ## check style with black
	black --check pipelab tests

lint: lint/flake8 lint/black ## check style

format: ## format code with black
	black pipelab tests

test: ## run tests quickly with the default Python
	python3 -m unittest discover -s tests

coverage: ## check code coverage quickly with the default Python
	coverage run --source pipelab -m unittest discover -s tests
	coverage report -m
	coverage html

install: clean ## install the package to the active Python's site-packages
	python3 setup.py install

dist: clean ## builds source and wheel package
	python3 setup.py sdist
	python3 setup.py bdist_wheel
	ls -l dist
