MAKEFLAGS += --warn-undefined-variables
SHELL := bash
.SHELLFLAGS := -eu -o pipefail -c
.DEFAULT_GOAL := help
.DELETE_ON_ERROR:
.SUFFIXES:
.SECONDARY:
RUN = poetry run

help: status
	@echo ""
	@echo "This project requires that dependencies are loaded into a poetry environment with 'poetry install'"
	@echo "make help -- show this help"
	@echo ""


# install any dependencies required for building
install:
	poetry install
.PHONY: install

deploy-gh-doc:
	$(RUN) mkdocs gh-deploy

test:
	$(RUN) pytest tests

lint:
	$(RUN) tox -e lint-fix

MKDOCS = $(RUN) mkdocs

mkd-%:
	$(MKDOCS) $*

# Test documentation locally
serve: mkd-serve

testdoc: serve

