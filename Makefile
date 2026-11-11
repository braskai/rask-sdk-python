.DEFAULT: help
.PHONY: help bootstrap generate check-generated format lint test test-unit test-acceptance build clean

help:
	@echo "Use \`$(MAKE) <target>' where <target> is one of the following:"
	@echo "  bootstrap        - install the dependencies with uv"
	@echo "  generate         - rewrite rask_sdk/_generated from contract/openapi.json"
	@echo "  check-generated  - fail if rask_sdk/_generated differs from the contract"
	@echo "  format           - format the code"
	@echo "  lint             - ruff, mypy and check-generated"
	@echo "  test             - unit and acceptance tests (RUN_SLOW=1 adds the slow scenarios)"
	@echo "  test-unit        - unit tests, no Node needed"
	@echo "  test-acceptance  - the shared acceptance suite (RASK_SDK_ACCEPTANCE_DIR, Node 22+)"
	@echo "  build            - sdist and wheel in dist/"
	@echo "  clean            - remove build artifacts and the virtualenv"

bootstrap:
	uv sync

generate:
	uv run python tools/generate.py

check-generated:
	uv run python tools/generate.py --check

format:
	uv run ruff format .
	uv run ruff check --fix .

lint: check-generated
	uv run ruff check .
	uv run ruff format --check .
	uv run mypy

test:
	uv run pytest

test-unit:
	uv run pytest tests/unit

# Without the harness the tests skip. Here that is a failure: nothing else is asked of this target.
test-acceptance:
	RASK_SDK_ACCEPTANCE_REQUIRED=1 uv run pytest tests/acceptance

build:
	uv build

clean:
	rm -rf build dist .venv .mypy_cache .pytest_cache .ruff_cache
