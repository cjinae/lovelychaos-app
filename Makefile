.PHONY: lint test test-unit test-integration test-contract test-ui test-security ci db-upgrade db-downgrade db-revision

lint:
	python3 -m ruff check .

db-upgrade:
	python3 -m alembic upgrade head

db-downgrade:
	python3 -m alembic downgrade -1

db-revision:
	python3 -m alembic revision --autogenerate -m "$(m)"

test:
	python3 -m pytest

test-unit:
	python3 -m pytest tests/unit

test-integration:
	python3 -m pytest tests/integration

test-contract:
	python3 -m pytest tests/contracts

test-ui:
	python3 -m pytest tests/ui

test-security:
	python3 -m pytest tests/security

ci: lint test-unit test-integration test-contract test-ui test-security
