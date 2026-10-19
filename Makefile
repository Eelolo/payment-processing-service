.PHONY: up down logs test lint e2e

up:  ## Build and start the whole stack
	docker compose up -d --build

down:  ## Stop the stack and drop its volumes
	docker compose down -v

logs:
	docker compose logs -f api outbox-relay consumer

test:  ## Run the test suite in an isolated stack (real PostgreSQL and RabbitMQ)
	docker compose -f docker-compose.test.yml run --rm --build tests; \
	status=$$?; docker compose -f docker-compose.test.yml down -v; exit $$status

lint:  ## ruff + mypy inside the test image
	docker compose -f docker-compose.test.yml run --rm --build --no-deps tests \
		sh -c "ruff check . && ruff format --check . && mypy ."

e2e:  ## Scenario check against the running stack (make up first)
	python3 scripts/e2e.py
