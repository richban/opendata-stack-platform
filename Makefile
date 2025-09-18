# OpenData Stack Platform Makefile
# System detection
UNAME := $(shell uname)

# Project paths
DAGSTER_WORKSPACE := dagster-workspace
SQLMESH_PROJECT := opendata_stack_platform_sqlmesh

clean:
	@echo "Cleaning Python cache files..."
	find . \( -type d -name "__pycache__" -o -type f -name "*.pyc" -o -type d -name ".pytest_cache" -o -type d -name "*.egg-info" \) -print0 | xargs -0 rm -rf

clean-dagster:
	@echo "Cleaning Dagster storage and logs..."
	rm -rf $(DAGSTER_HOME)/storage $(DAGSTER_HOME)/logs $(DAGSTER_HOME)/history

lint:
	@echo "Running linting checks..."
	ruff check .

lint-fix:
	@echo "Fixing linting issues..."
	ruff check --fix .

lint-format:
	@echo "Formatting code..."
	ruff format .

format: lint-format

# Dagster commands
dagster-dev: clean-dagster
	@echo "Starting Dagster development server..."
	cd $(DAGSTER_WORKSPACE) && dg dev

# SQLMesh commands
sqlmesh-plan:
	@echo "Running SQLMesh plan..."
	cd $(SQLMESH_PROJECT) && $(SQLMESH_CMD) plan

# Infrastructure commands
docker-up:
	@echo "Starting Docker services..."
	docker-compose up -d

docker-down:
	@echo "Stopping Docker services..."
	docker-compose down

