# OpenData Stack Platform Makefile
# System detection
UNAME := $(shell uname)

# Project paths
DAGSTER_WORKSPACE := dagster-workspace
SQLMESH_PROJECT := opendata_stack_platform_sqlmesh

clean:
	@echo "Cleaning Python cache files..."
	find . \( -type d -name "__pycache__" -o -type f -name "*.pyc" -o -type d -name ".pytest_cache" -o -type d -name "*.egg-info" \) -print0 | xargs -0 rm -rf

dg-clean:
	@echo "Cleaning Dagster storage and logs..."
	rm -rf $(DAGSTER_HOME)/storage $(DAGSTER_HOME)/logs $(DAGSTER_HOME)/history

clean-kafka:
	@echo "Cleaning Kafka topics..."
	./scripts/clean-kafka.sh

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
dg-dev:
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

listen-events-topic:
	nix-shell -p unixtools.watch.out --run "watch -n 5 'docker exec opendata-stack-platform-kafka-1 kafka-run-class kafka.tools.GetOffsetShell --broker-list kafka:9092 --topic listen_events --time -1'"

kafka-topic-sizes:
	@echo "=== Kafka Topic Size Report ==="
	@echo ""
	@topics=$$(docker exec opendata-stack-platform-kafka-1 kafka-topics --bootstrap-server kafka:9092 --list 2>/dev/null); \
	if [ -z "$$topics" ]; then \
		echo "No topics found or Kafka not running"; \
	else \
		echo "Topics, Message Counts, and Disk Usage:"; \
		echo ""; \
		echo "$$topics" | while read topic; do \
			echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"; \
			echo "Topic: $$topic"; \
			echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"; \
			offsets=$$(docker exec opendata-stack-platform-kafka-1 kafka-run-class kafka.tools.GetOffsetShell --broker-list kafka:9092 --topic $$topic --time -1 2>/dev/null); \
			if [ -n "$$offsets" ]; then \
				total=$$(echo "$$offsets" | awk -F: 'BEGIN{sum=0} {if($$3+0==$$3) sum+=$$3} END {printf "%d", sum}'); \
				echo "  Total Messages: $$total"; \
				echo "$$offsets" | awk -F: '{printf "  Partition %d: %d messages\n", $$2, $$3}'; \
				echo "  ---"; \
				disk_usage=$$(docker exec opendata-stack-platform-kafka-1 bash -c "cd /var/lib/kafka/data && for dir in $${topic}-*; do if [ -d \"\$$dir\" ]; then du -sh \"\$$dir\" 2>/dev/null | cut -f1; fi; done"); \
				if [ -n "$$disk_usage" ]; then \
					echo "  Disk Size: $$disk_usage"; \
				else \
					echo "  Disk Size: Unable to determine"; \
				fi; \
			else \
				echo "  No messages"; \
			fi; \
			echo ""; \
		done; \
	fi
	@echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
	@echo "=== End of Report ==="
