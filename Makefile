.PHONY: help demo demo-stop demo-status test foundation-check replay-check streaming-check observability-check check-prereqs

SPEEDUP ?= 60
# MAX_RATE is optional; when unset, replay Python automatically computes max(500.0, 4.0 * SPEEDUP)
MAX_RATE ?=
DEMO_MODE ?= integrated
STREAMLIT_PORT ?= 8501
FASTAPI_PORT ?= 8000

help:
	@echo "UrbanFlow Control & Demo Lifecycle:"
	@echo "  make demo          - Start end-to-end continuous virtual time demo with Glass Cockpit"
	@echo "  make demo-stop     - Stop demo streaming, replay, dashboard, and API"
	@echo "  make demo-status   - Check status of components, virtual time, and telemetry"
	@echo "  make test          - Run foundation, replay, streaming, and observability tests"
	@echo ""
	@echo "Configuration options (override via make VAR=val):"
	@echo "  SPEEDUP=$(SPEEDUP) (virtual acceleration factor, e.g. 60 or 300)"
	@echo "  MAX_RATE=$(MAX_RATE) (burst limit events/sec, defaults in Python to max(500.0, 4.0*SPEEDUP))"
	@echo "  DEMO_MODE=$(DEMO_MODE) (fixture or integrated)"
	@echo "  STREAMLIT_PORT=$(STREAMLIT_PORT)"
	@echo "  FASTAPI_PORT=$(FASTAPI_PORT)"

check-prereqs:
	@if [ ! -f .env ]; then \
		echo "ERROR: .env file missing. Run 'cp .env.example .env' first."; \
		exit 1; \
	fi
	@if ! command -v docker >/dev/null 2>&1 || ! docker compose version >/dev/null 2>&1; then \
		echo "ERROR: Docker Compose is not installed or the Docker daemon is not running."; \
		exit 1; \
	fi
	@if [ ! -f ./urbanflow-streaming/.venv/bin/python ]; then \
		echo "ERROR: Python virtual environment not found at ./urbanflow-streaming/.venv."; \
		echo "Please install dependencies:"; \
		echo "  cd urbanflow-streaming && uv venv && uv pip install -e '.[analytics,serving,offline]'"; \
		exit 1; \
	fi
	@mkdir -p .telemetry && chmod 777 .telemetry 2>/dev/null || true

demo: check-prereqs demo-stop
	@echo "==> 1. Starting infrastructure services (SeaweedFS, Kafka, Spark)..."
	docker compose up -d seaweedfs kafka spark-master spark-worker-1
	@echo "==> 2. Initializing storage buckets and Kafka topics..."
	docker compose run --rm bucket-init
	docker compose run --rm kafka-topic-init
	@echo "==> 2b. Resetting clean live demo storage and checkpoints..."
	@curl -s -X DELETE "http://localhost:8888/buckets/urbanflow/gold/live_zone_metrics/live_demo?recursive=true" >/dev/null 2>&1 || true
	@curl -s -X DELETE "http://localhost:8888/buckets/urbanflow/checkpoints/live_trip_metrics/live_demo?recursive=true" >/dev/null 2>&1 || true
	@echo "==> 3. Launching Spark Structured Streaming (5-min sliding window / 1-min slide)..."
	docker compose run -d -p 4040:4040 --name urbanflow-demo-streaming spark-submit \
		--master 'local[2]' \
		/workspace/urbanflow-streaming/spark/live_trip_metrics.py \
		--config /workspace/urbanflow-streaming/config/settings.integrated.yaml
	@echo "==> 4. Launching Continuous Virtual Time Replay Producer ($(SPEEDUP)x acceleration, loop=on)..."
	docker compose run -d --name urbanflow-demo-replay replay-python \
		python /workspace/urbanflow-replay/producer/replay_trips.py \
		--mode $(DEMO_MODE) \
		--speedup $(SPEEDUP) \
		$(if $(MAX_RATE),--max-rate $(MAX_RATE),) \
		--loop
	@echo "==> 5. Starting FastAPI Observability Service on port $(FASTAPI_PORT)..."
	@setsid env \
		URBANFLOW_S3_ENDPOINT=http://localhost:8333 \
		AWS_ACCESS_KEY_ID=$$(grep AWS_ACCESS_KEY_ID .env | cut -d= -f2) \
		AWS_SECRET_ACCESS_KEY=$$(grep AWS_SECRET_ACCESS_KEY .env | cut -d= -f2) \
		URBANFLOW_HISTORICAL_PATH=s3a://urbanflow/gold/historical_zone_baseline/ \
		URBANFLOW_LIVE_PATH=s3a://urbanflow/gold/live_zone_metrics/live_demo/ \
		URBANFLOW_ZONE_LOOKUP_PATH=s3a://urbanflow/bronze/zone_lookup/taxi_zone_lookup.csv \
		PYTHONPATH=urbanflow-streaming \
		./urbanflow-streaming/.venv/bin/python -m uvicorn serving.app:app \
		--host 0.0.0.0 --port $(FASTAPI_PORT) > /tmp/urbanflow-fastapi.log 2>&1 < /dev/null &
	@for i in $$(seq 1 10); do \
		if curl -s -f http://localhost:$(FASTAPI_PORT)/v1/streaming/health >/dev/null 2>&1; then \
			lsof -i :$(FASTAPI_PORT) -t > .fastapi.pid 2>/dev/null || true; \
			echo "   FastAPI started successfully on port $(FASTAPI_PORT)."; \
			break; \
		fi; \
		sleep 1; \
	done
	@echo "==> 6. Starting Streamlit Analytics Dashboard on port $(STREAMLIT_PORT)..."
	@setsid env \
		URBANFLOW_S3_ENDPOINT=http://localhost:8333 \
		AWS_ACCESS_KEY_ID=$$(grep AWS_ACCESS_KEY_ID .env | cut -d= -f2) \
		AWS_SECRET_ACCESS_KEY=$$(grep AWS_SECRET_ACCESS_KEY .env | cut -d= -f2) \
		URBANFLOW_HISTORICAL_PATH=s3a://urbanflow/gold/historical_zone_baseline/ \
		URBANFLOW_LIVE_PATH=s3a://urbanflow/gold/live_zone_metrics/live_demo/ \
		PYTHONPATH=urbanflow-streaming \
		./urbanflow-streaming/.venv/bin/streamlit run urbanflow-streaming/analytics_app/app.py \
		--server.port $(STREAMLIT_PORT) \
		--server.headless true > /tmp/urbanflow-streamlit.log 2>&1 < /dev/null &
	@for i in $$(seq 1 10); do \
		if curl -s -f http://localhost:$(STREAMLIT_PORT)/_stcore/health >/dev/null 2>&1; then \
			lsof -i :$(STREAMLIT_PORT) -t > .streamlit.pid 2>/dev/null || true; \
			echo "   Streamlit dashboard started successfully on port $(STREAMLIT_PORT)."; \
			break; \
		fi; \
		sleep 1; \
	done
	@echo ""
	@echo "=================================================================="
	@echo "UrbanFlow Continuous Demo & Glass Cockpit are RUNNING!"
	@echo "Dashboard:    http://localhost:$(STREAMLIT_PORT)"
	@echo "FastAPI API:  http://localhost:$(FASTAPI_PORT)/v1/streaming/health"
	@echo "FastAPI Docs: http://localhost:$(FASTAPI_PORT)/docs"
	@echo "Spark UI:     http://localhost:4040"
	@echo "Speedup:      $(SPEEDUP)x (1 second wall clock = $(SPEEDUP) seconds virtual time)"
	@echo "Replay Mode:  $(DEMO_MODE) (Continuous Monotonic Looping)"
	@echo "Check status: make demo-status"
	@echo "Stop demo:    make demo-stop"
	@echo "=================================================================="

demo-stop:
	@echo "==> Stopping demo components..."
	@docker rm -f urbanflow-demo-replay urbanflow-demo-streaming 2>/dev/null || true
	@if [ -f .fastapi.pid ]; then \
		PID=$$(head -n1 .fastapi.pid); \
		kill $$PID 2>/dev/null || true; \
		rm -f .fastapi.pid; \
	fi
	@fuser -k $(FASTAPI_PORT)/tcp 2>/dev/null || true
	@if [ -f .streamlit.pid ]; then \
		PID=$$(head -n1 .streamlit.pid); \
		kill $$PID 2>/dev/null || true; \
		rm -f .streamlit.pid; \
	fi
	@fuser -k $(STREAMLIT_PORT)/tcp 2>/dev/null || true
	@rm -f .telemetry/streaming_health.json 2>/dev/null || true
	@echo "Demo stopped."

demo-status:
	@echo "=================================================================="
	@echo "UrbanFlow Component & Glass Cockpit Status"
	@echo "=================================================================="
	@docker ps --filter "name=urbanflow" --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}"
	@echo ""
	@echo "--- FastAPI Observability Service ---"
	@if curl -s -f http://localhost:$(FASTAPI_PORT)/v1/streaming/health >/dev/null 2>&1; then \
		echo "FastAPI is RUNNING at http://localhost:$(FASTAPI_PORT)"; \
		curl -sS http://localhost:$(FASTAPI_PORT)/v1/streaming/health | python3 -m json.tool 2>/dev/null || true; \
	elif [ -f .fastapi.pid ] && kill -0 $$(head -n1 .fastapi.pid) 2>/dev/null; then \
		echo "FastAPI is STARTING (PID $$(head -n1 .fastapi.pid)) at http://localhost:$(FASTAPI_PORT)..."; \
	else \
		echo "FastAPI is NOT running."; \
	fi
	@echo ""
	@echo "--- Streamlit Dashboard ---"
	@if curl -s -f http://localhost:$(STREAMLIT_PORT)/_stcore/health >/dev/null 2>&1; then \
		echo "Streamlit Dashboard is RUNNING at http://localhost:$(STREAMLIT_PORT)"; \
	elif [ -f .streamlit.pid ] && kill -0 $$(head -n1 .streamlit.pid) 2>/dev/null; then \
		echo "Streamlit Dashboard is STARTING (PID $$(head -n1 .streamlit.pid)) at http://localhost:$(STREAMLIT_PORT)..."; \
	else \
		echo "Streamlit Dashboard is NOT running."; \
	fi
	@echo ""
	@echo "--- Spark Telemetry Heartbeat (.telemetry/streaming_health.json) ---"
	@if [ -f .telemetry/streaming_health.json ]; then \
		python3 -m json.tool .telemetry/streaming_health.json 2>/dev/null || cat .telemetry/streaming_health.json; \
	else \
		echo "No telemetry heartbeat found yet."; \
	fi
	@echo ""
	@echo "--- Replay Producer Virtual Clock Logs ---"
	@docker logs --tail 8 urbanflow-demo-replay 2>/dev/null || echo "Replay container not running."
	@echo ""
	@echo "--- Kafka Trip Events Topic ---"
	@docker compose run --rm --no-deps kafka-cli --describe --topic urbanflow.trip-events 2>/dev/null || true
	@echo "=================================================================="

foundation-check:
	docker compose run --rm foundation-check

replay-check:
	docker compose run --rm replay-python python /workspace/urbanflow-replay/tests/run_contract_tests.py

streaming-check:
	@URBANFLOW_S3_ENDPOINT=http://localhost:8333 \
	 AWS_ACCESS_KEY_ID=$$(grep AWS_ACCESS_KEY_ID .env | cut -d= -f2) \
	 AWS_SECRET_ACCESS_KEY=$$(grep AWS_SECRET_ACCESS_KEY .env | cut -d= -f2) \
	 ./urbanflow-streaming/.venv/bin/python urbanflow-streaming/tests/verify_analytics_smoke.py \
		--live-path s3a://urbanflow/gold/live_zone_metrics/live_demo/

observability-check:
	@PYTHONPATH=urbanflow-streaming ./urbanflow-streaming/.venv/bin/pytest \
		urbanflow-streaming/tests/test_observability.py \
		urbanflow-streaming/tests/test_serving.py

test: foundation-check replay-check streaming-check observability-check
	python3 tests/verify_monotonic_virtual_time.py
	AWS_ACCESS_KEY_ID=$$(grep AWS_ACCESS_KEY_ID .env | cut -d= -f2) \
	AWS_SECRET_ACCESS_KEY=$$(grep AWS_SECRET_ACCESS_KEY .env | cut -d= -f2) \
	python3 tests/verify_feb_silver_monotonicity.py --max-rows 100000
	@echo ""
	@echo "ALL TESTS PASSED!"

