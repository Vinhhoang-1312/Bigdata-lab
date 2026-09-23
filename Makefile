.PHONY: help demo demo-stop demo-status test foundation-check replay-check streaming-check

SPEEDUP ?= 60
# MAX_RATE is optional; when unset, replay Python automatically computes max(500.0, 4.0 * SPEEDUP)
MAX_RATE ?=
DEMO_MODE ?= integrated
STREAMLIT_PORT ?= 8501

help:
	@echo "UrbanFlow Control & Demo Lifecycle:"
	@echo "  make demo          - Start end-to-end continuous virtual time demo"
	@echo "  make demo-stop     - Stop demo streaming, replay, and dashboard"
	@echo "  make demo-status   - Check status of components and virtual time progression"
	@echo "  make test          - Run foundation, replay, and virtual time tests"
	@echo ""
	@echo "Configuration options (override via make VAR=val):"
	@echo "  SPEEDUP=$(SPEEDUP) (virtual acceleration factor, e.g. 60 or 300)"
	@echo "  MAX_RATE=$(MAX_RATE) (burst limit events/sec, defaults in Python to max(500.0, 4.0*SPEEDUP))"
	@echo "  DEMO_MODE=$(DEMO_MODE) (fixture or integrated)"

demo: demo-stop
	@echo "==> 1. Starting infrastructure services (SeaweedFS, Kafka, Spark)..."
	docker compose up -d seaweedfs kafka spark-master spark-worker-1
	@echo "==> 2. Initializing storage buckets and Kafka topics..."
	docker compose run --rm bucket-init
	docker compose run --rm kafka-topic-init
	@echo "==> 3. Launching Spark Structured Streaming (5-min sliding window / 1-min slide)..."
	docker compose run -d --name urbanflow-demo-streaming spark-submit \
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
	@echo "==> 5. Starting Streamlit Analytics Dashboard on port $(STREAMLIT_PORT)..."
	@URBANFLOW_S3_ENDPOINT=http://localhost:8333 \
	 AWS_ACCESS_KEY_ID=$$(grep AWS_ACCESS_KEY_ID .env | cut -d= -f2) \
	 AWS_SECRET_ACCESS_KEY=$$(grep AWS_SECRET_ACCESS_KEY .env | cut -d= -f2) \
	 URBANFLOW_HISTORICAL_PATH=s3a://urbanflow/gold/historical_zone_baseline/ \
	 URBANFLOW_LIVE_PATH=s3a://urbanflow/gold/live_zone_metrics/live_demo/ \
	 nohup ./urbanflow-streaming/.venv/bin/streamlit run urbanflow-streaming/analytics_app/app.py \
		--server.port $(STREAMLIT_PORT) \
		--server.headless true > /tmp/urbanflow-streamlit.log 2>&1 & echo $$! > .streamlit.pid
	@echo ""
	@echo "=================================================================="
	@echo "UrbanFlow Continuous Demo is RUNNING!"
	@echo "Dashboard:    http://localhost:$(STREAMLIT_PORT)"
	@echo "Speedup:      $(SPEEDUP)x (1 second wall clock = $(SPEEDUP) seconds virtual time)"
	@echo "Replay Mode:  $(DEMO_MODE) (Continuous Monotonic Looping)"
	@echo "Check status: make demo-status"
	@echo "Stop demo:    make demo-stop"
	@echo "=================================================================="

demo-stop:
	@echo "==> Stopping demo components..."
	@docker rm -f urbanflow-demo-replay urbanflow-demo-streaming 2>/dev/null || true
	@if [ -f .streamlit.pid ]; then \
		PID=$$(cat .streamlit.pid); \
		kill $$PID 2>/dev/null || true; \
		rm -f .streamlit.pid; \
		echo "Stopped Streamlit dashboard (PID $$PID)"; \
	fi
	@echo "Demo stopped."

demo-status:
	@echo "=================================================================="
	@echo "UrbanFlow Component Status"
	@echo "=================================================================="
	@docker ps --filter "name=urbanflow" --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}"
	@echo ""
	@echo "--- Streamlit Dashboard ---"
	@if [ -f .streamlit.pid ] && kill -0 $$(cat .streamlit.pid) 2>/dev/null; then \
		echo "Dashboard is RUNNING (PID $$(cat .streamlit.pid)) at http://localhost:$(STREAMLIT_PORT)"; \
	else \
		echo "Dashboard is NOT running."; \
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

test: foundation-check replay-check streaming-check
	python3 tests/verify_monotonic_virtual_time.py
	AWS_ACCESS_KEY_ID=$$(grep AWS_ACCESS_KEY_ID .env | cut -d= -f2) \
	AWS_SECRET_ACCESS_KEY=$$(grep AWS_SECRET_ACCESS_KEY .env | cut -d= -f2) \
	python3 tests/verify_feb_silver_monotonicity.py --max-rows 100000
	@echo ""
	@echo "ALL TESTS PASSED!"

