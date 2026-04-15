.PHONY: app api ui stop clean test lint fmt

# --- Run everything ---

app: ## Start backend (port 8000) + frontend (port 3000)
	@echo "Starting Headwater..."
	@echo "  Backend:  http://localhost:8000"
	@echo "  Frontend: http://localhost:3000"
	@echo "  API docs: http://localhost:8000/docs"
	@echo ""
	@trap 'kill 0' INT TERM; \
	cd /Users/sethurama/DEV/LM/UTE/adm/headwater && \
	uv run uvicorn headwater.api.app:app --host 0.0.0.0 --port 8000 --reload --log-level info 2>&1 | sed 's/^/[api] /' & \
	cd /Users/sethurama/DEV/LM/UTE/adm/headwater/ui && npm run dev 2>&1 | sed 's/^/[ui]  /' & \
	wait

# --- Individual services ---

api: ## Start backend only (port 8000)
	uv run uvicorn headwater.api.app:app --host 0.0.0.0 --port 8000 --reload --log-level info

ui: ## Start frontend only (port 3000)
	cd ui && npm run dev

# --- Quality ---

test: ## Run all Python tests
	uv run pytest -v

lint: ## Run ruff lint check
	uv run ruff check .

fmt: ## Auto-format with ruff
	uv run ruff format .

# --- Cleanup ---

stop: ## Kill any running headwater processes
	@-pkill -f "uvicorn headwater" 2>/dev/null || true
	@-pkill -f "next dev" 2>/dev/null || true
	@echo "Stopped."

clean: stop ## Stop + remove caches
	@rm -rf .pytest_cache __pycache__ headwater/__pycache__
	@rm -rf ui/.next
	@echo "Cleaned."

help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*##' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*## "}; {printf "  make %-10s %s\n", $$1, $$2}'
