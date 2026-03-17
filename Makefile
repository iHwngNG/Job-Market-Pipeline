# ==============================================================================
# 🚀 Vietnam Job Market Intelligence Pipeline
# ==============================================================================

# Variables
DC = docker-compose

.PHONY: help up down restart logs-airflow logs-dashboard logs-consumer build-all clean test-crawl

help: ## Show detailed help menu
	@echo "Vietnam Job Market Pipeline - Commands:"
	@echo "--------------------------------------------------------------------------------"
	@echo "  make up               - Khởi động toàn bộ pipeline (chế độ ẩn/background)"
	@echo "  make down             - Tắt toàn bộ system"
	@echo "  make restart          - Tắt và khởi động lại toàn bộ pipeline"
	@echo "  make logs-airflow     - Xem log của Airflow Webserver"
	@echo "  make logs-dashboard   - Xem log của Streamlit Dashboard"
	@echo "  make logs-consumer    - Xem log của Kafka Consumer (lưu data vào Postgres)"
	@echo "  make test-crawl       - Test nhanh tính năng crawler dò số trang (Local)"
	@echo "  make build-all        - Re-build lại toàn bộ Docker images tự chế của project"
	@echo "  make clean            - Xóa toàn bộ Volumes (Cảnh báo: Sẽ mất mọi Database/Data)"
	@echo "--------------------------------------------------------------------------------"

up: ## Start all services in detached mode
	@echo "Khởi động Chrome chế độ CDP (Remote Debugging) trên Windows..."
	@powershell.exe -ExecutionPolicy Bypass -WindowStyle Hidden -File ".\utils\launch_chrome_cdp.ps1" || echo "Lỗi khi tự động mở Chrome. Vui lòng mở thủ công."
	$(DC) up -d

down: ## Stop all services
	$(DC) down

restart: down up ## Restart all services

build-all: ## Rebuild all custom Docker images
	$(DC) build --no-cache

clean: ## Remove containers and ALL volumes (WARNING: Data will be lost)
	$(DC) down -v

logs-airflow: ## Tail logs for Airflow webserver
	$(DC) logs -f airflow-webserver

logs-dashboard: ## Tail logs for Streamlit Dashboard
	$(DC) logs -f dashboard

logs-consumer: ## Tail logs for Kafka Consumer
	$(DC) logs -f consumer
