.PHONY: venv setup-precommit install test lint clean pre-commit-install

venv:
	python3 -m venv venv
	@echo "Virtual environment created. Activate it with: source venv/bin/activate"

setup-precommit: venv pre-commit-install
	@echo "Pre-commit setup complete! You can now use pre-commit hooks locally."
	@echo "To activate: source venv/bin/activate"

pre-commit-install:
	pip install -r requirements-precommit.txt
	pre-commit install
	@echo "Pre-commit hooks installed successfully."

install:
	pip install -r requirements.txt
	pip install -r requirements-dev.txt

setup: install pre-commit-install
	@echo "Setup complete! You can now use pre-commit hooks."

test:
	pytest tests/

lint:
	flake8 src/

clean:
	find . -type d -name "__pycache__" -exec rm -r {} +
	find . -type f -name "*.pyc" -exec rm -f {} +

# ===========================================================================
# Docker: un contenedor por radar (genpro25-<radar> en docker-compose.yml)
#
# "make rma1" construye y levanta solo el servicio genpro25-rma1.
# Radares disponibles: rma00 rma1 a rma18, rma20, ar5, ar7, ar8.
# Las recetas usan solo comandos simples para que anden en cmd, PowerShell y sh.
# ===========================================================================
COMPOSE ?= docker compose

.PHONY: help ps status stats config down stop up-all logs

help:
	@echo Docker de radarlib, un contenedor por radar
	@echo ----------------------------------------
	@echo   rmaN - Construye y levanta un radar, por ejemplo make rma1 o make rma20
	@echo   arN - Igual para los AR, por ejemplo make ar5
	@echo   rebuild-RADAR - Reconstruye SIN cache y con imagen base nueva, por ejemplo make rebuild-rma1
	@echo   restart-RADAR - Reinicia sin reconstruir, por ejemplo make restart-rma1
	@echo   stop-RADAR - Detiene un radar, por ejemplo make stop-rma1
	@echo   logs-RADAR - Logs en vivo de un radar, por ejemplo make logs-rma1
	@echo   shell-RADAR - Abre una shell dentro del contenedor, por ejemplo make shell-rma1
	@echo ----------------------------------------
	@echo   ps - Lista los contenedores
	@echo   status - Nombre y estado de salud de los radares
	@echo   stats - Uso de CPU y memoria
	@echo   logs - Logs de todos los radares que esten corriendo
	@echo   config - Valida el docker-compose.yml
	@echo   stop - Detiene todos los radares
	@echo   down - Baja y elimina todos los contenedores
	@echo   up-all - Levanta los 23 radares, pesado porque cada uno puede usar 3 GB

# Un radar: make rma1, make rma20, make rma00
rma%:
	$(COMPOSE) up -d --build genpro25-rma$*

# Un radar AR: make ar5, make ar7, make ar8
ar%:
	$(COMPOSE) up -d --build genpro25-ar$*

# Reconstruccion limpia: sin cache y bajando la imagen base de nuevo. Es lenta.
rebuild-%:
	$(COMPOSE) build --no-cache --pull genpro25-$*
	$(COMPOSE) up -d genpro25-$*

restart-%:
	$(COMPOSE) restart genpro25-$*

stop-%:
	$(COMPOSE) stop genpro25-$*

logs-%:
	$(COMPOSE) logs -f --tail=100 genpro25-$*

shell-%:
	$(COMPOSE) exec genpro25-$* bash

ps:
	$(COMPOSE) ps -a

status:
	docker ps --filter name=genpro25 --format "table {{.Names}}\t{{.Status}}"

stats:
	docker stats --no-stream

logs:
	$(COMPOSE) logs -f --tail=100

config:
	$(COMPOSE) config -q

stop:
	$(COMPOSE) stop

down:
	$(COMPOSE) down

up-all:
	$(COMPOSE) up -d --build
