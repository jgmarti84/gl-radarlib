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
