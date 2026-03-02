FROM python:3.11-slim

# Set environment variables
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

# Install system dependencies
# Required: GDAL native library and development headers for Python GDAL package
# Also install git and build-essential for compiling Python packages with C extensions
RUN apt-get update && apt-get install -y --no-install-recommends \
    gdal-bin \
    libgdal-dev \
    build-essential \
    git \
    && rm -rf /var/lib/apt/lists/*

# Set GDAL environment variables for pip to find gdal-config
ENV GDAL_CONFIG=/usr/bin/gdal-config

# Set working directory to the workspace root where pyproject.toml is located
WORKDIR /workspace

# Copy the entire project including pyproject.toml and src/
COPY . .

# Install pip dependencies first (for better layer caching)
# This installs the radarlib package in editable mode(-e .) along with all its dependencies
# defined in pyproject.toml
RUN pip install --no-cache-dir -e .

# Install any additional runtime requirements from requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Set working directory to app folder for main.py
WORKDIR /workspace/app

# Set the entrypoint to run main.py
CMD ["python", "main.py"]
