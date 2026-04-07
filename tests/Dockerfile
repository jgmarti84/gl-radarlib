FROM python:3.12-slim

RUN apt-get update && apt-get install -y \
    git \
    build-essential \
    gdal-bin \
    libgdal-dev \
    && rm -rf /var/lib/apt/lists/*

# Set the working directory in the container
WORKDIR /app

COPY requirements.txt /app/requirements.txt
RUN python -m pip install --upgrade pip setuptools wheel
RUN pip install --no-cache-dir -r requirements.txt

COPY requirements-dev.txt /app/requirements-dev.txt
RUN pip install --no-cache-dir -r requirements-dev.txt

COPY src/ /app/src/
COPY tests/ /app/tests/
COPY app/ /app/app/

ENV PYTHONPATH=/app/src/
