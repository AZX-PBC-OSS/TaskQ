FROM python:3.14-slim

RUN pip install --no-cache-dir uv

WORKDIR /app

COPY . /app

RUN uv sync --extra fastapi --extra redis
