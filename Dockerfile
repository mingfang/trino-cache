# syntax=docker/dockerfile:1.4

# Choose a python version that you know works with your application
FROM python:3.11-slim

# Install uv for fast package management
COPY --from=ghcr.io/astral-sh/uv:0.4.20 /uv /bin/uv
ENV UV_SYSTEM_PYTHON=1

WORKDIR /app

# Copy requirements file
COPY --link requirements.txt .

# Install the requirements using uv
RUN uv pip install -r requirements.txt

# Copy application files
COPY --link app.py .
# Uncomment the following line if you need to copy additional files
# COPY --link . .

EXPOSE 8080

CMD [ "marimo", "run", "app.py", "--host", "0.0.0.0", "-p", "8080" ]