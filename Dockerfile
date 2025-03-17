# Use Python 3.9 as the base image
FROM python:3.9-slim

# Set working directory
WORKDIR /app

# Set environment variables
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    POETRY_VERSION=1.3.2

# Install system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Install Poetry for dependency management
RUN pip install "poetry==$POETRY_VERSION"

# Copy poetry configuration files
COPY pyproject.toml poetry.lock* ./

# Configure poetry to not use a virtual environment in the container
RUN poetry config virtualenvs.create false

# Install dependencies
RUN poetry install --no-dev --no-interaction --no-ansi

# Copy application code
COPY . .

# Create necessary directories for logs and message history
RUN mkdir -p /var/log/agent_messaging /app/message_history

# Expose the port the app runs on
EXPOSE 8000

# Create a non-root user to run the application
RUN adduser --disabled-password --gecos "" appuser
RUN chown -R appuser:appuser /app /var/log/agent_messaging
USER appuser

# Command to run the application
CMD ["gunicorn", "-c", "gunicorn_config.py", "app:app"]