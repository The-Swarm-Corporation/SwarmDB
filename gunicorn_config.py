"""
Gunicorn configuration file for production deployment of the Agent Messaging System API.

This configuration is optimized for production environments with appropriate
worker settings, logging, and timeouts.

Usage:
    gunicorn -c gunicorn_config.py app:app
"""

import multiprocessing
import os

# Server socket configuration
bind = os.getenv("GUNICORN_BIND", "0.0.0.0:8000")
backlog = 2048

# Worker processes
# Using Uvicorn workers for ASGI application (FastAPI)
worker_class = "uvicorn.workers.UvicornWorker"

# Number of worker processes
# A common formula is (2 * CPU cores) + 1
# We'll use this formula but allow override via environment variable
workers = int(
    os.getenv(
        "GUNICORN_WORKERS", (multiprocessing.cpu_count() * 2) + 1
    )
)

# Number of threads per worker
# For CPU-bound applications, it's often best to stick with 1
# For I/O-bound applications (like this one), we can use more
threads = int(os.getenv("GUNICORN_THREADS", 4))

# Worker process max requests
# Restart workers after handling this many requests to help with memory leaks
max_requests = int(os.getenv("GUNICORN_MAX_REQUESTS", 10000))
max_requests_jitter = int(
    os.getenv("GUNICORN_MAX_REQUESTS_JITTER", 1000)
)

# Worker timeouts
# How long to wait for worker to process a request before killing and restarting
timeout = int(os.getenv("GUNICORN_TIMEOUT", 120))
graceful_timeout = int(os.getenv("GUNICORN_GRACEFUL_TIMEOUT", 60))
keep_alive = int(os.getenv("GUNICORN_KEEP_ALIVE", 5))

# Process naming
proc_name = "agent_messaging_api"

# Logging
accesslog = os.getenv(
    "GUNICORN_ACCESS_LOG", "/var/log/agent_messaging/access.log"
)
errorlog = os.getenv(
    "GUNICORN_ERROR_LOG", "/var/log/agent_messaging/error.log"
)
loglevel = os.getenv("GUNICORN_LOG_LEVEL", "info")
access_log_format = os.getenv(
    "GUNICORN_ACCESS_LOG_FORMAT",
    '%(h)s %(l)s %(u)s %(t)s "%(r)s" %(s)s %(b)s "%(f)s" "%(a)s" %(L)s',
)

# SSL/TLS settings (if terminating SSL at Gunicorn)
# Recommended to use a reverse proxy (Nginx, etc.) for SSL in production
if os.getenv("GUNICORN_USE_SSL", "false").lower() == "true":
    certfile = os.getenv("GUNICORN_CERTFILE")
    keyfile = os.getenv("GUNICORN_KEYFILE")

# Security settings
limit_request_line = int(
    os.getenv("GUNICORN_LIMIT_REQUEST_LINE", 4094)
)
limit_request_fields = int(
    os.getenv("GUNICORN_LIMIT_REQUEST_FIELDS", 100)
)
limit_request_field_size = int(
    os.getenv("GUNICORN_LIMIT_REQUEST_FIELD_SIZE", 8190)
)

# Server mechanics
preload_app = (
    os.getenv("GUNICORN_PRELOAD_APP", "true").lower() == "true"
)
daemon = os.getenv("GUNICORN_DAEMON", "false").lower() == "true"
pidfile = os.getenv(
    "GUNICORN_PIDFILE", "/var/run/agent_messaging_api.pid"
)
umask = int(os.getenv("GUNICORN_UMASK", 0))
user = os.getenv("GUNICORN_USER", None)
group = os.getenv("GUNICORN_GROUP", None)


# Server hooks
def on_starting(server):
    """
    Hook called when the application starts.
    This is a good place to initialize shared resources.
    """
    pass


def on_exit(server):
    """
    Hook called when the application exits.
    This is a good place to clean up resources.
    """
    pass


def worker_int(worker):
    """
    Hook called when a worker process receives SIGINT or SIGQUIT.
    This is a good place to save worker state.
    """
    worker.log.info("worker received INT or QUIT signal")


def worker_abort(worker):
    """
    Hook called when a worker process aborts.
    This is a good place to log worker failure.
    """
    worker.log.info("worker received SIGABRT signal")
