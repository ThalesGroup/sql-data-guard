"""
gunicorn settings for the Docker image (production server; `python sql_data_guard_rest.py` is the
Flask development server and is only meant for local use).

Uses the same environment variables as the service, plus:
  WEB_CONCURRENCY  number of worker processes (default 2)
  APP_THREADS      threads per worker (default 4)
"""

import os

bind = f"{os.environ.get('APP_HOST', '0.0.0.0')}:{os.environ.get('APP_PORT', '5000')}"
workers = int(os.environ.get("WEB_CONCURRENCY", "2"))
# threaded workers keep a slow or stalled client from blocking a whole worker
worker_class = "gthread"
threads = int(os.environ.get("APP_THREADS", "4"))
timeout = 30
accesslog = "-"
errorlog = "-"
loglevel = "info"
