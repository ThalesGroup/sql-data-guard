FROM python:3.12-alpine
# The library comes from PyPI. The release workflow passes the version that was just published, so the
# image contains exactly that release; without the argument (local builds) the latest release is used.
ARG SDG_VERSION=""
ENV PYTHONUNBUFFERED=1 APP_HOST=0.0.0.0
RUN pip install --no-cache-dir "flask>=3,<4" "sql_data_guard${SDG_VERSION:+==${SDG_VERSION}}" \
    && adduser -D -u 10001 app
WORKDIR /app/
COPY src/sql_data_guard/rest/sql_data_guard_rest.py .
COPY src/sql_data_guard/rest/logging.conf .
USER app
EXPOSE 5000
CMD ["python", "-u", "sql_data_guard_rest.py"]
