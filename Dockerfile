FROM python:3.12-alpine
COPY requirements.txt .
RUN pip install flask sql_data_guard
WORKDIR /app/
COPY src/sql_data_guard/rest/sql_data_guard_rest.py .
COPY src/sql_data_guard/rest/logging.conf .
CMD ["python", "-u", "sql_data_guard_rest.py"]