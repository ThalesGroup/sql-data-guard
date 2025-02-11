FROM python:3.12-alpine
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
RUN pip install flask
WORKDIR /app/sql_data_guard
COPY src/sql_data_guard/. .
ENV PYTHONPATH "/app/sql_data_guard"
CMD ["python", "-u", "rest/sql_data_guard_rest.py"]
