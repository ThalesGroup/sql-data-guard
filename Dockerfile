FROM python:3.12-alpine
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
RUN pip install flask
WORKDIR /app
COPY src/*.py .
COPY src/rest/* .
CMD [ "python", "-u", "./sql_data_guard_rest.py"]
