FROM python:3.12-alpine
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
RUN pip install sql_data_guard docker
WORKDIR /app/
COPY src/sql_data_guard/mcpwrapper/mcp_wrapper.py .
CMD ["python", "-u", "mcp_wrapper.py"]