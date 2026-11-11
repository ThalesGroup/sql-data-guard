FROM python:3.12-alpine
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
ARG SDG_VERSION=""
RUN pip install --no-cache-dir docker "sql_data_guard${SDG_VERSION:+==${SDG_VERSION}}"
WORKDIR /app/
COPY src/sql_data_guard/mcpwrapper/mcp_wrapper.py .
CMD ["python", "-u", "mcp_wrapper.py"]