FROM python:3.11-slim
 
WORKDIR /app
 
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
 
COPY central_server.py .
COPY redis_window.py .
COPY frontend/ ./static/
 
EXPOSE 8080
 
CMD ["sh", "-c", "mkdir -p ${PROMETHEUS_MULTIPROC_DIR:-/tmp/prom_multiproc} && rm -f ${PROMETHEUS_MULTIPROC_DIR:-/tmp/prom_multiproc}/* ; exec uvicorn central_server:app --host 0.0.0.0 --port 8080 --workers ${UVICORN_WORKERS:-1}"]
