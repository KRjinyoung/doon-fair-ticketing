FROM python:3.11-slim
 
WORKDIR /app
 
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
 
COPY pop_server.py .
COPY first_layer.py .
COPY pop_auth.py .
COPY bot_rules.py .
COPY frontend/ ./static/
 
EXPOSE 8000
 
# 워커 수를 환경변수로 조절한다 (기본 4). 재빌드 없이 바꿀 수 있다. [WORKERS_V1]
CMD ["sh", "-c", "mkdir -p ${PROMETHEUS_MULTIPROC_DIR:-/tmp/prom_multiproc} && rm -f ${PROMETHEUS_MULTIPROC_DIR:-/tmp/prom_multiproc}/* ; exec uvicorn pop_server:app --host 0.0.0.0 --port 8000 --workers ${UVICORN_WORKERS:-4}"]
