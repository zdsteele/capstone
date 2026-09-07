# EDGAR Intelligence Platform — Flask app + in-process agent.
# Talks to Databricks (SQL warehouse, model serving, vector search) and Lakebase
# (Postgres) over the network, so it runs anywhere — this image targets Render.
FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PORT=8000

WORKDIR /app

# deps first for layer caching. --use-deprecated=legacy-resolver: the pinned
# langchain 0.3 line stalls pip's new resolver (see requirements.txt).
COPY requirements.txt .
RUN pip install --use-deprecated=legacy-resolver -r requirements.txt

COPY . .

EXPOSE 8000
HEALTHCHECK --interval=30s --timeout=5s --start-period=20s \
  CMD python -c "import urllib.request,os,sys; sys.exit(0 if urllib.request.urlopen(f'http://127.0.0.1:{os.environ.get(\"PORT\",\"8000\")}/healthz').status==200 else 1)"

# gthread workers so the SSE endpoint (/api/assistant/stream) doesn't block
# other requests; generous timeout for long agent turns.
CMD ["sh", "-c", "gunicorn app:app --worker-class gthread --workers 2 --threads 8 --timeout 180 --bind 0.0.0.0:${PORT:-8000}"]
