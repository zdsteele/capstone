# EDGAR Intelligence Platform — Flask app + in-process agent.
# Talks to Databricks (SQL warehouse, model serving, vector search) and Lakebase
# (Postgres) over the network, so it runs anywhere — this image targets Render.
FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PORT=8000 \
    # cap glibc malloc arenas — with threaded workers the default (8/CPU) bloats
    # RSS badly on small instances (langchain/pyarrow/mlflow stack).
    MALLOC_ARENA_MAX=2 \
    MLFLOW_DISABLE_AGENT_HINT=1

WORKDIR /app

# deps first for layer caching. --use-deprecated=legacy-resolver: the pinned
# langchain 0.3 line stalls pip's new resolver (see requirements.txt).
# The full stack (mlflow + langchain + pandas + pyarrow + sklearn ...) needs
# ~600-800MB resident for one worker — run this on a 2GB instance (render.yaml
# plan: standard). Trimming mlflow->skinny was tried and fights
# databricks-langchain's conflicting unitycatalog deps; not worth it.
COPY requirements.txt .
RUN pip install --use-deprecated=legacy-resolver -r requirements.txt \
 && python -c "from databricks_langchain import ChatDatabricks; print('import check OK')"

COPY . .

EXPOSE 8000
HEALTHCHECK --interval=30s --timeout=5s --start-period=20s \
  CMD python -c "import urllib.request,os,sys; sys.exit(0 if urllib.request.urlopen(f'http://127.0.0.1:{os.environ.get(\"PORT\",\"8000\")}/healthz').status==200 else 1)"

# ONE gthread worker — the langchain/mlflow/pyarrow import stack is ~400-500MB
# per process, so 2 workers OOMs a 512MB instance. 8 threads still serve
# concurrent requests (and the SSE endpoint) fine. --max-requests recycles the
# worker periodically to shed any slow buffer growth (pyarrow / sql connector).
CMD ["sh", "-c", "gunicorn app:app --worker-class gthread --workers 1 --threads 8 --timeout 180 --max-requests 400 --max-requests-jitter 80 --bind 0.0.0.0:${PORT:-8000}"]
