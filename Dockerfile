FROM python:3.10-slim-bullseye

WORKDIR /app
ADD requirements.txt /app/requirements.txt

ENV PYTHONUNBUFFERED=1 \
    PYTHONUSERBASE=/app \
    PATH="/app/.local/bin:/app/bin:${PATH}" \
    FLASK_ENV=production \
    LOG_LEVEL=WARNING \
    PIP_NO_CACHE_DIR=1

# Install any OS deps you actually need; otherwise skip apt entirely
# RUN apt-get update && apt-get install -y --no-install-recommends <pkgs> && rm -rf /var/lib/apt/lists/*

RUN python -m pip install --upgrade pip && \
    python -m pip install --user -r /app/requirements.txt

ADD src /app
ENTRYPOINT ["python", "/app/api.py", "http://nucleus-frontend.domino-platform:80", "/"]
