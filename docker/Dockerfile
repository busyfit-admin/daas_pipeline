FROM python:slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

WORKDIR /app

# Install Python dependencies first to take advantage of layer caching
COPY requirements.txt .
# build-essential is only needed to compile packages that don't ship wheels for the
# newest Python yet (e.g. zstandard, used by pyiceberg); it is removed afterwards.
RUN apt-get update \
    && apt-get install -y --no-install-recommends build-essential \
    && pip install --upgrade pip \
    && pip install -r requirements.txt \
    && apt-get purge -y --auto-remove build-essential \
    && rm -rf /var/lib/apt/lists/*

ENTRYPOINT ["python"]
