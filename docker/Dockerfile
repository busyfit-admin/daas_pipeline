FROM python:slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

WORKDIR /app

# Install Python dependencies first to take advantage of layer caching
COPY requirements.txt .
RUN pip install --upgrade pip && pip install -r requirements.txt

ENTRYPOINT ["python"]
