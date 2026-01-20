
FROM python:3.11-slim

WORKDIR /app

# System deps (optional: build tools)
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# Install Docker Client (Static Binary)
RUN curl -fsSL https://download.docker.com/linux/static/stable/x86_64/docker-26.1.3.tgz -o docker.tgz \
    && tar xzvf docker.tgz \
    && mv docker/docker /usr/bin/docker \
    && rm -rf docker docker.tgz

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY app ./app

ENV PYTHONUNBUFFERED=1
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]
