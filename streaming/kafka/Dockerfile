
FROM python:3.11-slim

WORKDIR /app

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

RUN apt-get update && apt-get install -y \
    gcc \
    librdkafka-dev \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

COPY streaming/ ./streaming/
COPY validation/ ./validation/
COPY schema/ ./schema/

CMD ["python", "-m", "streaming.kafka.weather_streaming"]
