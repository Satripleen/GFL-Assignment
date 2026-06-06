# Java + Python in one image. The base sets JAVA_HOME=/opt/java/openjdk, which
# src/config picks up directly (so it never needs the host's brew openjdk).
FROM eclipse-temurin:17-jre

RUN apt-get update && apt-get install -y --no-install-recommends \
        python3 python3-pip python3-venv \
    && rm -rf /var/lib/apt/lists/*

# Isolated venv on PATH so `python`/`pip` resolve to it.
ENV VIRTUAL_ENV=/opt/venv
RUN python3 -m venv $VIRTUAL_ENV
ENV PATH="$VIRTUAL_ENV/bin:$PATH"

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Pre-warm the Delta Lake jars into the image's ivy cache so `docker run`
# starts Spark without a network round-trip on first launch.
RUN python -c "from pyspark.sql import SparkSession; \
from delta import configure_spark_with_delta_pip; \
configure_spark_with_delta_pip(SparkSession.builder).getOrCreate().stop()"

COPY . .
ENV PYTHONPATH=/app

# Default: run the whole Bronze->Gold pipeline.
CMD ["python", "-m", "src.pipeline"]
