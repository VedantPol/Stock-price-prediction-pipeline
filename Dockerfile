# Dockerfile - slim, non-root
FROM python:3.11-slim

LABEL org.opencontainers.image.authors="you@example.com"
LABEL org.opencontainers.image.description="Nifty EOD ETL - yfinance -> parquet/duckdb -> HTML"

ENV APP_HOME=/opt/nifty_etl
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1
ENV PATH="${APP_HOME}/.venv/bin:$PATH"

# System deps
RUN apt-get update \
 && apt-get install -y --no-install-recommends \
      build-essential gcc g++ libstdc++6 libgcc-s1 curl ca-certificates \
 && rm -rf /var/lib/apt/lists/*


WORKDIR $APP_HOME

# Copy and install requirements first for cache
COPY requirements.txt .
RUN python -m pip install --upgrade pip setuptools wheel \
 && pip install --no-cache-dir --prefer-binary -r requirements.txt


# Copy app code
COPY . .

# Make entrypoint executable
COPY entrypoint.sh /usr/local/bin/entrypoint.sh
RUN chmod +x /usr/local/bin/entrypoint.sh

# Non-root user
RUN useradd --create-home --shell /bin/bash appuser \
 && chown -R appuser:appuser $APP_HOME
USER appuser

VOLUME ["/data"]
ENV DATA_DIR=/data/nifty_data
ENV RUN_INTERVAL_SECONDS=86400

WORKDIR $APP_HOME

ENTRYPOINT ["/usr/local/bin/entrypoint.sh"]
CMD ["run"]
