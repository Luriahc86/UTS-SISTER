FROM python:3.11-slim

WORKDIR /app

# Create non-root user and own the working directory
RUN adduser --disabled-password --gecos '' appuser \
    && chown -R appuser:appuser /app

USER appuser

# Install dependencies first (layer-cached separately from source)
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

# Copy application source
COPY src/ ./src/

# Persistent dedup database lives here
VOLUME /app/data

EXPOSE 8080

CMD ["python", "-m", "src.main"]
