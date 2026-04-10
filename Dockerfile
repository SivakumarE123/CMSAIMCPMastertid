FROM python:3.12-slim

WORKDIR /app

# System dependencies for spaCy and Presidio
RUN apt-get update && \
    apt-get install -y --no-install-recommends gcc g++ && \
    rm -rf /var/lib/apt/lists/*

# Install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Download spaCy model for PII detection
RUN python -m spacy download en_core_web_lg

# Copy application code
COPY main.py .
COPY cosmosservice.py .
COPY piiservice.py .
COPY denylist.py .
COPY mistral.py .
COPY videotranscription.py .
COPY multitranscription.py .

# Azure Container Apps sets PORT env var; default 8090 for local
ENV PORT=8090
EXPOSE 8090

CMD ["python", "main.py"]
