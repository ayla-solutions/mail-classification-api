FROM python:3.11-slim

# Set working directory
WORKDIR /app

# Install dependencies first to leverage Docker cache
COPY requirements.txt ./
RUN pip install --no-cache-dir --upgrade pip \
    && pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY mail_classification_api ./mail_classification_api

# The API listens on port 8000 by default
EXPOSE 8000

# Entry point launches Uvicorn with the unified API
CMD ["uvicorn", "mail_classification_api.main:app", "--host", "0.0.0.0", "--port", "8000"]
