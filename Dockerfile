# Mail Classifier API
FROM python:3.10-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

# System deps for OCR / PDF
RUN apt-get update && DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
    tesseract-ocr poppler-utils fonts-dejavu-core \
 && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

EXPOSE 8000

# LOG_LEVEL can be INFO|DEBUG; SERVICE_NAME helps identify the app in logs
ENV SERVICE_NAME="mail-classifier"

CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]
