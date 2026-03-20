
FROM python:3.11-slim


WORKDIR /app


COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY ingestion/ ./ingestion/


RUN mkdir -p data/raw logs


CMD ["python", "ingestion/run_ingestion.py"]
