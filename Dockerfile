FROM python:3.12.3-slim

WORKDIR /app

ENV MONGODB_URI=mongodb://airlines:airlines@34.154.65.11:27017/admin
ENV BUCKET_NAME=airfrance-bucket
ENV DATABASE_NAME=airlines

RUN apt-get update && apt-get install -y git && apt-get clean
COPY requirements.txt .
COPY api.py .

RUN python -m pip install --upgrade pip

RUN pip install --no-cache-dir -r requirements.txt


EXPOSE 8000

CMD ["uvicorn", "api:app", "--host", "0.0.0.0", "--port", "8000", "--reload"]
