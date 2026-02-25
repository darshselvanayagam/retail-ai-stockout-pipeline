FROM python:3.12-slim
WORKDIR /app
COPY consumer_to_s3.py /app/consumer_to_s3.py
RUN pip install --no-cache-dir confluent-kafka boto3
CMD ["python", "consumer_to_s3.py"]

