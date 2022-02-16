FROM python:3

COPY scripts/kafka/kafka_producer.py .
COPY scripts/kafka/requirements.txt .

RUN pip install -r requirements.txt

ENTRYPOINT ["python", "kafka_producer.py"]