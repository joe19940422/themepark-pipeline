FROM python:3.11-slim
WORKDIR /app
COPY kafka_producer.py .
# Install required Python libraries
RUN pip install requests kafka-python
CMD ["python", "kafka_producer.py"]