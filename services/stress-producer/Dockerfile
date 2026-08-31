FROM python:3.12-slim
RUN useradd --create-home app
WORKDIR /app
RUN pip install --no-cache-dir requests
COPY stress-producer.py .
USER app
CMD ["python", "-u", "stress-producer.py"]