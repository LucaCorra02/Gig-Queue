FROM python:3.12-slim
RUN useradd --create-home app
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY main.py .
COPY static ./static
USER app
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8080"]