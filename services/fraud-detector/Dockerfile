FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1

WORKDIR /app

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY main.py detect.lua ./

RUN useradd --create-home --shell /usr/sbin/nologin app && chown -R app /app
USER app

CMD ["python", "main.py"]