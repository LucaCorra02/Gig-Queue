import json
import os
from contextlib import asynccontextmanager
import asyncio
import sys
from confluent_kafka import KafkaException, Producer
from pydantic import BaseModel, StringConstraints
from fastapi import FastAPI, HTTPException, Request
from loguru import logger
from typing import Annotated
import uuid
import time

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
TOPIC_REQUESTS = os.getenv("TOPIC_REQUESTS", "topic-requests")
DELIVERY_TIMEOUT_S = float(os.getenv("DELIVERY_TIMEOUT_S", "10"))

logger.remove()
logger.add(sys.stderr, format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level: <8}</level> | <cyan>{name}</cyan>:<cyan>{function}</cyan> - <level>{message}</level>")

conf = {
    'bootstrap.servers': KAFKA_BOOTSTRAP,
    'acks': 'all',
    'enable.idempotence': True,
    'max.in.flight.requests.per.connection': 5,
    "retries": 10,
    "retry.backoff.ms": 100,
    "delivery.timeout.ms": 30000,
    "linger.ms": 5, # batching
    "compression.type": "snappy",
    "client.id": f"ticket-api-{os.getpid()}",
}
producer = Producer(conf)

async def kafka_poller(): # ask for kafka response
    while True:
        producer.poll(0.1)
        await asyncio.sleep(0.01)

@asynccontextmanager
async def server_life(app: FastAPI):
    bg_poller = asyncio.create_task(kafka_poller())
    logger.info("Kafka producer created, background poller started")
    yield
    bg_poller.cancel()
    latest_msg = producer.flush(5.0) #send last messages
    if latest_msg:
        logger.warning(f"Kafka producer flush timeout: {latest_msg} messages not delivered")

app = FastAPI(lifespan=server_life)

NonEmpty = Annotated[str, StringConstraints(strip_whitespace=True, min_length=1)]

class BuyRequest(BaseModel):
    event_id: NonEmpty
    user_id: NonEmpty

class BuyResponse(BaseModel):
    order_id: str
    status: str
    partition: int
    offset: int

def client_ip(request: Request) -> str:
    forwarded = request.headers.get("x-forwarded-for", "")
    if forwarded: return forwarded.split(",")[0].strip()
    return request.client.host if request.client else "unknown"

@app.post("/buy", response_model=BuyResponse, status_code=202)
async def buy_ticket(body: BuyRequest, request: Request):
    loop = asyncio.get_running_loop()
    future = loop.create_future()

    def check_delivery(err,msg):
        if not future.done():
            if err:
                loop.call_soon_threadsafe(future.set_exception, KafkaException(err))
            else:
                loop.call_soon_threadsafe(future.set_result, msg)

    order_id = uuid.uuid4().hex
    payload = {
        "order_id": order_id,
        "event_id": body.event_id,
        "user_id": body.user_id,
        "client_ip": client_ip(request),
        "timestamp_ms": int(time.time() * 1000)
    }

    try:
        producer.produce(
            topic= TOPIC_REQUESTS,
            key= body.event_id.encode('utf-8'),
            value=json.dumps(payload).encode('utf-8'),
            callback=check_delivery
        )
    except BufferError:
        logger.warning("Kafka producer queue is full")
        raise HTTPException(status_code=503, detail="Kafka producer queue is full")

    try:
        msg = await asyncio.wait_for(future, timeout=DELIVERY_TIMEOUT_S)
    except asyncio.TimeoutError:
        logger.error("Kafka delivery report timeout")
        raise HTTPException(status_code=504, detail="Kafka delivery report timeout")
    except KafkaException as e:
        logger.error(f"Kafka delivery report error: {e}")
        raise HTTPException(status_code=503, detail=f"Kafka delivery report error: {e}")

    return BuyResponse(
        order_id=payload["order_id"],
        status="queued",
        partition=msg.partition(),
        offset=msg.offset()
    )

@app.get("/healthz")
async def healthz():
    try:
        await asyncio.to_thread(producer.list_topics, TOPIC_REQUESTS, 3.0)
    except KafkaException:
        raise HTTPException(status_code=503, detail="Kafka not reachable")
    return {"status": "ok"}