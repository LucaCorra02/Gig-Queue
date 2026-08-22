import json
import os
from contextlib import asynccontextmanager
import asyncio
import sys
from confluent_kafka import KafkaException, Producer, TopicPartition, Consumer
from pydantic import BaseModel, StringConstraints, Field
from fastapi import FastAPI, HTTPException, Request, Query
from loguru import logger
from typing import Annotated, Optional
import uuid
import time
import redis

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
TOPIC_REQUESTS = os.getenv("TOPIC_REQUESTS", "topic-requests")
DELIVERY_TIMEOUT_S = float(os.getenv("DELIVERY_TIMEOUT_S", "10"))
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")
QUEUE_TTL_S = int(os.getenv("QUEUE_TTL_S", "86400"))
INVENTORY_GROUP = os.getenv("INVENTORY_GROUP", "group-inventory")
MAX_QUANTITY = int(os.getenv("MAX_QUANTITY", "6"))

logger.remove()
logger.add(sys.stderr, format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level: <8}</level> | <cyan>{name}</cyan>:<cyan>{function}</cyan> - <level>{message}</level>")
rdb = redis.from_url(REDIS_URL, decode_responses=True)

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

monitor = Consumer({ # This consumer never subscribes to any topic, it is only used to monitor order status
    "bootstrap.servers": KAFKA_BOOTSTRAP,
    "group.id": INVENTORY_GROUP,
    "enable.auto.commit": False,
})

def fetch_commit() -> dict[int,int]: # {partition: committed_offset} number of order that have been processed by the inventory service
    meta = monitor.list_topics(TOPIC_REQUESTS, timeout=10.0)
    partitions = [ TopicPartition(TOPIC_REQUESTS, p) for p in meta.topics[TOPIC_REQUESTS].partitions]
    committed = monitor.committed(partitions, timeout=10)
    return {tp.partition: tp.offset for tp in committed if tp.offset >= 0}

committed_offsets = {} # {partition: committed_offset} number of order that have been processed by the inventory service
throughput = {} # {partition: throughput} number of order that have been processed by the inventory service per second

async def queue_monitor():
    global committed_offsets
    previous, previous_ts = {}, time.monotonic()

    while True:
        try:
            current = await asyncio.to_thread(fetch_commit)
            now = time.monotonic()
            elapsed = now - previous_ts # time elapsed since last fetch_commit() call

            if previous and elapsed > 0:
                for partition, offset in current.items():
                    consumed = offset - previous.get(partition, offset) # number of orders processed since last call
                    rate = consumed / elapsed
                    throughput[partition] = 0.6 * throughput.get(partition, rate) + 0.4 * rate

            committed_offsets = current
            previous, previous_ts = current, now
        except Exception as e:
            logger.warning(f"Queue monitor failed: {e}")
        await asyncio.sleep(1.0)



async def kafka_poller(): # ask for kafka response
    while True:
        producer.poll(0.1)
        await asyncio.sleep(0.01)

@asynccontextmanager
async def server_life(app: FastAPI):
    bg_poller = asyncio.create_task(kafka_poller())
    logger.info("Kafka producer created, background poller started")
    bg_monitor = asyncio.create_task(queue_monitor())
    yield
    bg_poller.cancel()
    latest_msg = producer.flush(5.0) #send last messages
    if latest_msg:
        logger.warning(f"Kafka producer flush timeout: {latest_msg} messages not delivered")
    bg_monitor.cancel()
    monitor.close()

app = FastAPI(lifespan=server_life)

NonEmpty = Annotated[str, StringConstraints(strip_whitespace=True, min_length=1)]

class BuyRequest(BaseModel):
    event_id: NonEmpty
    user_id: NonEmpty
    quantity: int = Field(default=1, ge=1, le=MAX_QUANTITY)

class BuyResponse(BaseModel):
    order_id: str
    status: str
    quantity: int
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

    ip = client_ip(request)
    blocked = await asyncio.to_thread(
        rdb.exists, f"blocked:user:{body.user_id}", f"blocked:ip:{ip}"
    )
    if blocked:
        logger.warning(f"Blocked request user={body.user_id} ip={ip}")
        raise HTTPException(status_code=403, detail="Request rejected")
    
    order_id = uuid.uuid4().hex
    payload = {
        "order_id": order_id,
        "event_id": body.event_id,
        "user_id": body.user_id,
        "quantity": body.quantity,
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

    try:
        # Increment the queue for this event
        seq = await asyncio.to_thread(rdb.incr, f"queue_seq:{body.event_id}")
        await asyncio.to_thread(rdb.expire, f"queue_seq:{body.event_id}", QUEUE_TTL_S)
        # Store order partition and offset to allow clients to query the order status
        await asyncio.to_thread(
            rdb.setex, f"queue:{order_id}", QUEUE_TTL_S,
            json.dumps({"partition": msg.partition(), "offset": msg.offset(),
                        "seq": seq, "event": body.event_id})
        )
    except Exception as e:
        logger.warning(f"Failed to store order {order_id} status in Redis: {e}")

    return BuyResponse(
        order_id=payload["order_id"],
        status="queued",
        quantity=body.quantity,
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

class StatusResponse(BaseModel):
    order_id: str
    status: str
    queue_ahead: Optional[int] = None
    eta_seconds: Optional[float] = None
    seat: Optional[int] = None
    last_seat: Optional[int] = None
    quantity: Optional[int] = None
    reason: Optional[str] = None

def offsets_ahead(partition: int, offset: int) -> int:
    head = committed_offsets.get(partition)
    if head is None: return 0
    return max(0, offset - head) # Number of orders ahead in the queue for this partition


def eta_seconds(partition: int, ahead: int) -> Optional[float]:
    if ahead <= 0: return 0.0
    rate = throughput.get(partition, 0.0)
    if rate <= 0.1: return None
    return round(ahead / rate, 1)


@app.get("/status", response_model=StatusResponse)
async def order_status(order_id: str = Query(..., min_length=8)):
    try:
        position = await asyncio.to_thread(rdb.get, f"queue:{order_id}")
        record = await asyncio.to_thread(rdb.hgetall, f"order:{order_id}")
    except redis.RedisError as e:
        logger.error(f"Redis unreachable: {e}")
        raise HTTPException(status_code=503, detail="Redis temporarily unavailable")

    # Order has been processed
    if record:
        if record["status"] == "confirmed":
            return StatusResponse(
                order_id=order_id, status="confirmed",
                queue_ahead=0, eta_seconds=0.0,
                seat=int(record["seat"]),
                last_seat=int(record.get("last_seat", record["seat"])),
                quantity=int(record.get("quantity", 1)),
            )
        return StatusResponse(
            order_id=order_id, status="rejected",
            queue_ahead=0, eta_seconds=0.0,
            quantity=int(record.get("quantity", 1)),
            reason=record.get("reason", "sold_out"),
        )

    if not position: raise HTTPException(status_code=404, detail="Unknown order")

    info = json.loads(position)
    event_id = info["event"]
    done = await asyncio.to_thread(rdb.get, f"queue_done:{event_id}") # current order
    ahead = max(0, info["seq"] - int(done or 0) - 1)
    eta = eta_seconds(info["partition"], offsets_ahead(info["partition"], info["offset"])) # ETA based on all the orders in the partition

    return StatusResponse(
        order_id=order_id,
        status="queued" if ahead > 0 else "processing",
        queue_ahead=ahead,
        eta_seconds=eta,
    )