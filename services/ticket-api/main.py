import json
import os
from contextlib import asynccontextmanager
import asyncio
from confluent_kafka import Producer
from pydantic import BaseModel
from fastapi import FastAPI, HTTPException
import uuid



KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
TOPIC_REQUESTS = os.getenv("TOPIC_REQUESTS", "topic-requests")
print(KAFKA_BOOTSTRAP, TOPIC_REQUESTS)

conf = {
    'bootstrap.servers': KAFKA_BOOTSTRAP,
    'acks': 'all',
    'enable.idempotence': True,
    'max.in.flight.requests.per.connection': 5,
}
producer = Producer(conf)

async def kafka_poller(): # ask for kafka response
    while True:
        producer.poll(0.1)
        await asyncio.sleep(0.01)

@asynccontextmanager
async def server_life(app: FastAPI):
    bg_poller = asyncio.create_task(kafka_poller())
    yield
    bg_poller.cancel()
    producer.flush(5.0) #send last messages

app = FastAPI(lifespan=server_life)

class BuyRequest(BaseModel):
    event_id: str
    user_id: str

@app.post("/buy")
async def buy_ticket(req: BuyRequest):
    order_id = uuid.uuid64().hex
    loop = asyncio.get_running_loop()
    future = loop.create_future()

    def check_delivery_error(err,msg):
        if err: 
            loop.call_soon_threadsafe(future.set_exception, Exception(str(err)))
        else:
            loop.call_soon_threadsafe(future.set_result, msg)

    payload = {
        "order_id": order_id,
        "event_id": req.event_id,
        "user_id": req.user_id
    }
    producer.produce(
        topic= TOPIC_REQUESTS,
        key=  req.event_id.encode('utf-8'),
        value=json.dumps(payload).encode('utf-8'),
        callback=check_delivery_error

    )
    try: #wait for kafka response
        msg = await asyncio.wait_for(future, timeout=10.0)
        return {
            "status": "queued",
            "partition": msg.partition(),
            "offset": msg.offset()
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail="write error on Kafka: {e}")
