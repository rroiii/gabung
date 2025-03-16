from fastapi import FastAPI
from aiokafka import AIOKafkaConsumer
from pymongo import MongoClient, UpdateOne
import asyncio
import json
import logging
from contextlib import asynccontextmanager
import uvicorn
from pymongo.server_api import ServerApi
import time
from typing import Dict, List
from concurrent.futures import ThreadPoolExecutor
import signal

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

uri = ""
db_client = MongoClient(uri, server_api=ServerApi('1'), maxPoolSize=50)
db = db_client["log"]
cpu_usage_log_collection = db["cpu_usage_log"]
file_log_collection = db["file_log"]
process_log_collection = db["process_log"]
network_log_collection = db["network_log"]

KAFKA_SERVER = ""
KAFKA_TOPICS = ['log-process', 'log-file-operation', 'log-cpu-usage', 'log-network']
CONSUMER_GROUP = f"log_consumer_group_{int(time.time())}"

message_buffer: Dict[str, List[dict]] = {
    "log-cpu-usage": [],
    "log-file-operation": [],
    "log-process": [],
    "log-network": []
}
BUFFER_SIZE = 100
BUFFER_FLUSH_INTERVAL = 5

topic_to_collection = {
    "log-cpu-usage": cpu_usage_log_collection,
    "log-file-operation": file_log_collection,
    "log-process": process_log_collection,
    "log-network": network_log_collection
}

thread_executor = ThreadPoolExecutor(max_workers=10)

shutdown_requested = False

async def process_buffer(topic: str):
    """Memproses buffer untuk topic tertentu secara batch"""
    if not message_buffer[topic]:
        return
    
    collection = topic_to_collection[topic]
    documents = message_buffer[topic].copy()
    message_buffer[topic] = []
    
    try:
        if documents:
            logger.info(f"Inserting {len(documents)} documents for topic {topic}")
            collection.insert_many(documents, ordered=False)
    except Exception as e:
        logger.error(f"Error inserting batch for {topic}: {e}")

@asynccontextmanager
async def lifespan(app: FastAPI):
    global shutdown_requested
    consumer = None
    
    loop = asyncio.get_running_loop()
    
    def handle_shutdown(sig, frame):
        global shutdown_requested
        logger.info(f"Received shutdown signal {sig}")
        shutdown_requested = True
    
    for sig in (signal.SIGTERM, signal.SIGINT):
        signal.signal(sig, handle_shutdown)
    
    while consumer is None and not shutdown_requested:
        try:
            logger.info("Connecting to Kafka...")
            consumer = AIOKafkaConsumer(
                *KAFKA_TOPICS,
                bootstrap_servers=KAFKA_SERVER,
                group_id=CONSUMER_GROUP,
                auto_offset_reset='latest',
                max_poll_records=500,
                fetch_max_wait_ms=500,
                fetch_max_bytes=52428800,
                max_partition_fetch_bytes=1048576
            )
            await consumer.start()
            logger.info("Connected to Kafka successfully!")
        except Exception as e:
            logger.error(f"Kafka connection failed: {e}. Retrying in 5 seconds...")
            consumer = None
            await asyncio.sleep(5)
    
    if consumer is None:
        logger.error("Failed to connect to Kafka after retries, application cannot start")
        return
    
    async def periodic_flush():
        while not shutdown_requested:
            for topic in KAFKA_TOPICS:
                await process_buffer(topic)
            await asyncio.sleep(BUFFER_FLUSH_INTERVAL)
    
    async def process_messages():
        try:
            batch = []
            start_time = time.time()
            
            async for message in consumer:
                if shutdown_requested:
                    break
                    
                topic = message.topic
                try:
                    value = json.loads(message.value.decode('utf-8'))
                    message_buffer[topic].append(value)
                    
                    if len(message_buffer[topic]) >= BUFFER_SIZE:
                        await process_buffer(topic)
                        
                except json.JSONDecodeError:
                    logger.warning(f"Invalid JSON in message: {message.value}")
                except Exception as e:
                    logger.error(f"Error processing message: {e}")
                
                batch.append(message)
                if len(batch) >= 10000:
                    elapsed = time.time() - start_time
                    logger.info(f"Processed 10000 messages in {elapsed:.2f} seconds ({10000/elapsed:.2f} msgs/sec)")
                    batch = []
                    start_time = time.time()
                    
        except asyncio.CancelledError:
            logger.info("Message processing task cancelled")
        except Exception as e:
            logger.error(f"Kafka consumer error: {e}")
    
    flush_task = asyncio.create_task(periodic_flush())
    consumer_task = asyncio.create_task(process_messages())
    
    logger.info("Kafka consumer started successfully")
    yield
    
    logger.info("Shutting down Kafka consumer...")
    shutdown_requested = True
    
    flush_task.cancel()
    consumer_task.cancel()
    
    try:
        for topic in KAFKA_TOPICS:
            await process_buffer(topic)
        
        await consumer.stop()
        
        thread_executor.shutdown(wait=True)
        
        logger.info("Kafka consumer stopped")
    except Exception as e:
        logger.error(f"Error during shutdown: {e}")

app = FastAPI(lifespan=lifespan)

@app.get("/")
async def root():
    return {"status": "running", "message": "Kafka consumer is active"}

@app.get("/metrics")
async def metrics():
    buffer_stats = {topic: len(message_buffer[topic]) for topic in KAFKA_TOPICS}
    return {
        "status": "running", 
        "buffer_stats": buffer_stats,
        "shutdown_requested": shutdown_requested
    }

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)