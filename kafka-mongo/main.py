from fastapi import FastAPI
from aiokafka import AIOKafkaConsumer
from pymongo import MongoClient
import asyncio
import json
import logging
from contextlib import asynccontextmanager
import uvicorn
from pymongo.server_api import ServerApi

uri = ""
db_client = MongoClient(uri, server_api=ServerApi('1'))
db = db_client["log"]
cpu_usage_log_collection = db["cpu_usage_log"]
file_log_collection = db["file_log"]
process_log_collection = db["process_log"]
network_log_collection = db["network_log"]

KAFKA_SERVER = ""
logging.basicConfig(level=logging.INFO)

KAFKA_TOPICS = ['log-process', 'log-file-operation', 'log-cpu-usage', 'log-network']

@asynccontextmanager
async def lifespan(app: FastAPI):
    consumer = None

    while consumer is None:
        try:
            logging.info("Trying to connect to Kafka...")
            consumer = AIOKafkaConsumer(
                *KAFKA_TOPICS,
                bootstrap_servers=KAFKA_SERVER,
                group_id="log_consumer_group",
                auto_offset_reset='latest'
            )
            await consumer.start()
            logging.info("Connected to Kafka successfully!")
        except Exception as e:
            logging.error(f"Kafka connection failed: {e}. Retrying in 30 seconds...")
            consumer = None
            await asyncio.sleep(30)
    
    async def process_messages():
        try:
            async for message in consumer:
                topic = message.topic
                value = message.value.decode('utf-8')
                
                logging.info(f"Received message from {topic}: {value}")
                
                try:
                    msg_data = {
                        "topic": topic,
                        "raw_value": value
                    }
                    
                    if topic == "log-cpu-usage":
                        cpu_usage_log_collection.insert_one(msg_data)
                    elif topic == "log-file-operation":
                        file_log_collection.insert_one(msg_data)
                    elif topic == "log-process":
                        process_log_collection.insert_one(msg_data)
                    elif topic == "log-network":
                        network_log_collection.insert_one(msg_data)
                        
                except Exception as e:
                    logging.error(f"Error processing message: {e}")
                    
        except Exception as e:
            logging.error(f"Kafka consumer error: {e}")
    
    task = asyncio.create_task(process_messages())
    
    logging.info("Kafka consumer started successfully")
    yield
    
    logging.info("Shutting down Kafka consumer...")
    task.cancel()
    await consumer.stop()
    logging.info("Kafka consumer stopped")

app = FastAPI(lifespan=lifespan)

@app.get("/")
async def root():
    return {"status": "running", "message": "Kafka consumer is active"}

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)