import asyncio
import json
import os
from datetime import datetime
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
from contextlib import asynccontextmanager
from themepark_producer import ThemeParkProducer
from themepark_consumer import ThemeParkConsumer

KAFKA_BROKER = os.getenv('KAFKA_BROKER', 'kafka:9092')
RAW_TOPIC = os.getenv('RAW_TOPIC', 'themepark_raw')
AVG_TOPIC = os.getenv('AVG_TOPIC', 'attraction_avg_waittime')
CONSUMER_GROUP = os.getenv('CONSUMER_GROUP', 'themepark-consumer-local')

producer = ThemeParkProducer(KAFKA_BROKER, RAW_TOPIC)
consumer = ThemeParkConsumer(KAFKA_BROKER, AVG_TOPIC, CONSUMER_GROUP)

analytics_connections = []

def notify_analytics_connections(data):
    message = f"data: {json.dumps({'type': 'analytics', 'data': data, 'timestamp': datetime.now().isoformat()})}\\n\\n"
    
    disconnected = []
    for conn in list(analytics_connections):
        try:
            conn.put_nowait(message)
        except:
            disconnected.append(conn)
    
    for conn in disconnected:
        if conn in analytics_connections:
            analytics_connections.remove(conn)

@asynccontextmanager
async def lifespan(app: FastAPI):
    print("🎢 Starting Theme Park Analytics API...")
    
    consumer.add_callback(notify_analytics_connections)
    consumer.start_consuming()
    asyncio.create_task(producer.fetch_and_produce())
    
    yield
    
    print("🛑 Shutting down...")
    producer.stop()
    consumer.stop_consuming()

app = FastAPI(title="Theme Park Analytics API", lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/")
async def root():
    return {"message": "Theme Park Analytics API is running"}

@app.get("/api/attractions")
async def get_attractions():
    return {"attractions": consumer.get_avg_waittimes()}

@app.get("/api/analytics/stream")
async def stream_analytics():
    async def event_generator():
        queue = asyncio.Queue()
        analytics_connections.append(queue)
        
        try:
            initial_data = consumer.get_avg_waittimes()
            if initial_data:
                yield f"data: {json.dumps({'type': 'analytics', 'data': initial_data, 'timestamp': datetime.now().isoformat()})}\\n\\n"
            
            while True:
                try:
                    message = await asyncio.wait_for(queue.get(), timeout=30.0)
                    yield message
                except asyncio.TimeoutError:
                    yield f"data: {json.dumps({'type': 'ping'})}\\n\\n"
        finally:
            if queue in analytics_connections:
                analytics_connections.remove(queue)
    
    return StreamingResponse(
        event_generator(),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "Connection": "keep-alive"}
    )

@app.get("/api/health")
async def health_check():
    return {
        "status": "healthy",
        "timestamp": datetime.now().isoformat(),
        "active_connections": len(analytics_connections)
    }

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8002)
