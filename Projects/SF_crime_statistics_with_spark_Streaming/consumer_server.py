import asyncio 
from confluent_kafka import Consumer

BROKER_URL = 'PLAINTEXT://localhost:9092'

async def consume(topic_name):
    
    consumer = Consumer(
        {
            "bootstrap.servers": BROKER_URL,
            "group.id": "0"
        }
    )
    consumer.subscribe([topic_name])
    
    while True:
        message = consumer.poll(1.0)
        if not message:
            print("no message received by consumer")
        elif message.error():
            print(f"error from consumer {message.error()}")
        else:
            print(f"consumed message {message.key()}: {message.value()}")
        await asyncio.sleep(0.1)
    
def run_consumer():
    try:
        asyncio.run(consume("police.service.calls"))
    except KeyboardInterrupt as e:
        print("shutting down")

if __name__ == '__main__':
    run_consumer()