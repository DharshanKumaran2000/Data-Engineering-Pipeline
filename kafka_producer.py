from kafka import KafkaProducer
import json
import time
import random
from datetime import datetime

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda x: json.dumps(x).encode('utf-8')
)

while True:
    message = {
        "user_id": random.randint(1, 100),
        "product_id": random.randint(1, 50),
        "price": round(random.uniform(10.0, 500.0), 2),
        "timestamp": datetime.utcnow().isoformat()
    }
    producer.send('transactions', value=message)
    print(f"Sent: {message}")
    time.sleep(1)