import time
from confluent_kafka import Producer

p = Producer({
    'bootstrap.servers': 'localhost:9092',
})

for i in range(2):
    data = f"Hi from Julien: {i+1}"
    p.produce('test_topic', data.encode('utf-8'))
    time.sleep(1)

p.flush()
