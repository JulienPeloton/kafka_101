#!/bin/python3
# A simple Kafka Consumer running on a secured Kafka Cluster

from confluent_kafka import Consumer, KafkaError

c = Consumer({
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'consumers',
    'auto.offset.reset': 'earliest',
})

c.subscribe(['test_topic'])

count = 0
while True:
    msg = c.poll(10.0)

    if count > 3:
        print("3 seconds without message. Exiting...")
        break

    if msg is None:
        count += 1
        continue
    if msg.error():
        print("Consumer error: {}".format(msg.error()))
        continue

    print('Received message: {}'.format(msg.value().decode('utf-8')))

c.close()
