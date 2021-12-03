from kafka import KafkaConsumer, KafkaProducer
from kafka.errors import NoBrokersAvailable
import time
import json

class KafkaCommunication:
  consumer = None
  producer = None

  def initialize_producer(self, kafka_address, kafka_port):
    print('Initializing KafkaProducer...', flush=True)
    retries = 0
    while self.producer == None and retries <= 10:
      try:
        self.producer = KafkaProducer(
          bootstrap_servers=[f"{kafka_address}:{kafka_port}"]
        )
        return True
      except NoBrokersAvailable:
        print("No brokers available for Producer, retrying...", flush=True)
        time.sleep(1)
        retries += 1
        if retries > 10:
          print('Unable to find broker after 10 retries, giving up..', flush=True)
          return False

  def initialize_consumer(self, kafka_address, kafka_port, topics=[], topic_regex=""):
    print('Initializing KafkaConsumer...', flush=True)
    retries = 0
    while self.consumer == None and retries <= 10:
      try:
        self.consumer = KafkaConsumer(
          bootstrap_servers=[f"{kafka_address}:{kafka_port}"],
          value_deserializer = lambda x: json.loads(x.decode('utf-8'))
        )
        if topic_regex:
          self.consumer.subscribe(pattern=topic_regex)
        elif topics:
          self.consumer.subscribe(topics=topics)
        return True
      except NoBrokersAvailable:
        print("No brokers available for Consumer, retrying...", flush=True)
        time.sleep(1)
        retries += 1
        if retries > 10:
          print('Unable to find broker after 10 retries, giving up..', flush=True)
          return False

  def send_message(self, topic, message):
    print(f'Sending message: {message}')
    self.producer.send(topic, bytes(message, 'utf-8'))

  def poll_messages(self):
    return self.consumer.poll()