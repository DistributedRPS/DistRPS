from kafka import KafkaAdminClient, admin, KafkaProducer, KafkaConsumer
from kafka.errors import NoBrokersAvailable
from typing import Callable
import time
import json

class KafkaAdminWrapper:

  admin_client = None

  def connect(self, address, port):
    print(f'Initializing KafkaAdmin with address: {address} and port: {port}')
    retries = 0
    while self.admin_client == None and retries <= 10:
      try:
        self.admin_client = KafkaAdminClient(
          bootstrap_servers=f"{address}:{port}",
          client_id='load_balancer',
        )
        return True
      except:
        print("Unable to start kafka admin, retrying...", flush=True)
        time.sleep(1)
        retries += 1
        if retries > 10:
          print('Unable to start kafka admin after 10 retries, giving up..', flush=True)
          return False

  def create_topic(self, topic_name):
    topic = admin.NewTopic(
      topic_name,
      1,
      1,
    )

    try:
      self.admin_client.create_topics([ topic ])
      print(f"Created a new Topic, {topic_name}", flush=True)
      return True
    except BaseException as error:
      print(f"Failed creating a new Topic, {topic_name}", flush=True)
      return False

  def delete_topic(self, topic_name):
    try:
      self.admin_client.delete_topics([ topic_name ])
      print(f"Deleted Topic, {topic_name}", flush=True)
      return True
    except BaseException as error:
      print(f"Failed deleting Topic, {topic_name}", flush=True)
      return False

class KafkaCommunication:
  # The Consumers topic list needs to be updated every time a new server joins.
  # Maybe a regex subscription will also receive messages from topics that did
  # not exist when the Consumer was created? So no explicit adding needed?
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
    self.producer.send(topic, bytes(message, 'utf-8'))

  def poll_messages(self):
    return self.consumer.poll()
  
  def get_subscribed_topics(self):
    return self.consumer.topics()

  def start_listening(self, callback: Callable[[dict], any]):
    print('Started listening for messages...')
    for message in self.consumer:
      print(f'Message: {message.value}')
      callback(message.value)