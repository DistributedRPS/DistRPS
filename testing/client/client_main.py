from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable
import requests
import time
import sys

# These should probably be parsed from a configuration file instead of
# being hardcoded here
LOAD_BALANCER_ADDRESS = "127.0.0.1" # "kafka-load_balancer-1"
TOPIC_NAME = "messages"
KAFKA_PORT = 29092
KAFKA_ADDRESS = "kafka-kafka-1" #"127.0.0.1"
KAFKA_GROUP = "test-consumer-group"

# Get commandline arguments
args = sys.argv[1:]
if args[0] == "-production":
  LOAD_BALANCER_ADDRESS = "kafka-load_balancer-1"

print(f"Using {LOAD_BALANCER_ADDRESS} as load balancer address.", flush=True)

try:
  print("Fetching a Kafka topic to connect to...", flush=True)
  received_topic_name = requests.get(f"http://{LOAD_BALANCER_ADDRESS}:5000").text
  print(f"Received topic name: {received_topic_name}", flush=True)
  TOPIC_NAME = received_topic_name
except BaseException as error:
  print("Unable to fetch topic name from load balancer!", flush=True)
  print(f"Error: {error}", flush=True)

retries = 0
consumer = None
while consumer == None and retries <= 10:
  try:
    consumer = KafkaConsumer(
      TOPIC_NAME,
      group_id = KAFKA_GROUP,
      bootstrap_servers = [f'{KAFKA_ADDRESS}:{KAFKA_PORT}'],
      auto_offset_reset = 'earliest',
      enable_auto_commit = True,
      value_deserializer = lambda x: x.decode('utf-8')
    )
  except:
    print("No brokers available, retrying...", flush=True)
    time.sleep(1)
    retries += 1
    if retries > 10:
      print('Unable to find broker after 10 retries, giving up..', flush=True)


if consumer.bootstrap_connected():
  print("Consumer successfully connected!", flush=True)
else:
  print("Consumer failed to connect!", flush=True)

for message in consumer:
  message = message.value
  print(f"MESSAGE: {message}", flush=True)