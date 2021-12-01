from kafka import KafkaProducer, KafkaConsumer, producer
from kafka.errors import NoBrokersAvailable
from kafka_communication import KafkaCommunication
import requests
import time
import sys
import json
import server_game
import uuid

TOPIC_NAME = "messages"
KAFKA_PORT = 9092
KAFKA_ADDRESS = "server"
LOAD_BALANCER_ADDRESS = "127.0.0.1"

kafka_communicator = KafkaCommunication()

id = uuid.uuid4()
server_id = f'{str(id)}'

# Get commandline arguments
args = sys.argv[1:]
if args and args[0] == "-docker":
  LOAD_BALANCER_ADDRESS = "kafka-load_balancer-1"
  KAFKA_PORT = 29092
  KAFKA_ADDRESS = "kafka-kafka-1"
elif args and args[0] == "-vm":
  LOAD_BALANCER_ADDRESS = "192.168.56.101"

print(f"Using {LOAD_BALANCER_ADDRESS} as load balancer address.", flush=True)

# Fetch Kafka info from the load balancer node
try:
  print("Fetching a Kafka topic to connect to...", flush=True)
  # Add server ID here.
  response = requests.get(
    f"http://{LOAD_BALANCER_ADDRESS}:5000/server/register",
    params={ 'id': server_id },
  )

  data = None

  if response.status_code != 200:
    print("Something went wrong...", flush=True)
    try:
      data = response.json()
      print(f"Error: {data['error']}", flush=True)
    except BaseException as error:
      print("Unable to parse error from response!", flush=True)
      raise
    raise BaseException
  else:
    try:
      data = response.json()
      TOPIC_NAME = data["kafka_topic"]
      KAFKA_ADDRESS = data["kafka_address"]
      KAFKA_PORT = data["kafka_port"]
      print(f"Received topic name: {TOPIC_NAME}", flush=True)
      print(f"Received kafka address: {KAFKA_ADDRESS}", flush=True)
      print(f"Received port: {KAFKA_PORT}", flush=True)
    except BaseException as error:
      print("Unable to parse JSON data from the response!", flush=True)

except BaseException as error:
    print("Unable to fetch Kafka details from load balancer!", flush=True)
    print(f"Error: {error}", flush=True)

kafka_communicator.initialize_consumer(KAFKA_ADDRESS, KAFKA_PORT, ["messages"])
kafka_communicator.initialize_producer(KAFKA_ADDRESS, KAFKA_PORT)

kafka_communicator.send_message(TOPIC_NAME, "Does this work?")

def send_message(message):
  # Add flag to message so server knows that it doesn't need to react to it
  # when it's own Consumer sees it.
  print(f"Sending message: {message}", flush=True)
  kafka_communicator.send_message(TOPIC_NAME, bytes(f'{ "data": {message} }', 'utf-8'))

server_game.game_service(TOPIC_NAME, server_id)
