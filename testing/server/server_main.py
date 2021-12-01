from kafka import KafkaProducer, KafkaConsumer
from kafka.errors import NoBrokersAvailable
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

consumer = None
producer = None

id = uuid.uuid4()
server_id = 'server' + str(id)

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
    params={ id: server_id },
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

def connect_producer():
  retries = 0
  while producer == None and retries <= 10:
    try:
      producer = KafkaProducer(
        bootstrap_servers=[f"{KAFKA_ADDRESS}:{KAFKA_PORT}"]
      )
    except NoBrokersAvailable:
      print("No brokers available for Producer, retrying...", flush=True)
      time.sleep(1)
      retries += 1
      if retries > 10:
        print('Unable to find broker after 10 retries, giving up..', flush=True)

# We need a consumer to receive data from the load_balancer.
# The same server-specific topic is used for both directions of communication
# The messages will contain a flag so server/LB know if it's their own message.
def connect_consumer():
  retries = 0
  while consumer == None and retries <= 10:
    try:
      consumer = KafkaConsumer(
        TOPIC_NAME,
        bootstrap_servers=[f"{KAFKA_ADDRESS}:{KAFKA_PORT}"],
        value_deserializer = lambda x: json.loads(x.decode('utf-8'))
      )
    except NoBrokersAvailable:
      print("No brokers available for Consumer, retrying...", flush=True)
      time.sleep(1)
      retries += 1
      if retries > 10:
        print('Unable to find broker after 10 retries, giving up..', flush=True)

connect_producer()
connect_consumer()

def send_message(message):
  # Add flag to message so server knows that it doesn't need to react to it
  # when it's own Consumer sees it.
  print(f"Sending message: {message}", flush=True)
  producer.send(TOPIC_NAME, bytes(f'{ "data": {message} }', 'utf-8'))

# Flask routes
# @app.route("/")
# def main_page():
#     return render_template('main.html')

# @app.route("/message", methods=["GET", "POST"])
# def receive_message():
#     if request.method == "POST":
#       print("Request content: " + request.form["message"], flush=True)
#       # put msg into topic
#       future = producer.send(TOPIC_NAME, bytes(request.form['message'], 'utf-8'))
#       return main_page()
#     else:
#       print("GET request received", flush=True)
#       return main_page()

server_game.game_service(TOPIC_NAME, server_id)
