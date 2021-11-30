from flask import Flask, render_template, request
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable
import requests
import time
import sys

app = Flask(__name__)

TOPIC_NAME = "messages"
KAFKA_PORT = 9092
KAFKA_ADDRESS = "server"
LOAD_BALANCER_ADDRESS = "client"

# Get commandline arguments
args = sys.argv[1:]
if args and args[0] == "-docker":
  LOAD_BALANCER_ADDRESS = "kafka-load_balancer-1"
  KAFKA_PORT = 29092
  KAFKA_ADDRESS = "kafka-kafka-1"
elif args and args[0] == "-vm":
  LOAD_BALANCER_ADDRESS = "<address of vm running the load balancer>"
  KAFKA_ADDRESS = "<address of vm runnig the kafka instance>"

print(f"Using {LOAD_BALANCER_ADDRESS} as load balancer address.", flush=True)

# Fetch Kafka info from the load balancer node
try:
  print("Fetching a Kafka topic to connect to...", flush=True)
  response = requests.get(f"http://{LOAD_BALANCER_ADDRESS}:5000/server/register")

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


# Connect to a Kafka topic with a Producer
retries = 0
producer = None
while producer == None and retries <= 10:
  try:
    producer = KafkaProducer(
      bootstrap_servers=[f"{KAFKA_ADDRESS}:{KAFKA_PORT}"]
    )
  except NoBrokersAvailable:
    print("No brokers available, retrying...", flush=True)
    time.sleep(1)
    retries += 1
    if retries > 10:
      print('Unable to find broker after 10 retries, giving up..', flush=True)


# Flask routes
@app.route("/")
def main_page():
    return render_template('main.html')

@app.route("/message", methods=["GET", "POST"])
def receive_message():
    if request.method == "POST":
      print("Request content: " + request.form["message"], flush=True)
      # put msg into topic
      future = producer.send(TOPIC_NAME, bytes(request.form['message'], 'utf-8'))
      return main_page()
    else:
      print("GET request received", flush=True)
      return main_page()