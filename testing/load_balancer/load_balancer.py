from flask import Flask
from kafka import KafkaAdminClient
import time

app = Flask(__name__)

# This kafka address and port should come from configuration
KAFKA_ADDRESS = "kafka-kafka-1"
KAFKA_PORT = 29092

clients = {}
servers = {}
kafka_topics = [ "messages" ]

retries = 0
kafkaAdmin = None
while kafkaAdmin == None and retries <= 10:
  try:
    kafkaAdmin = KafkaAdminClient()
  except:
    print("Unable to start kafka admin, retrying...", flush=True)
    time.sleep(1)
    retries += 1
    if retries > 10:
      print('Unable to start kafka admin after 10 retries, giving up..', flush=True)

def add_client(client_address, client_id):
  clients[f"{client_id}"] = client_address

def add_server(server_address, server_id, kafka_topic):
  servers[f"{server_id}"] = {
    "address": server_address,
    "topic": kafka_topic,
  }

def add_kafka_topic(topic_name):
  pass

@app.route("/client/register")
def client_register():
  # implement some algorithm to pick a suitable topic to return
  return kafka_topics[0]

@app.route("/server/register")
def server_register():
  return "server registration"