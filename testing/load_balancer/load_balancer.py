from flask import Flask
import sys

app = Flask(__name__)

# This kafka address and port should come from configuration
KAFKA_ADDRESS = "127.0.0.1"
KAFKA_PORT = 9092

clients = {}
servers = {}
kafka_topics = [ "messages" ]

# Get commandline arguments
args = sys.argv[1:]
if args and args[0] == "-docker":
  KAFKA_ADDRESS = "kafka-kafka-1"
  KAFKA_PORT = 29092
elif args and args[0] == "-vm":
  KAFKA_ADDRESS = "<address of vm running kafka instance>"

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
  return {
    "kafka_topic": kafka_topics[0],
    "kafka_address": KAFKA_ADDRESS,
    "kafka_port": KAFKA_PORT,
  }

@app.route("/server/register")
def server_register():
  return {
    "kafka_topic": kafka_topics[0],
    "kafka_address": KAFKA_ADDRESS,
    "kafka_port": KAFKA_PORT,
  }