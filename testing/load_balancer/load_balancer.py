from time import sleep
from flask import Flask, request
from kafka_communication import KafkaAdminWrapper, KafkaCommunication
from os import environ
import sys

app = Flask(__name__)

kafka_admin = KafkaAdminWrapper()
kafka_communicator = KafkaCommunication()

# This kafka address and port should come from configuration
KAFKA_ADDRESS = "127.0.0.1"
KAFKA_PORT = 9092
ENV = environ["ENV"]

clients = {}
servers = {}
kafka_topics = ["messages"]

# Get commandline arguments
if ENV == "docker":
    KAFKA_ADDRESS = "kafka-kafka-1"
    KAFKA_PORT = 29092
elif ENV == "vm" or ENV == "production":
    KAFKA_ADDRESS = "192.168.56.103"

if not kafka_admin.connect(KAFKA_ADDRESS, KAFKA_PORT):
    print(
        (f"Failed to connect to Kafka broker with address: {KAFKA_ADDRESS}"
         f" and port: {KAFKA_PORT}"),
        flush=True)


kafka_communicator.initialize_producer(KAFKA_ADDRESS, KAFKA_PORT)
# When creating topics, append a recognizable key to the end of the topic names
# Then use that key for the Consumer topic subscription regex
kafka_communicator.initialize_consumer(KAFKA_ADDRESS, KAFKA_PORT, topic_regex='-server\\b')

def add_client(client_address, client_id):
    clients[f"{client_id}"] = client_address


def add_server(server_address, server_id, kafka_topic):
    servers[f"{server_id}"] = {
        "address": server_address,
        "topic": kafka_topic,
    }


def add_kafka_topic(topic_name):
    return kafka_admin.create_topic(topic_name)


def get_free_topic_for_client():
    pass


@app.route("/client/register")
def client_register():
    client_id = request.args.get("id")
    if not client_id:
      return { "error": "No Client ID received!"}, 400

    # implement some algorithm to pick a suitable topic to return
    get_free_topic_for_client()

    return {
        "kafka_topic": kafka_topics[0],
        "kafka_address": KAFKA_ADDRESS,
        "kafka_port": KAFKA_PORT,
    }


@app.route("/server/register")
def server_register():
  print('New server registered, polling for older messages: ')
  subbed_topics = kafka_communicator.get_subscribed_topics()
  print (f'Topic subscribed to: {subbed_topics}')
  msgs = kafka_communicator.poll_messages()
  print(f'Messages: ', msgs)

  server_id = request.args.get("id")
  if not server_id:
    print("Server did not provide a Server ID!")
    return { "error": "No Server ID received!" }, 400
  
  print(f'Server with id {server_id} is up.')
  new_topic_name = f"{server_id}-server"

  if add_kafka_topic(new_topic_name):
    return {
        "kafka_topic": new_topic_name,
        "kafka_address": KAFKA_ADDRESS,
        "kafka_port": KAFKA_PORT,
    }
  else:
     return { "error": "Topic could not be created" }, 500
