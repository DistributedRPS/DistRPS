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
LOAD_BALANCER_KAFKA_TOPIC = "load_balancer"
ENV = environ.get("ENV")

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
kafka_communicator.initialize_consumer(
  KAFKA_ADDRESS, KAFKA_PORT, [f"{LOAD_BALANCER_KAFKA_TOPIC}"]
)

def add_client(client_address, client_id):
    clients[f"{client_id}"] = client_address


def add_server(server_address, server_id, kafka_topic):
    servers[f"{server_id}"] = {
        "address": server_address,
        "topic": kafka_topic,
    }


def add_kafka_topic(topic_name):
    return kafka_admin.create_topic(topic_name)

# Go through all registered servers and if found, return the first server 
# that has 1 client. Otherwise return the first server found with 0 clients,
# or if neither is found, return None
def get_free_topic_for_client():
  print('Trying to find a server for client...')
  free_server = None
  
  for server in servers.keys():
    if len(servers[server]["clients"]) == 1:
      print(f'Found a server: {server} with other client connected to it!')
      return server
    if len(servers[server]["clients"]) == 0 and free_server == None:
      print('Found an empty server!')
      free_server = server
  
  print(f'Returning: {free_server}')
  return free_server

# When a client registers, we match it with a game server
@app.route("/client/register")
def client_register():
    client_id = request.args.get("id")
    if not client_id:
      return { "error": "No Client ID received!"}, 400

    server_id = get_free_topic_for_client()
    if not server_id:
      # we should spin up a new server if none are free for a new client!
      return {
        "error": "No available servers found!"
      }, 404

    servers[server_id][clients].append(client_id)
    return {
        "kafka_topic": server_id, # server id doubles as the topic name
        "kafka_address": KAFKA_ADDRESS,
        "kafka_port": KAFKA_PORT,
    }


@app.route("/server/register")
def server_register():
  server_id = request.args.get("id")
  if not server_id:
    print("Server did not provide a Server ID!")
    return { "error": "No Server ID received!" }, 400

  new_topic_name = server_id
  print(f'Creating a new topic: {server_id}, for the registered server...')
  if add_kafka_topic(new_topic_name):
    print(f'Successfully created topic {server_id}')
  else:
    print(f'Unable to create topic {server_id}. It might already exist.')

  servers[f"{server_id}"] = { "clients": [], "tournament": None }  
  print(f'Server with id {server_id} is up, sending Kafka details...')

  return {
      "load_balancer_kafka_topic": LOAD_BALANCER_KAFKA_TOPIC,
      "server_kafka_topic": new_topic_name,
      "kafka_address": KAFKA_ADDRESS,
      "kafka_port": KAFKA_PORT,
  }
