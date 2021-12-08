from time import sleep
from flask import Flask, request
from kafka_communication import KafkaAdminWrapper, KafkaCommunication
from heartbeat_tracker import Heartbeat
from os import environ
from threading import Thread
from constants import MESSAGE_CODES
import sys
import psutil
app = Flask(__name__)

kafka_admin = KafkaAdminWrapper()
kafka_communicator = KafkaCommunication()
heartbeat = Heartbeat()

KAFKA_ADDRESS = "127.0.0.1"
KAFKA_PORT = 9092
LOAD_BALANCER_KAFKA_TOPIC = "load_balancer"
ENV = environ.get("ENV")

clients = {}
# Keep track of all servers in the system, and which clients they are serving
# { <server_id: { "clients": [], "tournaments": None | int } }
servers = {}
kafka_topics = [LOAD_BALANCER_KAFKA_TOPIC]

# Check environment configuration
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

cpu_values_start = psutil.cpu_times() #start values for benchmark
physical_memory_values_start = psutil.Process().memory_info()
kafka_communicator.initialize_producer(KAFKA_ADDRESS, KAFKA_PORT)
kafka_communicator.initialize_consumer(
  KAFKA_ADDRESS, KAFKA_PORT, [f"{LOAD_BALANCER_KAFKA_TOPIC}"]
)

def message_handler(message):
  print(f'Incoming message: {message}')
  if 'data' in message and message['data'] == 'heartbeat':
    heartbeat.receive_heartbeat(message['sender_id'])
    return

  # In case we end up receiving own messages, just disregard them
  if 'sender_id' in message and message['sender_id'] == 'load_balancer':
    return

  if 'message_code' in message:
    message_code = message['message_code']
    print(f'Received a message with code: {message_code}')

    if message_code == MESSAGE_CODES['DELETE_TOPIC']:
      if 'topic' in message:
        if kafka_admin.delete_topic(message['topic']):
          # Inform that topic successfully deleted?
          pass
      else:
        print('Bad "Delete topic" -request: no topic name provided')
        # Send error message back to requester?

    if message_code == MESSAGE_CODES['ADD_TOPIC']:
      if 'topic' in message:
        if kafka_admin.create_topic(message['topic']):
          # Send topic name back to requester?
          pass
      else:
        print('Bad "Add topic" -request: no topic name provided')
        # Send error message back to requester?

    

# Start listening for Kafka messages
message_listener_thread = Thread(
  target=kafka_communicator.start_listening,
  args=(message_handler,)
)
message_listener_thread.start()

def handle_heartbeat_timeout(timeouts):
  print(f'Servers: {timeouts} have timeouted their heartbeats!')
  # Find the backups for each timeouted server
  # Give each backup server the game topics the corresponding crashed server was using

# Start tracking Server heartbeats and notify on unresponsive servers
heartbeat_thread = Thread(
  target=heartbeat.watch_timeouts,
  args=(30, handle_heartbeat_timeout)
)
heartbeat_thread.start()

# def add_client(client_address, client_id):
#     clients[f"{client_id}"] = client_address


def add_server(server_address, server_id, kafka_topic):
    servers[f"{server_id}"] = {
        "address": server_address,
        "topic": kafka_topic,
    }


def create_kafka_topic(topic_name):
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
    print(f'Client with ID: {client_id} is registering...')
    if not client_id:
      return { "error": "No Client ID received!"}, 400

    server_id = get_free_topic_for_client()
    if not server_id:
      # We should spin up a new server if none are free for a new client!
      return {
        "error": "No available servers found!"
      }, 404

    servers[server_id]["clients"].append(client_id)
    return {
        "kafka_topic": server_id, # Server ID doubles as the topic name
        "kafka_address": KAFKA_ADDRESS,
        "kafka_port": KAFKA_PORT,
    }


@app.route("/server/register")
def server_register():
  server_id = request.args.get("id")
  if not server_id:
    print("Server did not provide a Server ID!")
    return { "error": "No Server ID received!" }, 400

  print(f'Creating a new topic: {server_id}, for the registered server...')
  if create_kafka_topic(server_id):
    print(f'Successfully created topic {server_id}')
  else:
    print(f'Unable to create topic {server_id}. It might already exist.')

  servers[f"{server_id}"] = { "clients": [], "tournament": None }  
  print(f'Server with id {server_id} is up, sending Kafka details...')

  cpu_values_end = psutil.cpu_times()  # end values for benchmark
  physical_memory_values_end = psutil.Process().memory_info()
  # below calculates OS cpu usage and physical memory usage
  sum1 = sum(list(cpu_values_start))
  sum2 = sum(list(cpu_values_end))
  diff = sum2 - sum1
  idlediff = cpu_values_end.idle - cpu_values_start.idle
  iddlepercentage = (idlediff * 100) / diff
  cpuusage = 100 - iddlepercentage
  usedmemory = physical_memory_values_end.rss - physical_memory_values_start.rss

  # write results into a file for processing
  open("load_balancer.txt", 'w').close()
  f = open("load_balancer.txt", "a")
  f.write(str(cpuusage))
  f.write(",")
  f.write(str(usedmemory))
  f.write(",")
  f.write("load_balancer")
  f.close()
  print("Bench results inputted")
  return {
      "load_balancer_kafka_topic": LOAD_BALANCER_KAFKA_TOPIC,
      "server_kafka_topic": server_id, # Server ID doubles as the topic name
      "kafka_address": KAFKA_ADDRESS,
      "kafka_port": KAFKA_PORT,
  }
