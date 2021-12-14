from time import sleep
from flask import Flask, request
from kafka_communication import KafkaAdminWrapper, KafkaCommunication
from heartbeat_tracker import Heartbeat
from os import environ
from threading import Thread
from constants import MESSAGE_CODES
import sys
import uuid
import random
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
# { <server_id>: { "clients": [], "topics": { <topic_name>: [client_id_1, client_id_2] } } }
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
          return
      else:
        print('Bad "Delete topic" -request: no topic name provided')
        # Send error message back to requester?

    # if message_code == MESSAGE_CODES['ADD_TOPIC']:
    #   if 'topic' in message:
    #     if kafka_admin.create_topic(message['topic']):
    #       # Send topic name back to requester?
    #       return
    #   else:
    #     print('Bad "Add topic" -request: no topic name provided')
    #     # Send error message back to requester?

    

# Start listening for Kafka messages
message_listener_thread = Thread(
  target=kafka_communicator.start_listening,
  args=(message_handler,)
)
message_listener_thread.start()

def handle_heartbeat_timeout(timeouted_servers):
  print(f'Servers: {timeouted_servers} have timeouted their heartbeats!')

  non_crashed_servers = list(set.difference(set(servers.keys()), timeouted_servers))
  if not non_crashed_servers:
    print('No servers left!')
    # Make sure the server_ids of the timeouted servers aren't lost. Save
    # them somewhere so LB can retry this later.
    return

  for server_id in timeouted_servers:
    if server_id in servers:
      topics = servers[server_id]['topics']
      # Select a backup server for the game topics of the crashed server
      backup_server = random.choice(non_crashed_servers)
      kafka_communicator.send_message(
        LOAD_BALANCER_KAFKA_TOPIC,
        {
          'server_id': backup_server,
          'message_code': MESSAGE_CODES['MIGRATE_GAME'],
          'topic': str(list(topics.keys())),
          'info': 'A list of topics to use to migrate a game to new server',
        }
      )
    

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
def get_free_topic_for_client(client_id):
  print('Trying to find a server for client...')
  free_topic = None
  
  for server in servers.keys():
    # Find a server with an odd number of clients if available
    if len(servers[server]["clients"]) % 2 == 1:
      print('Found a server with a client waiting for match!')
      # Find the topic with just one client within that server
      for topic in servers[server]['topics']:
        if len(servers[server]['topics'][topic]) == 1:
          print(f'Found topic: {topic} with one other client connected to it on the server!')
          servers[server]['topics'][topic].append(client_id)
          servers[server]['clients'].append(client_id)
          return topic

  print('Unable to find a free game... Creating a new one...')
  # We did not find a server with a 'ready' topic.
  # So we create a new topic, give that to a random server, and give that topic
  # to the client.
  free_topic = str(uuid.uuid4())
  if not kafka_admin.create_topic(free_topic):
    print('Unable to create a new topic for the game!')

  selected_server = random.choice(list(servers.keys()))
  servers[selected_server]['topics'][free_topic] = [client_id,]
  servers[selected_server]["clients"].append(client_id)
  # TODO: We might want to change the client LIST to an INTEGER, since all the client IDs are already stored in the same data structure

  print(f'Requesting server to add topic: {free_topic} to its subscriptions')
  kafka_communicator.send_message(
    selected_server,
    {
      'message_code': MESSAGE_CODES['ADD_TOPIC'],
      'topic': free_topic,
      'server_id': selected_server,
    }
  )
  
  print(f'Created a new topic: {free_topic}, assigned to server: {selected_server}')
  return free_topic

# When a client registers, we match it with a game server
@app.route("/client/register")
def client_register():
    client_id = request.args.get("id")
    print(f'Client with ID: {client_id} is registering...')
    if not client_id:
      return { "error": "No Client ID received!"}, 400

    topic = get_free_topic_for_client(client_id)
    if not topic:
      # We should spin up a new server if none are free for a new client!
      return {
        "error": "No available servers found!"
      }, 404

    cpu_values_end = psutil.cpu_times()  # end values for benchmark
    physical_memory_values_end = psutil.Process().memory_info()

    # below calculates OS cpu usage and physical memory usage
    sum1 = sum(list(cpu_values_start))
    sum2 = sum(list(cpu_values_end))
    diff = sum2 - sum1
    idlediff = cpu_values_end.idle - cpu_values_start.idle
    iddlepercentage = (idlediff * 100) / diff
    cpuusage = 100 - iddlepercentage
    usedmemory = physical_memory_values_end.rss

    # write results into a file for processing
    f = open("ld.txt", "a")
    f.write(str(cpuusage))
    f.write(",")
    f.write(str(usedmemory))
    f.write(",")
    f.write("load_balancer")
    f.close()
    return {
        "kafka_topic": topic, # Server ID doubles as the topic name
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

  # TODO: For now, the LB topic get a list with two 'None' -items so when we are
  # later looking for topics for client games, we won't assign them to the LB topic
  servers[f"{server_id}"] = { "clients": [], "topics": { LOAD_BALANCER_KAFKA_TOPIC: [None, None] } }  
  print(f'Server with id {server_id} is up, sending Kafka details...')

  return {
      "load_balancer_kafka_topic": LOAD_BALANCER_KAFKA_TOPIC,
      "server_kafka_topic": server_id, # Server ID doubles as the topic name
      "kafka_address": KAFKA_ADDRESS,
      "kafka_port": KAFKA_PORT,
  }
