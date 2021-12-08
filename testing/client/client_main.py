from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable
import requests
import time
import sys
import client_game
import uuid
import psutil

# These should probably be parsed from a configuration file instead of
# being hardcoded here
LOAD_BALANCER_ADDRESS = "127.0.0.1" # "kafka-load_balancer-1"
TOPIC_NAME = "messages"
KAFKA_PORT = 9092
KAFKA_ADDRESS = "192.168.56.103" #"127.0.0.1"
KAFKA_GROUP = "test-consumer-group"

id = uuid.uuid4()
client_id = 'client' + str(id)

# Get commandline arguments
args = sys.argv[1:]
if args and args[0] == "-docker":
  LOAD_BALANCER_ADDRESS = "kafka-load_balancer-1"
  KAFKA_PORT = 29092
elif args and args[0] == "-vm":
  LOAD_BALANCER_ADDRESS = "192.168.56.101"

print(f"Using {LOAD_BALANCER_ADDRESS} as load balancer address.", flush=True)

# Fetch Kafka info from the load balancer node
try:
  print("Fetching a Kafka topic to connect to...", flush=True)
  cpu_values_start = psutil.cpu_times() #start values for benchmark
  physical_memory_values_start = psutil.Process().memory_info()
  response = requests.get(
    f"http://{LOAD_BALANCER_ADDRESS}:5000/client/register",
    params={ "id": client_id },
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


client_game.game(TOPIC_NAME, client_id)

cpu_values_end = psutil.cpu_times() #end values for benchmark
physical_memory_values_end = psutil.Process().memory_info()

#below calculates OS cpu usage and physical memory usage
sum1 = sum(list(cpu_values_start))
sum2 = sum(list(cpu_values_end))
diff = sum2 - sum1
idlediff = cpu_values_end.idle - cpu_values_start.idle
iddlepercentage = (idlediff * 100) / diff
cpuusage = 100 - iddlepercentage
usedmemory = physical_memory_values_end.rss - physical_memory_values_start.rss

#write results into a file for processing
f = open("client_" + str(client_id) +".txt", "a")
f.write(str(cpuusage))
f.write(",")
f.write(str(usedmemory))
f.write(",")
f.write("client_" + str(client_id))
f.close()
