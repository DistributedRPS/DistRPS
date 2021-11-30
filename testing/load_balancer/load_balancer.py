from flask import Flask
from kafka_communication import KafkaAdminWrapper
import sys

app = Flask(__name__)

kafka_admin = KafkaAdminWrapper()

# This kafka address and port should come from configuration
KAFKA_ADDRESS = "192.168.56.103"
KAFKA_PORT = 9092

clients = {}
servers = {}
kafka_topics = ["messages"]

# Get commandline arguments
args = sys.argv[1:]
if args and args[0] == "-docker":
    KAFKA_ADDRESS = "kafka-kafka-1"
    KAFKA_PORT = 29092
elif args and args[0] == "-vm":
    KAFKA_ADDRESS = "<address of vm running kafka instance>"

if not kafka_admin.connect(KAFKA_ADDRESS, KAFKA_PORT):
    print(
        (f"Failed to connect to Kafka broker with address: {KAFKA_ADDRESS}"
         f" and port: {KAFKA_PORT}"),
        flush=True)


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
    # implement some algorithm to pick a suitable topic to return
    get_free_topic_for_client()

    return {
        "kafka_topic": kafka_topics[0],
        "kafka_address": KAFKA_ADDRESS,
        "kafka_port": KAFKA_PORT,
    }


@app.route("/server/register")
def server_register():
    new_topic_name = "unique_topic_name_given_by_server"
    # if add_kafka_topic(new_topic_name):
    return {
        "kafka_topic": kafka_topics[0],
        "kafka_address": KAFKA_ADDRESS,
        "kafka_port": KAFKA_PORT,
    }
    # else:
    #  return { "error": "Topic could not be created" }, 500
