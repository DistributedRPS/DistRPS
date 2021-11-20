from flask import Flask, render_template, request
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable
import time

app = Flask(__name__)

TOPIC_NAME = "messages"
KAFKA_PORT = 29092
KAFKA_ADDRESS = "kafka-kafka-1" #"127.0.0.1"

# adminClient = KafkaAdminClient(
#   bootstrap_servers=[f"{KAFKA_ADDRESS}:{KAFKA_PORT}"],
#   client_id="admin_client",
# )

# topics = []
# topics.append(NewTopic(TOPIC_NAME, num_partitions=1, replication_factor=1))
# adminClient.create_topics(new_topics=topics, validate_only=False)

producer = None

while producer == None:
  try:
    producer = KafkaProducer(
      bootstrap_servers=[f"{KAFKA_ADDRESS}:{KAFKA_PORT}"]
    )
  except NoBrokersAvailable:
    print("No brokers available, retrying...", flush=True)
    time.sleep(1)

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