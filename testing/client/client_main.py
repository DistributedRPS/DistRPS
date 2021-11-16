from flask import Flask
from kafka import KafkaConsumer

app = Flask(__name__)

TOPIC_NAME = "messages"
KAFKA_PORT = 29092
KAFKA_ADDRESS = "kafka-kafka-1" #"127.0.0.1"
KAFKA_GROUP = "test-consumer-group"

consumer = KafkaConsumer(
  TOPIC_NAME,
  group_id = KAFKA_GROUP,
  bootstrap_servers = [f'{KAFKA_ADDRESS}:{KAFKA_PORT}'],
  auto_offset_reset = 'earliest',
  enable_auto_commit = True,
  value_deserializer = lambda x: x.decode('utf-8')
)

if consumer.bootstrap_connected():
  print("Consumer successfully connected!", flush=True)
else:
  print("Consumer failed to connect!", flush=True)

for message in consumer:
  message = message.value
  print(f"MESSAGE: {message}", flush=True)

@app.route("/")
def main_page():
  return '<p>No content, just run this server in the background</p>'