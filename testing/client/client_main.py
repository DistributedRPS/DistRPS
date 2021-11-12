from flask import Flask
from flask.templating import render_template
from kafka import KafkaConsumer

app = Flask(__name__)

TOPIC_NAME = "messages"
KAFKA_PORT = 9092
KAFKA_ADDRESS = "127.0.0.1"
KAFKA_GROUP = "test_group"

consumer = KafkaConsumer(
  TOPIC_NAME,
  group_id = KAFKA_GROUP,
  bootstrap_servers = [f'{KAFKA_ADDRESS}:{KAFKA_PORT}'],
  auto_offset_reset = 'earliest',
  enable_auto_commit = True,
  value_deserializer = lambda x: x.decode('utf-8')
)

for message in consumer:
  message = message.value
  print(message)

@app.route("/")
def main_page():
  return '<p>No content, just run this server in the background</p>'