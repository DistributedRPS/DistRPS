from flask import Flask, render_template, request
from kafka import KafkaProducer
from kafka.errors import KafkaError

app = Flask(__name__)

TOPIC_NAME = "messages"
KAFKA_PORT = 9092
KAFKA_ADDRESS = "127.0.0.1"

producer = KafkaProducer(
  bootstrap_servers=[f"{KAFKA_ADDRESS}:{KAFKA_PORT}"]
)

@app.route("/")
def main_page():
    return render_template('main.html')

@app.route("/message", methods=["GET", "POST"])
def receive_message():
    if request.method == "POST":
      print("Request content: " + request.form["message"])
      # put msg into topic
      future = producer.send(TOPIC_NAME, bytes(request.form['message'], 'utf-8'))
      return main_page()
    else:
      print("GET request received")
      return main_page()