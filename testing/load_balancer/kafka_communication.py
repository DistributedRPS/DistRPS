from kafka import KafkaAdminClient
import time

def connect():
  retries = 0
  kafkaAdmin = None
  while kafkaAdmin == None and retries <= 10:
    try:
      kafkaAdmin = KafkaAdminClient()
    except:
      print("Unable to start kafka admin, retrying...", flush=True)
      time.sleep(1)
      retries += 1
      if retries > 10:
        print('Unable to start kafka admin after 10 retries, giving up..', flush=True)

def create_topic(topic_name):
  pass

def delete_topic(topic_name):
  pass