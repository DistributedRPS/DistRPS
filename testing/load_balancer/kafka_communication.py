from kafka import KafkaAdminClient, admin
import time

class KafkaAdminWrapper:

  admin_client = None

  def connect(self, address, port):
    retries = 0
    while self.admin_client == None and retries <= 10:
      try:
        self.admin_client = KafkaAdminClient(
          bootstrap_servers=f"{address}:{port}",
          client_id='load_balancer',
        )
        return True
      except:
        print("Unable to start kafka admin, retrying...", flush=True)
        time.sleep(1)
        retries += 1
        if retries > 10:
          print('Unable to start kafka admin after 10 retries, giving up..', flush=True)
          return False

  def create_topic(self, topic_name):
    topic = admin.NewTopic(
      topic_name,
      1,
      1,
    )

    try:
      self.admin_client.create_topics([ topic ])
      print(f"Created a new Topic, {topic_name}", flush=True)
      return True
    except BaseException as error:
      print(f"Failed creating a new Topic, {topic_name}", flush=True)
      return False

  def delete_topic(self, topic_name):
    try:
      self.admin_client.delete_topics([ topic_name ])
      print(f"Deleted Topic, {topic_name}", flush=True)
      return True
    except BaseException as error:
      print(f"Failed deleting Topic, {topic_name}", flush=True)
      return False