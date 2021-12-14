# temp script, just for testing out the function of adding topics to server
from kafka import KafkaConsumer
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable
import time
import json
from kafka.admin import KafkaAdminClient, NewTopic
from constants import MESSAGE_CODES

KAFKA_PORT = 9092
KAFKA_ADDRESS = "192.168.56.103"  # "127.0.0.1"
balancer_topic = 'balancer-special' # the special topic communicating with load balancer, maybe not needed when it's the same with topic_name
producer = KafkaProducer(
    bootstrap_servers=[f"{KAFKA_ADDRESS}:{KAFKA_PORT}"],
    value_serializer=lambda x: json.dumps(x).encode('utf-8')
)

# create and add topic to server
def add_topic(topic_name):
    producer.send(topic_name, {'info': 'nonsense message.'})    # because I found I always had nodenotready errors initializing kafka admin client, I just make use of auto creation here
    producer.send(
        balancer_topic, 
        {
          'server_id': 'server-pm123',
          'message_code': MESSAGE_CODES['ADD_TOPIC'],
          'topic': topic_name,
          'info': 'Add this topic to your active topic list.'
        }
    )   # inform the server
    time.sleep(1)
    producer.send(topic_name, {'info': 'Now you can receive messages in this channel successfully.'})   # not needed


if __name__ == '__main__':
    while True:
        s = input('Enter a name to add the topic: ')
        add_topic(str(s))