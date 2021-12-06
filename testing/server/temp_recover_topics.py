# temp script, just for testing out the function of adding topics to server
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable
import time
import json

KAFKA_PORT = 9092
KAFKA_ADDRESS = "192.168.56.103"  # "127.0.0.1"
balancer_topic = 'balancer-special' # the special topic communicating with load balancer, maybe not needed when it's the same with topic_name
producer = KafkaProducer(
    bootstrap_servers=[f"{KAFKA_ADDRESS}:{KAFKA_PORT}"],
    value_serializer=lambda x: json.dumps(x).encode('utf-8')
)

def recover_topics(topics, serverID):
    producer.send(balancer_topic, {
        'serverID': serverID, 
        'balanceType': '2', 
        'topic': topics, 
        'info': 'A server crashed. Retrieve and serve these topics'})

if __name__ == '__main__':
    while True:
        server = input('Enter serverID: ')
        s = input('Enter topics to recover (splited by space): ')
        recover_topics(s.split(' '), server)