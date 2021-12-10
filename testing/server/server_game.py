# the game-related services on servers
# should be imported and called by the main program.
from kafka.admin import KafkaAdminClient, NewTopic  # temporary
from threading import Thread
from constants import MESSAGE_CODES
import game_common
import retrieve_helper

# service supporting game loop in client
# event definition (server->client):
# eventType: 
#   0-tournament starts
#   1-need input(R/P/S)
#   2-game state update
#   3-tournament ends
# request definition (client->server):
# requstType:
#   0-want to join
#   1-player input
# topic_init can be string or list of string
def game_service(topic_init, server_id, kafka_address, kafka_port):
    game_common.topic_name = topic_init
    if type(topic_init) is str:
        game_common.balancer_topic = topic_init
    elif type(topic_init) is list:
        game_common.balancer_topic = topic_init[0]
    else:
        print('***Error: the game_service(topic_init, server_id) argument topic_init should be a list or str', flush=True)
    game_common.server_id = server_id
    game_common.init_var(kafka_address, kafka_port)
    game_common.add_topic(game_common.topic_name)
    print("connected to producer and consumer")
    # no exit point, this service should be always running
    while True:
        msg = game_common.consumer.poll(timeout_ms=100000, max_records=1)
        if msg == {} or None:
            continue
        for v in msg.values():
            content = v[0].value
            topic = v[0].topic
        #print(f'***LOG: {server_id} receive {content}', flush=True)
        # handle messages from the load balancer
        if topic == game_common.balancer_topic:
            handle_balancer_msg(content)
        # handle messages from clients (players)
        elif 'requestType' in content:
            game_common.handle_client_msg(topic, content)

# handle the messages from the load balancer
# 'message_code' difinition (load balancer<-> server):
#   0-add topic(s), {'serverID': '0', 'message_code': 0, 'topic': [] or str, ...} load balancer->server
#   1-remove one topic, {'serverID': '', 'message_code': '1', 'topic': '', ...} server->load balancer
#   2-retrieve and serve these topics, {'serverID': '', 'message_code': '2', 'topic': [], ...} load balancer->server
def handle_balancer_msg(content):
    if 'message_code' not in content:
        return
    message_code = content['message_code']
    if 'serverID' not in content or content['serverID'] != game_common.server_id:  # make sure the message is sent to me
        return
    if message_code == MESSAGE_CODES['ADD_TOPIC']:
            if 'topic' in content:
                game_common.add_topic(content['topic'])
            else:
                print('***Warning: arg topic should be in the content of the message.', flush=True)
    elif message_code == MESSAGE_CODES['DELETE_TOPIC']:   # sent by myself
        pass
    elif message_code == MESSAGE_CODES['SEND_TOPIC_LIST']:
        if 'topic' in content:
            topics = content['topic']
            if type(topics) is list:
                retrieve_thread=Thread(target=retrieve_helper.retrieve_states, args=(topics))
                retrieve_thread.start()
            else:
                print('***Warning: arg topic should be a list', flush=True)
        else:
            print('***Warning: arg topic should be in the content of the message', flush=True)
    else:
        print('***Warning: message_code is not accepted in this message', flush=True)


if __name__ == '__main__':
    balancer_topic = 'balancer-special'    # this topic just for communication bewtween server & load balancer, about topic adding/removing & fault tolerance, etc.
    game_test_topic = 'game-test52'
    try:
        admin_client = KafkaAdminClient(bootstrap_servers=[f"{game_common.KAFKA_ADDRESS}:{game_common.KAFKA_PORT}"])
        topic_list = []
        topic_list.append(NewTopic(name=balancer_topic, num_partitions=1, replication_factor=1))
        topic_list.append(NewTopic(name=game_test_topic, num_partitions=1, replication_factor=1))
        admin_client.create_topics(new_topics=topic_list, validate_only=False)
        admin_client.close()
        print('topic created')
    except:
        print('error when creating topics', flush=True)
    print('Service started. Wait for some time and start clients.', flush=True)
    game_service([balancer_topic, game_test_topic], 'server-tmp123563')
