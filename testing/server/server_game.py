# the game-related services on servers
# should be imported and called by the main program.
from kafka.admin import KafkaAdminClient, NewTopic  # temporary
from threading import Thread
import game_common
import retrieve_helper

# TODO:
# FAULT TOLERANCE (server-side)
# assign existed topics to it and it will retrieve those game states and begin serving

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
def game_service(topic_init, sid):
    game_common.topic_name = topic_init
    game_common.server_id = sid
    game_common.init_var()
    game_common.add_topic(game_common.topic_name)
    print("connected to producer and consumer")
    # no exit point, this service should be always running
    for msg in game_common.consumer:
        content = msg.value
        #print(f'***LOG: {server_id} receive {content}', flush=True)
        topic = msg.topic
        # handle messages from the load balancer
        if topic == game_common.balancer_topic:
            handle_balancer_msg(content)
        # handle messages from clients (players)
        elif 'requestType' in content:
            request_type = content['requestType']
            if request_type == '0':
                game_common.init_player_state(topic, content['clientID'])
            elif request_type == '1':
                game_common.handle_input(topic, content['clientID'], content['choice'])
            else:
                print('***Warning: requestType is not accepted in this message', flush=True)

# handle the messages from the load balancer
# 'balanceType' difinition (load balancer<-> server):
#   0-add topic(s), {'serverID': '0', 'balanceType': 0, 'topic': [] or str, ...} load balancer->server
#   1-remove one topic, {'serverID': '', 'balanceType': '1', 'topic': '', ...} server->load balancer
#   2-retrieve and serve these topics, {'serverID': '', 'balanceType': '2', 'topic': [], ...} load balancer->server
def handle_balancer_msg(content):
    # TODO: fault tolerance if xxx: break, retrieve(...) and update the topic list, start game_service elsewere again, maybe Use threading
    if 'balanceType' not in content:
        return
    balance_type = content['balanceType']
    if 'serverID' not in content or content['serverID'] != game_common.server_id:  # make sure the message is sent to me
        return
    if balance_type == '0':
            if 'topic' in content:
                game_common.add_topic(content['topic'])
            else:
                print('***Warning: arg topic should be in the content of the message.', flush=True)
    elif balance_type == '1':   # sent by myself
        pass
    elif balance_type == '2':
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
        print('***Warning: balanceType is not accepted in this message', flush=True)


if __name__ == '__main__':
    balancer_topic = 'balancer-special'    # this topic just for communication bewtween server & load balancer, about topic adding/removing & fault tolerance, etc.
    game_test_topic = 'game-test'
    try:
        admin_client = KafkaAdminClient(bootstrap_servers=[f"{KAFKA_ADDRESS}:{KAFKA_PORT}"])
        topic_list = []
        topic_list.append(NewTopic(name=balancer_topic, num_partitions=1, replication_factor=1))
        topic_list.append(NewTopic(name=game_test_topic, num_partitions=1, replication_factor=1))
        admin_client.create_topics(new_topics=topic_list, validate_only=False)
        admin_client.close()
        print('topic created')
    except:
        print('error when creating topics', flush=True)
    print('Service started. Wait for some time and start clients.', flush=True)
    game_service([balancer_topic, game_test_topic], 'server123456')
