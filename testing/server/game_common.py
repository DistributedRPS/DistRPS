# common functions for the game (server-side)
from kafka import KafkaConsumer
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable
import time
import json
from kafka.admin import KafkaAdminClient, NewTopic  # temporary
from threading import Thread
from threading import Lock

KAFKA_PORT = 9092
KAFKA_ADDRESS = "192.168.56.103"  # "127.0.0.1"
PLAYER_NUM = 2  # the number of players per tournament. Now I assume all players participate in all rounds.
TOTAL_ROUND = 3  # total rounds per tournament
producer = None
consumer = None
# can be the channel just between this one server and load balancer or shared by all servers, whatever (maybe the former is better)
balancer_topic = 'balancer-special' # the special topic communicating with load balancer, maybe not needed when it's the same with topic_name
topic_name = '' # (just used to be compatible with the old version codes)
active_topics = set()
server_id = ''
game_state_dic = {}  # key: topic_name(can identity the tournament), value: {client1: score, client2: score, round: num}
temp_state = {}  # store the player choice temporarily

# create producer & consumer instance
def init_var():
    global producer, consumer
    print(
        f"topic name: {topic_name}",
        flush=True)
    while producer == None:
        try:
            producer = KafkaProducer(
                bootstrap_servers=[f"{KAFKA_ADDRESS}:{KAFKA_PORT}"],
                value_serializer=lambda x: json.dumps(x).encode('utf-8')
            )
        except NoBrokersAvailable:
            print("No brokers available while creating producer, retrying...", flush=True)
            time.sleep(1)
    while consumer == None:
        try:
            consumer = KafkaConsumer(
                balancer_topic,
                client_id=server_id,
                group_id=server_id,
                bootstrap_servers=[f'{KAFKA_ADDRESS}:{KAFKA_PORT}'],
                value_deserializer=lambda x: json.loads(x.decode('utf-8'))
            )
        except:
            print("No brokers available while creating consumer, retrying...", flush=True)
            time.sleep(1)

# add some topics
def add_topic(topics):
    global active_topics
    if type(topics) is list:
        for t in topics:
            active_topics.add(t)
        consumer.subscribe(list(active_topics))
    elif type(topics) is str:
        active_topics.add(topics)
        consumer.subscribe(list(active_topics))
    else:
        print('***Warning: topic should be a list or string', flush=True)
    print('Updated subscription: ', consumer.subscription())

# remove one topic(string)
def remove_topic(topic):
    active_topics.remove(topic)
    consumer.subscribe(list(active_topics))
    # send one message to the load balancer, so it can delete this topic
    producer.send(balancer_topic, {'serverID': server_id, 'balanceType': '1', 'topic': topic, 'info': 'Please delete this topic'})

# tournament inited and wait for all players
def init_player_state(topic, client_id):
    global game_state_dic
    msg_start = {'serverID': server_id, 'eventType': '0', 'info': "Ok, let's start"}
    producer.send(topic, msg_start)
    # print(f'***LOG: {msg_start} sent by {server_id} in topic {topic}', flush=True)
    if topic in game_state_dic:
        game_state_dic[topic][client_id] = 0
        if len(game_state_dic[topic].keys()) == PLAYER_NUM + 1:  # because of round...
            # now all players are ok, so tournament "really" starts, the server asks for input
            game_state_dic[topic]['round'] = 1
            temp_state[topic] = {}
            request_input(topic)
    else:
        game_state_dic[topic] = {client_id: 0, 'round': 0}
    # print(f'***LOG: game state on server: {game_state_dic}', flush=True)


# ask for input
def request_input(topic):
    msg_ask = {'serverID': server_id, 'eventType': '1', 'info': 'Give me the input'}
    producer.send(topic, msg_ask)
    # print(f'***LOG: {msg_ask} sent by {server_id} in topic {topic}', flush=True)


# handle players' input
def handle_input(topic, client_id, gesture):
    global temp_state
    temp_state[topic][client_id] = gesture
    if len(temp_state[topic].keys()) == PLAYER_NUM:
        # this round finishes
        param = []
        for k, v in temp_state[topic].items():
            param.append({'client_id': k, 'gesture': v})
        winner = compare_gesture(param)
        # update game state
        if winner != None:
            game_state_dic[topic][winner] += 1
        # inform clients about update
        msg_update = {'serverID': server_id, 'eventType': '2',
                      'winner': winner, 'state': game_state_dic[topic],
                      'temp': temp_state[topic],
                      'info': 'update the game state'}
        producer.send(topic, msg_update)
        # print(f'***LOG: {msg_update} sent by {server_id} in topic {topic}', flush=True)
        # add round count and reset temp
        game_state_dic[topic]['round'] += 1
        temp_state[topic] = {}
        # check if game ends
        if game_state_dic[topic]['round'] > TOTAL_ROUND:
            end_tournament(topic)
        else:
            request_input(topic)

# handle the end of tournament
def end_tournament(topic):
    winner = find_winner(topic)
    msg_end = {'serverID': server_id, 'eventType': '3', 'info': 'tournament ends',
               'winner': winner, 'state': game_state_dic[topic]}
    producer.send(topic, msg_end)
    # print(f'***LOG: {msg_end} sent by {server_id} in topic {topic}', flush=True)
    # since the message is already sent and tournament ends, delete the record
    del game_state_dic[topic]
    #remove_topic(topic)    # TODO: free this comment when load balancer supports this



# find out the winner, still just work for two player right now
def find_winner(topic):
    clients = []
    records = game_state_dic[topic]
    for k in records.keys():
        if k != 'round':
            clients.append(k)
    if records[clients[0]] == records[clients[1]]:
        return None
    elif records[clients[0]] > records[clients[1]]:
        return clients[0]
    else:
        return clients[1]


# define the rule of RPS
#   R > S, S > P, P > R
# now just for two players
# parameters arr: [{client_id: string, gesture: string}, ...]
# return the person who wins, None for tie
def compare_gesture(arr):
    d1, d2 = arr[0], arr[1]
    g1 = d1['gesture']
    g2 = d2['gesture']
    if g1 == g2:
        return None
    if g1 == 'rock':
        if g2 == 'scissor':
            return d1['client_id']
        else:
            return d2['client_id']
    elif g1 == 'paper':
        if g2 == 'rock':
            return d1['client_id']
        else:
            return d2['client_id']
    else:
        if g2 == 'rock':
            return d2['client_id']
        else:
            return d1['client_id']
