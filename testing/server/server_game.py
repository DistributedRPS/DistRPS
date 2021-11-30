# the game-related services on servers
# should be imported and called by the main program.
from kafka import KafkaConsumer
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable
import time
import json
from kafka.admin import KafkaAdminClient, NewTopic  # temporary

KAFKA_PORT = 9092
KAFKA_ADDRESS = "192.168.56.103"  # "127.0.0.1"
PLAYER_NUM = 2  # the number of players per tournament. Now I assume all players participate in all rounds.
TOTAL_ROUND = 3  # total rounds per tournament
producer = None
consumer = None
topic_name = ''
server_id = 'server' + str(time.time())
game_state_dic = {}  # key: topic_name(can identity the tournament), value: {client1: score, client2: score, round: num}
temp_state = {}  # store the player choice temporarily


# TODO:
# FAULT TOLERANCE (server-side)
# assign existed topics to it and it will retrieve those game states and begin serving


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
                topic_name,
                client_id=server_id,
                group_id=server_id,
                bootstrap_servers=[f'{KAFKA_ADDRESS}:{KAFKA_PORT}'],
                value_deserializer=lambda x: json.loads(x.decode('utf-8'))
            )
        except:
            print("No brokers available while creating consumer, retrying...", flush=True)
            time.sleep(1)


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
def game_service(topic, id):
    global game_state_dic
    global topic_name
    topic_name = topic
    global server_id
    server_id = id
    init_var()
    print("connected to producer and consumer")
    # no exit point, this service should be always running
    for msg in consumer:
        content = msg.value
        topic = msg.topic
        # print(f'***LOG: {server_id} receive {content}', flush=True)
        # handle messages from clients (players)
        if 'requestType' in content:
            request_type = content['requestType']
            if request_type == '0':
                # right now, I assume the tournament is assigned by the load balancer, so respond is always yes
                # maybe some more machanics could be added
                init_player_state(topic, content['clientID'])
            elif request_type == '1':
                handle_input(topic, content['clientID'], content['choice'])
            else:
                print('***Warning: requestType is not accepted in this message', flush=True)
        # TODO: check subscription and update
        # TODO: if xxx: break, retrieve(...) and update the topic list, start game_service elsewere again
        # maybe Use threading


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


if __name__ == '__main__':
    # try:
    #     admin_client = KafkaAdminClient(bootstrap_servers=[f"{KAFKA_ADDRESS}:{KAFKA_PORT}"])
    #     topic_list = []
    #     topic_list.append(NewTopic(name=topic_name, num_partitions=1, replication_factor=1))
    #     admin_client.create_topics(new_topics=topic_list, validate_only=False)
    # except:
    #     pass
    print('Service started. Wait for some time and start clients.', flush=True)
    game_service()
