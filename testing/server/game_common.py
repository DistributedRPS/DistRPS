# common functions for the game (server-side)
import psutil
from kafka import KafkaConsumer
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable
import time
import json
from threading import Lock
from constants import MESSAGE_CODES


PLAYER_NUM = 10  # the number of players per tournament. Now I assume all players participate in all rounds.
TOTAL_ROUND = 3  # total rounds per tournament
producer = None
consumer = None
# can be the channel just between this one server and load balancer or shared by all servers, whatever (maybe the former is better)
balancer_topic = '' # the special topic communicating with load balancer, maybe not needed when it's the same with topic_name
topic_name = '' # (just used to be compatible with the old version codes)
active_topics = set()
server_id = 'server-default'
game_state_dic = {}  # key: topic_name(can identity the tournament), value: {client1: score, client2: score, round: num}
temp_state = {}  # store the player choice temporarily

active_topics_lock = Lock()
game_state_lock = Lock()
temp_state_lock = Lock()
cpu_values_start_server = None
# create producer & consumer instance
def init_var(kafka_address, kafka_port):
    global producer, consumer
    print(f"topic name: {topic_name}", flush=True)
    while producer == None:
        try:
            producer = KafkaProducer(
                bootstrap_servers=[f"{kafka_address}:{kafka_port}"],
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
                bootstrap_servers=[f'{kafka_address}:{kafka_port}'],
                value_deserializer=lambda x: json.loads(x.decode('utf-8'))
            )
        except:
            print("No brokers available while creating consumer, retrying...", flush=True)
            time.sleep(1)

def handle_client_msg(topic, content):
    request_type = content['requestType']
    if request_type == '0':
        init_player_state(topic, content['clientID'])
    elif request_type == '1':
        handle_input(topic, content['clientID'], content['choice'])
    else:
        print('***Warning: requestType is not accepted in this message', flush=True)

# Subscribe to a new topic while keeping old subscriptions active
def add_topic(topics):
    global active_topics
    active_topics_lock.acquire()
    if type(topics) is list:
        for topic in topics:
            active_topics.add(topic)
        consumer.subscribe(list(active_topics))
    elif type(topics) is str:
        active_topics.add(topics)
        consumer.subscribe(list(active_topics))
    else:
        print('***Warning: topic should be a list or string', flush=True)
    active_topics_lock.release()
    print(active_topics)
    print('Updated subscription: ', consumer.subscription())

# Stop listening to the given topic
def remove_topic(topic):
    global active_topics
    active_topics_lock.acquire()
    active_topics.remove(topic)
    active_topics_lock.release()
    consumer.subscribe(list(active_topics))
    # send one message to the load balancer, so it can delete this topic
    send_del2lb(topic)

def send_del2lb(topic):
    producer.send(
      balancer_topic,
      {
        'serverID': server_id,
        'message_code': MESSAGE_CODES['DELETE_TOPIC'],
        'topic': topic,
        'info': 'Please delete this topic',
      }
    )

# tournament inited and wait for all players
def init_player_state(topic, client_id):
    global game_state_dic
    msg_start = {'serverID': server_id, 'eventType': '0', 'info': "Ok, let's start"}
    producer.send(topic, msg_start)
    game_state_lock.acquire()
    if topic in game_state_dic:
        game_state_dic[topic][client_id] = 0
        if len(game_state_dic[topic].keys()) == PLAYER_NUM + 1:  # because of round...
            # now all players are ok, so tournament "really" starts, the server asks for input
            game_state_dic[topic]['round'] = 0
            temp_state_lock.acquire()
            temp_state[topic] = {}
            temp_state_lock.release()
            request_input(topic)
    else:
        game_state_dic[topic] = {client_id: 0, 'round': 0}
    game_state_lock.release()


# ask for input
def request_input(topic):
    msg_ask = {'serverID': server_id, 'eventType': '1', 'info': 'Give me the input'}
    producer.send(topic, msg_ask)


# handle players' input
def handle_input(topic, client_id, gesture):
    global temp_state
    temp_state_lock.acquire()
    temp_state[topic][client_id] = gesture
    temp_state_lock.release()
    if len(temp_state[topic].keys()) == PLAYER_NUM:
        # this round finishes
        param = []
        for k, v in temp_state[topic].items():
            param.append({'client_id': k, 'gesture': v})
        winner = compare_gesture(param)
        # update game state
        game_state_lock.acquire()
        if winner != None:
            game_state_dic[topic][winner] += 1
        # inform clients about update
        msg_update = {'serverID': server_id, 'eventType': '2',
                      'winner': winner, 'state': game_state_dic[topic],
                      'temp': temp_state[topic],
                      'info': 'update the game state'}
        game_state_dic[topic]['round'] += 1
        game_state_lock.release()
        producer.send(topic, msg_update)
        # add round count and reset temp
        temp_state_lock.acquire()
        temp_state[topic] = {}
        temp_state_lock.release()
        round_end(topic)

def round_end(topic):
    # check if game ends
    if game_state_dic[topic]['round'] >= TOTAL_ROUND:
        end_tournament(topic)
    else:
        request_input(topic)

# handle the end of tournament
def end_tournament(topic):
    winner = find_winner(topic)
    msg_end = {'serverID': server_id, 'eventType': '3', 'info': 'tournament ends',
               'winner': winner, 'state': game_state_dic[topic]}
    producer.send(topic, msg_end)
    # since the message is already sent and tournament ends, delete the record
    game_state_lock.acquire()
    del game_state_dic[topic]
    game_state_lock.release()
    remove_topic(topic)

    cpu_values_end = psutil.cpu_times()  # end values for benchmark
    physical_memory_values_end = psutil.Process().memory_info()

    # below calculates OS cpu usage and physical memory usage
    sum1 = sum(list(cpu_values_start_server))
    sum2 = sum(list(cpu_values_end))
    diff = sum2 - sum1
    idlediff = cpu_values_end.idle - cpu_values_start_server.idle
    iddlepercentage = (idlediff * 100) / diff
    cpuusage = 100 - iddlepercentage
    usedmemory = physical_memory_values_end.rss

    # write results into a file for processing
    open("server.txt", 'w').close()
    f = open("server.txt", "a")
    f.write(str(cpuusage))
    f.write(",")
    f.write(str(usedmemory))
    f.write(",")
    f.write("server")
    f.close()
    print("Bench results inputted")

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
    player_1, player_2 = arr[0], arr[1]
    player_1_gesture = player_1['gesture']
    player_2_gesture = player_2['gesture']
    if player_1_gesture == player_2_gesture:
        return None
    if player_1_gesture == 'rock':
        if player_2_gesture == 'scissor':
            return player_1['client_id']
        else:
            return player_2['client_id']
    elif player_1_gesture == 'paper':
        if player_2_gesture == 'rock':
            return player_1['client_id']
        else:
            return player_2['client_id']
    else:
        if player_2_gesture == 'rock':
            return player_2['client_id']
        else:
            return player_1['client_id']
