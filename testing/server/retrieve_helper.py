from kafka import KafkaConsumer
import time
import json
import game_common
from constants import EVENT_TYPES

recover_consumer = None

# just run this function as a thread
def retrieve_states(topics, kafka_address, kafka_port):
    init_var(topics, kafka_address, kafka_port)
    all_messages, situation = recover_states()
    # subscribe first
    game_common.add_topic(list(situation.keys()))
    # start timeout calculating
    set_last_time(list(situation.keys()))
    # then deal with the rest of the messages
    handle_left_msg(all_messages, situation)

# create consumer instance
def init_var(topics, kafka_address, kafka_port):
    global recover_consumer
    print(f"topic name: {topics}", flush=True)
    while recover_consumer == None:
        try:
            uniq = 'retrieve' + str(time.time())  # for convenience of test, generate different group_id
            recover_consumer = KafkaConsumer(
                client_id=uniq,
                group_id=uniq,
                bootstrap_servers=[f'{kafka_address}:{kafka_port}'],
                value_deserializer=lambda x: json.loads(x.decode('utf-8')),
                auto_offset_reset='earliest'    # get all history
            )
            recover_consumer.subscribe(topics)
            print('Retrieve Consumer created successfully.', flush=True)
        except:
            print("No brokers available while creating consumer, retrying...", flush=True)
            time.sleep(1)

# recover all game states
def recover_states():
    msg_storage = {}
    timeout = 10*1000
    situation = {}
    while True:
        messages = recover_consumer.poll(timeout)
        if messages == {} or messages == None:
            print('fetched all messages already', flush=True)
            break
        for msg_list in messages.values():
            for msg in msg_list:
                #print(msg, flush=True)
                if msg.topic in msg_storage:
                    msg_storage[msg.topic].append(msg.value)
                else:
                    msg_storage[msg.topic] = [msg.value]
    for k, v in msg_storage.items():
        code_num, start_ind = analyze_messages(k, v)
        if code_num == 3:
            continue    # no need to do anything, and no need to add this topic to serve
        else:
            situation[k] = (code_num, start_ind)
            game_common.temp_state_lock.acquire()
            game_common.temp_state[k] = {}
            game_common.temp_state_lock.release()
    return msg_storage, situation


# analyze one topic's messages and retrieve game states
# return a tuple (code, start_ind)
def analyze_messages(topic, records):
    last_msg = None
    last_ind = None
    client_ids = set()
    for r in records:
        if 'clientID' in r:
            client_ids.add(r['clientID'])
    for i in range(-1, -1*(len(records)+1), -1):
        if 'eventType' in records[i]:
            last_msg = records[i]
            last_ind = len(records) + i
            break
    # deal with different cases
    if last_msg == None:    # no game state at all
        return (0, 0)
    elif last_msg['eventType'] == EVENT_TYPES['TOURNAMENT_START']:  # tournament just started (maybe still waiting for clients)
        return (0, 0)
    elif last_msg['eventType'] == EVENT_TYPES['REQUEST_INPUT']:  # server asked for input
        # check if its round
        previous = records[last_ind - 1]
        if 'eventType' in previous and previous['eventType'] == EVENT_TYPES['STATE_UPDATE']:
            game_common.game_state_lock.acquire()
            game_common.game_state_dic[topic] = previous['state']
            game_common.game_state_lock.release()
        else:   # first round
            tmp = {'round': 0}
            for cid in client_ids:
                tmp[cid] = 0
            game_common.game_state_lock.acquire()
            game_common.game_state_dic[topic] = tmp
            game_common.game_state_lock.release()
        return (1, last_ind+1)
    elif last_msg['eventType'] == EVENT_TYPES['STATE_UPDATE']:  # just updated
        # directly fetch game states
        game_common.game_state_lock.acquire()
        game_common.game_state_dic[topic] = last_msg['state']
        game_common.game_state_lock.release()
        return (2, last_ind+1)
    elif last_msg['eventType'] == EVENT_TYPES['TOURNAMENT_END'] or last_msg['eventType'] == EVENT_TYPES['TOURNAMENT_TIMEOUT']:  # just ended
        game_common.send_del2lb(topic)
        return (3, len(records))
    else:
        print('***Warning: found invalid eventType when going through previous messages', flush=True)

# process those messages left
def handle_left_msg(all_messages, situation):
    for topic, sit in situation.items():
        code_num = sit[0]
        start_ind = sit[1]
        records = all_messages[topic]
        if code_num == 0 or code_num == 1:
            # can process again totally
            if start_ind >= len(records):
                continue
            for i in range(start_ind, len(records)):
                msg = records[i]
                if 'requestType' in msg:
                    game_common.handle_client_msg(topic, msg)
        elif code_num == 2:
            # special case: update->need input / update->result publish
            if start_ind >= len(records):
                game_common.round_end(topic)
            else:
                for i in range(start_ind, len(records)):
                    msg = records[i]
                    if 'requestType' in msg:
                        game_common.handle_client_msg(topic, msg)

# start to calculate timeout right now
def set_last_time(topics):
    game_common.tournament_time_lock.acquire()
    for t in topics:
        game_common.tournament_last_time[t] = time.time()
    game_common.tournament_time_lock.release()