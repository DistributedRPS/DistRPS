# The game loop for the clients
# should be imported and called by the main program.
from kafka import KafkaConsumer
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable
import time
import json


KAFKA_PORT = 9092
KAFKA_ADDRESS = "192.168.56.103" #"127.0.0.1"
producer = None
consumer = None
# Note: all messages in this topic must be encoded in json format!
topic_name = ''
client_id = ''
game_state = {} # {clientID1: score, ...}
rps = {'0': 'rock', '1': 'paper', '2': 'scissor'}

# create producer & consumer instance
def init_var():
    global producer, consumer
    while producer == None:
        try:
            producer = KafkaProducer(
            bootstrap_servers=[f"{KAFKA_ADDRESS}:{KAFKA_PORT}"],
            value_serializer = lambda x: json.dumps(x).encode('utf-8')
            )
        except NoBrokersAvailable:
            print("No brokers available, retrying...", flush=True)
            time.sleep(1)
    while consumer == None:
        try:
            consumer = KafkaConsumer(
              topic_name,
              client_id = client_id,
              group_id = client_id, 
              bootstrap_servers = [f'{KAFKA_ADDRESS}:{KAFKA_PORT}'],
              value_deserializer = lambda x: json.loads(x.decode('utf-8')),
            )
        except:
            print("No brokers available, retrying...", flush=True)
            time.sleep(1)

# the game itself (3 rounds 2 players tournament)
def game(topic, id):
    global topic_name
    topic_name = topic
    global client_id
    client_id = id
    init_var()
    consumer.poll() # start consuming now, may take some time
    join_tournament()
    print('start game loop', flush=True)
    game_loop()

# join the tournament
def join_tournament():
    msg = {'clientID': client_id, 'info': 'I want to join the tournament', 'requestType': '0'}
    producer.send(topic_name, msg)
    #print(f'***LOG: {msg} sent by {client_id}', flush=True)

# the game loop
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
def game_loop():
    while True:
        msg = consumer.poll(timeout_ms=100000, max_records=1)
        if msg == {} or None:
            continue
        #print(f'***LOG: {msg}', flush=True)
        for v in msg.values():
            content = v[0].value
        if 'eventType' in content:
            event_type = content['eventType']
            if event_type == '0':
                print('Tourament starts! Waiting for other players...', flush=True)
            elif event_type == '1':
                get_input()
            elif event_type == '2':
                update_state(content)
            elif event_type == '3':
                show_result(content)
                # since the game ends, stop receiving messages in this topic. (?)
                break
            else:
                print('***Warning: eventType is not accepted in this message', flush=True)
            
# get input
def get_input():
    while True:
        print('Write your input as number. 0 for rock, 1 for paper, 2 for scissor:', flush=True)
        player_choice = input()
        if player_choice == '0' or player_choice == '1' or player_choice == '2':
            break
        print('Please enter the correct number without any other characters.', flush=True)
    choice_msg = {'clientID': client_id, 'info': 'update player choice RPS', 'requestType': '1', 'choice': rps[player_choice]}
    producer.send(topic_name, choice_msg)
    #print(f'***LOG: {choice_msg} sent by {client_id}', flush=True)    

# game state update, show round result
def update_state(content):
    global game_state
    # print what others showed
    temp_state = content['temp']
    for k, v in temp_state.items():
        if k != client_id:
            print(f'Player {k} shows {v}.', flush=True)
    # print winner for this round
    winner = content['winner']
    if winner == None:
        print("It's a tie!", flush=True)
    elif winner == client_id:
        print('You win this round!', flush=True)
    else:
        print(f'{winner} wins this round!', flush=True)
    # general game state
    print('Current game state: ', flush=True)
    game_state = content['state']
    print(f'This is round {game_state["round"]}.', flush=True)
    for k, v in game_state.items():
        if k == 'round':
            continue
        if k == client_id:
            print(f'Your score: {v}.')
        else:
            print(f"Player {k}'s score: {v}.")

# handle end and show result
def show_result(content):
    global game_state
    # show winner
    winner = content['winner']
    if winner == None:
        print("It's a tie!", flush=True)
    elif winner == client_id:
        print('Congrats! You win the game!', flush=True)
    else:
        print(f'{winner} wins the game!', flush=True)
    game_state = content['state']

if __name__ == '__main__':
    game('game-test', str(time.time()))