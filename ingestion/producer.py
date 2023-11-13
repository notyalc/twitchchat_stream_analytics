import os
import socket
import logging
import dotenv
from kafka import KafkaProducer
import json
from emoji import demojize

producer = KafkaProducer(bootstrap_servers='127.0.0.1:9092', acks=1,
                        key_serializer=lambda v:json.dumps(v).encode('utf-8'), 
                        value_serializer=lambda v:json.dumps(v).encode('utf-8'))

dotenv.load_dotenv()
server = os.environ.get('SERVER')
port = int(os.environ.get('PORT'))
nickname = os.environ.get('NICKNAME')
token = os.environ.get('TOKEN')

channel = '#shroud' # Change the channel to choose one of choice. 

def parse_resp(s, keys=['user', 'type', 'host']):
    parts = s.split(' ')
    username = parts[0].lstrip(':').split('!')[0]  # Extract only the username

    d = dict((key, value.lstrip(':')) for key, value in zip(keys, parts))
    d['user'] = username
    d['message'] = s.split(' :')[1].split('\r\n')[0]

    return d

print(parse_resp(":ranran46!ranran46@ranran46.tmi.twitch.tv PRIVMSG #scarra :Gnar is such an underrated carry\r\n"))

def get_twitch_stream():
    sock = socket.socket()
    sock.connect((server, port))
    sock.send(f"PASS {token}\r\n".encode())
    sock.send(f"NICK {nickname}\r\n".encode())
    sock.send(f"JOIN {channel}\r\n".encode())
    resp = sock.recv(2048).decode('utf-8')

    try:
        while True:
            if resp.startswith('PING'):
                sock.send("PONG\n".encode('utf-8'))
            elif len(resp) > 0:
                resp = sock.recv(2048).decode('utf-8')  
                resp_str = demojize(resp).encode('utf-8').decode('utf-8')
                resp_json = parse_resp(resp_str)
                producer.send(topic="twitch_chat", value=resp_json)
                
    except KeyboardInterrupt:
        sock.close()
        exit()

if __name__ == "__main__":
    get_twitch_stream()
