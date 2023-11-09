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

channel = '#boxbox'



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
                producer.send(topic="twitch_chat", value=resp_str)
    except KeyboardInterrupt:
        sock.close()
        exit()

if __name__ == "__main__":
    get_twitch_stream()
