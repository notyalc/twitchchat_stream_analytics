import socket
from kafka import KafkaProducer
import json
from emoji import demojize
from datetime import datetime

class kafka_producer:

    def __init__(
            self, 
            bootstrap_server: str,
            server: str,
            port: int,
            nickname: str,
            token: str,
            channel: str
            ):
        self.bootstrap_server = bootstrap_server
        self.server = server
        self.port = port
        self.nickname = nickname
        self.token = token
        self.channel = channel

        producer = KafkaProducer(bootstrap_servers=bootstrap_server, acks=1,
                                 key_serializer=lambda v:json.dumps(v).encode('utf-8'), 
                                 value_serializer=lambda v:json.dumps(v).encode('utf-8'))
        
        self.producer = producer

    def parse_resp(self, s):
        """Parse twitch response"""
        chat = {}
        x = s.split(':')[1:]
        space_split = x[0].split(' ')
        chat['timestamp'] = datetime.now().strftime('%y-%m-%d %H:%M:%S')
        chat['user'] = x[0].split('!')[0]
        chat['host'] = space_split[2]
        chat['type'] = space_split[1]
        chat['message'] = x[1].split('\r\n')[0]

        return chat

    def get_twitch_stream(self, topic: str):
        """Create connection to twitch to receive messages real time"""
        if self.channel[0] == '#':
            pass
        else:
            self.channel = '#'+self.channel

        sock = socket.socket()
        sock.connect((self.server, self.port))
        sock.send(f"PASS {self.token}\r\n".encode())
        sock.send(f"NICK {self.nickname}\r\n".encode())
        sock.send(f"JOIN {self.channel}\r\n".encode())
        resp = sock.recv(2048).decode('utf-8')

        try:
            while True:
                if resp.startswith('PING'):
                    sock.send("PONG\n".encode('utf-8'))
                elif len(resp) > 0:
                    resp = sock.recv(2048).decode('utf-8')  
                    resp_str = demojize(resp).encode('utf-8').decode('utf-8')
                    resp_json = self.parse_resp(resp_str)
                    self.producer.send(topic=topic, value=resp_json)
                    
        except KeyboardInterrupt:
            sock.close()
            exit()