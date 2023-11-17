import socket
from kafka import KafkaProducer
from emoji import demojize
from datetime import datetime
from time import sleep

class produce_twitch_chat:

    """
    Stream your favorite Twitch channel's chat to a Kafka topic.

    Args:
        topic: kafka topic
        producer: kafka producer
        server: server for Twitch IRC
        port: port for Twitch IRC
        token: your twitch authentication token
        nickname: twitch nickname
        channel: channel to stream
    """

    def __init__(
            self,
            topic: str,
            producer: KafkaProducer,
            server: str,
            port: int,
            token: str,
            nickname: str,
            channel: str
    ):
        self.topic = topic
        self.producer= producer
        self.server = server
        self.port = port
        self.token = token
        self.nickname = nickname
        self.channel = channel

    def parse_resp(self, s: str):
            """
            Transforms a Twitch IRC message to json.

            Args:
                s = IRC message string
            """
            chat = {}
            x = s.split(':')[1:]
            space_split = x[0].split(' ')
            chat['timestamp'] = datetime.now().strftime('%y-%m-%d %H:%M:%S')
            chat['user'] = x[0].split('!')[0]
            chat['host'] = space_split[2]
            chat['type'] = space_split[1]
            chat['message'] = x[1].split('\r\n')[0]

            return chat

    def get_twitch_stream(self):
            """
            Create web socket connection with Twitch to receive IRC messages.
            """
            if self.channel[0] == '#':
                pass
            else:
                self.channel = '#' + self.channel

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
                        self.producer.send(topic = self.topic, key = resp_json['user'], value = resp_json)
                    else:
                        sleep(60)
                        
            except KeyboardInterrupt:
                sock.close()
                exit()