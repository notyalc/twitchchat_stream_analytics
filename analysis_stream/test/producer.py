import socket
from emoji import demojize
from datetime import datetime
import json
import os
from dotenv import load_dotenv
import yaml
from pathlib import Path
from kafka import KafkaProducer
from analysis_stream.test.transform import *

class stream_chat():
     
    def __init__(
            self, 
            producer_config: dict,
            topic: str,
            server: str,
            port: int,
            nickname: str,
            token: str,
            channel: str
            ):
        self.producer_config = producer_config
        self.topic = topic
        self.server = server
        self.port = int(port)
        self.nickname = nickname
        self.token = token
        self.channel = channel

        producer = KafkaProducer(**producer_config)

        self.producer = producer

        """
        Stream twitch chat messages to a kafka topic.

        Args:
            producer_config: kafka producer configuration
            topic: kafka topic to produce to
            server: server for Twitch IRC
            port: port for Twitch IRC
            nickname: twitch nickname
            token: twitch authentication token
            channel: channel to stream
        """

    def parse_resp(self, s: str) -> dict:
        """
        Transforms a Twitch IRC message to json.

        Args:
            s = IRC message string
        """
        try:
            chat = {}
            x = s.split(':')[1:]
            space_split = x[0].split(' ')
            chat['timestamp'] = datetime.now().strftime('%y-%m-%d %H:%M:%S')
            chat['user'] = x[0].split('!')[0]
            chat['host'] = space_split[2]
            chat['type'] = space_split[1]
            chat['message'] = x[1].split('\r\n')[0]
            
            return chat

        except Exception:
            print(f'Failed to parse {s}.')

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
                    sentiment_result = senti_menti(resp_json)
                    self.producer.send(topic=self.topic, key=sentiment_result['user'], value=sentiment_result)

        except KeyboardInterrupt:
            sock.close()
            exit()

class grab_config():

    def __init__(
            self,
            yaml: str,
            properties: str
    ):
        self.yaml = yaml
        self.properties = properties

    def check_file_path(self, rep: str):
        """
        Check if a given file path exists

        Args:
            rep: string to replace '.py'
        """
        file_path = __file__.replace('.py', rep)
        if Path(file_path).exists():
            return file_path
        else:
            raise Exception(f'Missing {file_path} file.')

    def grab_yaml(self):
        yaml_file_path = self.check_file_path(rep = self.yaml)
        with open(yaml_file_path) as yaml_file:
            config = yaml.safe_load(yaml_file)
            return config
        
    def read_ccloud_config(self):
        conf = {}
        config_file_path = self.check_file_path(rep = self.properties)
        with open(config_file_path) as fh:
            for line in fh:
                line = line.strip()
                if len(line) != 0 and line[0] != "#":
                    parameter, value = line.strip().split('=', 1)
                    conf[parameter] = value.strip()
        return conf

if __name__ == '__main__':
    
    # environment variables
    load_dotenv()

    server = os.environ.get('SERVER')
    port = os.environ.get('PORT')
    nickname = os.environ.get('NICKNAME')
    token = os.environ.get('TOKEN')

    # config files
    gc = grab_config(
        yaml = '.yaml',
        properties = '.properties'
    )
    yaml_config = gc.grab_yaml()
    producer_config = gc.read_ccloud_config()
    producer_config['key_serializer'] = lambda v:json.dumps(v).encode('utf-8')
    producer_config['value_serializer'] = lambda v:json.dumps(v).encode('utf-8')

    sr = stream_chat(
    producer_config = producer_config,
    topic = yaml_config.get('topic'),
    server = server,
    port = port,
    nickname = nickname,
    token = token,
    channel = yaml_config.get('channel')
    )

    sr.get_twitch_stream()