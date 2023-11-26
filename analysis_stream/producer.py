import socket
from emoji import demojize
from datetime import datetime
import json
import os
from dotenv import load_dotenv
from kafka import KafkaProducer
from analysis_stream.config_func import *
from analysis_stream.transform import *
import boto3


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
        self.download_env_from_s3()
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

    def download_env_from_s3(self):
        """
        Download .env file from S3 and load environment variables.
        """
        s3_bucket_name = 'twitch-stream-analytics'
        s3_env_file_name = '.env'
        s3 = boto3.client('s3')
        
        try:
            s3.download_file(s3_bucket_name, s3_env_file_name, '.env')
            print(f"Downloaded .env file from S3: {s3_env_file_name}")
        except Exception as e:
            print(f"Error downloading .env file from S3: {e}")

        load_dotenv()

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
                    if sentiment_result is not None:
                        self.producer.send(topic=self.topic, key=sentiment_result['user'], value=sentiment_result)
                    else:
                        pass

        except KeyboardInterrupt:
            sock.close()
            exit()

if __name__ == '__main__':
    
    # environment variables
    load_dotenv()

    server = os.environ.get('SERVER')
    port = os.environ.get('PORT')
    nickname = os.environ.get('NICKNAME')
    token = os.environ.get('TOKEN')

    # config files
    gc = grab_config(
        path = __file__
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