import os
import yaml
from dotenv import load_dotenv
from pathlib import Path
from analysis_stream.ingestion.producer import *


if __name__ == "__main__":

    load_dotenv()
    server = os.environ.get('SERVER')
    port = int(os.environ.get('PORT'))
    nickname = os.environ.get('NICKNAME')
    token = os.environ.get('TOKEN')

    yaml_file_path = __file__.replace('.py', '.yaml')
    if Path(yaml_file_path).exists():
        with open(yaml_file_path) as yaml_file:
            config = yaml.safe_load(yaml_file)
    else:
        raise Exception(f'Missing {yaml_file_path} file.')
    
    kp = kafka_producer(
        bootstrap_server = config.get('topic1').get('bootstrap_server'),
        server = server,
        port = port,
        nickname = nickname,
        token = token,
        channel = config.get('channel')
        )
    
    if __name__ == '__main__':
        kp.get_twitch_stream(topic = config.get('topic1').get('name'))