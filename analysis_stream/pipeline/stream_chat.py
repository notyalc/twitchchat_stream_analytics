import os
import yaml
from dotenv import load_dotenv
from pathlib import Path
from multiprocessing import Process
from analysis_stream.ingestion.producer import *
from analysis_stream.ingestion.consumer import *


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

    producer_config = {
        'bootstrap.servers': config.get('bootstrap_servers'),
        'key_serializer': lambda v:json.dumps(v).encode('utf-8'), 
        'value_serializer': lambda v:json.dumps(v).encode('utf-8')
    }

    consumer_config = producer_config.copy()
    consumer_config['auto_offset_reset'] = 'earliest'

    kp = KafkaProducer(**producer_config)

    kc = KafkaConsumer(
        topics = config.get('twitch_topic').get('name'),
        **consumer_config
        )
    
    ptc = produce_twitch_chat(
        producer = kp,
        server = server,
        port = port,
        token = token,
        nickname = nickname,
        channel = config.get('channel')
    )

    ps = process_sentiment(
        consumer = kc,
        producer = kp,
        consume_topic = config.get('twitch_topic').get('name'),
        produce_topic = config.get('sentiment_topic').get('name')
    )

    # stream twitch chat to topic, process sentiment and produce to 2nd topic in parallel

    stream_chat = Process(target = ptc.get_twitch_stream())
    stream_sentiment = Process(target = ps.process_messages())
    
    stream_chat.start()
    stream_sentiment.start()

    stream_chat.join()
    stream_sentiment.join()