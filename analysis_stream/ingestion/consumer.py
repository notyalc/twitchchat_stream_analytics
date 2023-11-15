from kafka import KafkaConsumer
from kafka import KafkaProducer
from analysis_stream.ingestion.transformer import senti_menti
import json

consumer = KafkaConsumer('twitch_chat', bootstrap_servers='127.0.0.1:9092', auto_offset_reset='earliest')

class kafka_consumer:

    def __init__(
        self, 
        topic_name: str, 
        bootstrap_server: str, 
        auto_offset_reset: str = "earliest"
        ):
        self.topic_name = topic_name
        self.bootstrap_server = bootstrap_server
        self.auto_offset_reset = auto_offset_reset

        self.consumer = KafkaConsumer(topic_name, bootstrap_server, auto_offset_reset)

        self.producer = KafkaProducer(bootstrap_servers=bootstrap_server, acks=1,
                                 key_serializer=lambda v:json.dumps(v).encode('utf-8'), 
                                 value_serializer=lambda v:json.dumps(v).encode('utf-8'))

    def process_messages(self, sentiment_topic="sentiment_topic"):
        try:
            for message in self.consumer:
                twitch_message = json.loads(message.value)
                sentiment_result = senti_menti(twitch_message)
                sentiment_result_str = json.dumps(sentiment_result)
                self.producer.send(sentiment_topic, value=sentiment_result_str)
        except Exception as e:
            print(f"Error processing messages: {e}")