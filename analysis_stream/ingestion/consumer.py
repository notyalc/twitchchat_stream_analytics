from kafka import KafkaConsumer, KafkaProducer
from analysis_stream.ingestion.transformer import senti_menti
import json

class kafka_consumer:

    def __init__(
        self, 
        topic_name: str, 
        bootstrap_servers: str, 
        auto_offset_reset: str = "earliest"
        ):
        self.topic_name = topic_name
        self.bootstrap_servers = bootstrap_servers
        self.auto_offset_reset = auto_offset_reset

        self.consumer = KafkaConsumer(self.topic_name, bootstrap_servers=self.bootstrap_servers, auto_offset_reset=self.auto_offset_reset)
        self.producer = KafkaProducer(bootstrap_servers=bootstrap_servers, acks=1,
                                 key_serializer=lambda v: json.dumps(v).encode('utf-8'), 
                                 value_serializer=lambda v: json.dumps(v).encode('utf-8'))

    def process_messages(self, sentiment_topic="sentiment_topic"):
        try:
            for message in self.consumer:
                twitch_message = json.loads(message.value)
                sentiment_result = senti_menti(twitch_message)
                sentiment_result_str = json.dumps(sentiment_result)
                self.producer.send(sentiment_topic, value=sentiment_result_str)
        except Exception as e:
            print(f"Error processing messages: {e}")

