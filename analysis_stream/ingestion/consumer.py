from kafka import KafkaConsumer, KafkaProducer
from analysis_stream.ingestion.transformer import senti_menti
import json
from time import sleep

class process_sentiment:

    """
    Consume messages from a kafka topic, analyze sentiment, then produce to another topic

    Args:
        consumer: kafka consumer
        producer: kafka producer
        consume_topic: topic to consume from
        produce_topic: topic to produce to
    """

    def __init__(
            self,
            consumer: KafkaConsumer,
            producer: KafkaProducer,
            consume_topic: str,
            produce_topic: str
    ):
        self.consumer = consumer
        self.consume_topic = consume_topic
        self.producer = producer
        self.produce_topic = produce_topic

    def process_messages(self):
        try:
            while True:
                for message in self.consumer:
                    if message is not None:
                        twitch_message = json.loads(message.value.replace('\\',''))
                        sentiment_result = senti_menti(twitch_message)
                        sentiment_result_str = json.dumps(sentiment_result)
                        self.producer.send(self.produce_topic, value = sentiment_result_str)
                    else:
                        sleep(5)
        except Exception as e:
            print(f"Error processing messages: {e}")