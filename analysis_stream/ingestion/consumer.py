from kafka import KafkaConsumer
import json

consumer = KafkaConsumer('twitch_chat', bootstrap_servers='127.0.0.1:9092', auto_offset_reset='earliest')
for message in consumer:
    # Assuming the message value is a JSON-formatted string
    twitch_message = json.loads(message.value)
    print(twitch_message)