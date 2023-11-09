from kafka import KafkaConsumer

producer = KafkaConsumer(topic='twitch_chat', bootstrap_servers='127.0.0.1:9092', acks=1,
                        key_serializer=lambda v:json.dumps(v).encode('utf-8'), 
                        value_serializer=lambda v:json.dumps(v).encode('utf-8'))