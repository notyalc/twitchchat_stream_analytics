from kafka import KafkaConsumer

producer = KafkaConsumer('twitch_chat', bootstrap_servers='127.0.0.1:9092')