from kafka import KafkaConsumer

consumer = KafkaConsumer('test_topic', bootstrap_servers='localhost:9092', auto_offset_reset='earliest', api_version=(0, 10),
        consumer_timeout_ms=1000)

for msg in consumer:
    record = msg.value
    print(record)
if consumer is not None:
    consumer.close()

