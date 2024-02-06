from confluent_kafka import Consumer, KafkaError
import time

def fetch_all_messages(topic):
    conf = {
        'bootstrap.servers': 'localhost:9092',
        'group.id': 'fetcher_group',
        'auto.offset.reset': 'earliest',
        'socket.timeout.ms': 60000,  # Increase socket timeout to 60 seconds
        'request.timeout.ms': 60000  # Increase request timeout to 60 seconds
    }

    consumer = Consumer(conf)

    try:
        # Subscribe to the topic
        consumer.subscribe([topic])

        all_messages = []
        messages_fetched = 0

        while True:
            # Poll for messages
            msg = consumer.poll(timeout=1.0)

            if msg is None:
                break  # Exit the loop if no more messages

            if msg.error():
                # Error or event
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    # End of partition
                    print('%% %s [%d] reached end at offset %d\n' %
                          (msg.topic(), msg.partition(), msg.offset()))
                elif msg.error():
                    raise KafkaException(msg.error())
            else:
                # Append the message value to the list
                all_messages.append(msg.value().decode('utf-8'))
                messages_fetched += 1

                # Print progress update every 100 messages
                if messages_fetched % 100 == 0:
                    print(f"Fetched {messages_fetched} messages so far...")

    except KeyboardInterrupt:
        pass
    finally:
        # Close the consumer
        consumer.close()

    return all_messages

if __name__ == "__main__":
    topic_name = 'messageapp'

    all_messages = fetch_all_messages(topic_name)

    # Print all the fetched messages
    for message in all_messages:
        print(message)
